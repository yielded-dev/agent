import { Effect, Layer, Schema } from "effect"
import { Rpc } from "effect/unstable/rpc"
import { atom, defineActor, type ThreadCoordinate } from "tardie/core"
import { actorContext, agentMethods, compact, infer, messages, tools } from "tardie/agent"
import { Model, ModelLock, modelActs, modelInfo, toolActs } from "tardie/agent/services"
import { defineLibrary, MethodDescription, MethodHints } from "tardie/libraries"
import { modelLockService } from "tardie/model/lock"
import { cloudflareThreadName, createActorWorker } from "tardie/platform/cloudflare"
import { fingerprint, next, payload, type Message, type Turn } from "../../src/plan.ts"
import { serve, tables, type Bench } from "../../src/serve.ts"

const SYSTEM = "You are a benchmark agent. Call lookup as instructed, then answer briefly."
const MODEL = { provider: "scripted", model_id: "scripted-1" }
const COORDINATE = { actor: "bench-agent", instance: "main", thread: "bench" }

const kv = defineLibrary({
  name: "kv",
  description: "Record lookup",
  toolNames: { lookup: "lookup" },
  methods: [Rpc.make("lookup", { payload: Schema.Struct({ n: Schema.Finite }), success: Schema.String })
    .annotate(MethodDescription, "Look up record number n")
    .annotate(MethodHints, { readOnlyHint: true, openWorldHint: false })],
})

const actor = defineActor(COORDINATE.actor, Effect.gen(function* () {
  const toolView = yield* tools([kv])
  const context = yield* compact(messages)
  const agent = yield* infer(atom(get => ({ system: SYSTEM, tools: get(toolView), context: get(context) })))
  return { atom: agent, methods: agentMethods }
}))

type Entry = { role: string; text?: string; toolCalls?: readonly { input: unknown }[] }
let seen: Message[] = []

const model = Layer.succeed(Model, {
  call: input => Effect.sync(() => {
    seen = (input.context as readonly Entry[]).map(m => ({
      role: m.role as Message["role"], text: m.role === "tool" ? JSON.parse(m.text!) : m.text ?? "",
      ...(m.toolCalls?.length ? { calls: m.toolCalls.map(c => (c.input as { n: number }).n) } : {}),
    }))
    const step = next(seen)
    const usage = { input: 0, output: 0, usd: null }
    return "call" in step
      ? { text: "", toolCalls: [{ callId: `call-${step.call}`, name: "lookup", input: { n: step.call } }], usage }
      : { text: step.answer, toolCalls: [], usage }
  }),
})

const lock = Layer.succeed(ModelLock, modelLockService({
  schema: 2,
  providers: { scripted: { protocol: "openai-responses", baseUrl: "http://127.0.0.1/unused", env: [] } },
  models: [{ ...MODEL, contextWindowTokens: 1e9 }],
}, { default: MODEL, allow: "*" }))

const services = Layer.mergeAll(modelInfo, modelActs, toolActs([kv.implement({ lookup: ({ n }) => Effect.succeed(payload(n)) })]))
  .pipe(Layer.provide(Layer.merge(model, lock)))

const worker = createActorWorker({ actor, actorContext, services: () => services })

type Reference = {
  wait: Effect.Effect<void, Error>
  methods: { message: (input: { text: string }, request: { id: string }) => Effect.Effect<unknown, Error> }
}
type Internals = { identityReady: Promise<void>; address(): ThreadCoordinate; actors(): { reference(c: ThreadCoordinate): Effect.Effect<Reference, Error> } }

export class ActorDO extends worker.ActorObject {
  async bytes() { return this.ctx.storage.sql.databaseSize }
}

export class ThreadDO extends worker.ThreadObject implements Bench {
  private reference?: Reference

  private async open() {
    if (this.reference) return this.reference
    const self = this as unknown as Internals
    await self.identityReady
    const reference = await Effect.runPromise(self.actors().reference(self.address()))
    await Effect.runPromise(reference.wait)
    return this.reference = reference
  }

  async wake() { await this.open() }

  async turn({ id, text }: Turn) {
    const reference = await this.open()
    await Effect.runPromise(reference.methods.message({ text }, { id }))
    await Effect.runPromise(reference.wait)
  }

  async seed(turns: Turn[]) {
    for (const turn of turns) await this.turn(turn)
    return fingerprint(seen)
  }

  async stats() {
    const sql = this.ctx.storage.sql
    return { bytes: sql.databaseSize, tables: tables(sql), checkpoint: (await this.checkpoint())?.position }
  }
}

type Env = { ACTORS: DurableObjectNamespace<ActorDO>; THREADS: DurableObjectNamespace<ThreadDO> }

export default serve<Env>(env => {
  const thread = env.THREADS.getByName(cloudflareThreadName(COORDINATE))
  return {
    wake: () => thread.wake(),
    turn: turn => thread.turn(turn),
    seed: turns => thread.seed(turns),
    stats: async () => {
      const [stats, directory] = await Promise.all([thread.stats(), env.ACTORS.getByName(COORDINATE.instance).bytes()])
      return { ...stats, bytes: stats.bytes + directory }
    },
  }
}, async env => {
  const response = await worker.fetch(new Request(`http://bench/v1/actors/${COORDINATE.instance}/threads`, {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ name: COORDINATE.thread }),
  }), env as never)
  if (!response.ok) throw new Error(await response.text())
})
