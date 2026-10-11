import { DurableObject } from "cloudflare:workers"
import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context"
import { Type } from "@earendil-works/pi-ai"
import { createModels } from "@earendil-works/pi-ai/models"
import { fauxAssistantMessage, fauxProvider, fauxToolCall, type FauxResponseFactory } from "@earendil-works/pi-ai/providers/faux"
import { createRegistry, defineExtension, defineTool, Harness, section, type Conversation } from "@earendil-works/pi-durable"
import { openDurableObjectSqliteStorage } from "@earendil-works/pi-durable/storage/sqlite/cloudflare"
import { fingerprint, next, payload, type Message, type Turn } from "../../src/plan.ts"
import { serve, tables, type Bench } from "../../src/serve.ts"

const SYSTEM = "You are a benchmark agent. Call lookup as instructed, then answer briefly."

const faux = fauxProvider({ models: [{ id: "scripted-1", contextWindow: 1e9 }], tokenSize: { min: 1e9, max: 1e9 } })
const models = createModels()
models.setProvider(faux.provider)

type Block = { type: string; text?: string; arguments?: { n?: number } }
let seen: Message[] = []

const respond: FauxResponseFactory = transcript => {
  faux.appendResponses([respond])
  seen = transcript.messages.filter(m => ["user", "assistant", "toolResult"].includes(m.role)).map(m => {
    const blocks: Block[] = typeof m.content === "string" ? [{ type: "text", text: m.content }] : m.content as Block[]
    const text = blocks.filter(b => b.type === "text").map(b => b.text).join("")
    const calls = blocks.filter(b => b.type === "toolCall").map(b => b.arguments!.n!)
    return { role: m.role === "toolResult" ? "tool" as const : m.role as "user" | "assistant", text, ...(calls.length ? { calls } : {}) }
  })
  const step = next(seen)
  return "call" in step
    ? fauxAssistantMessage(fauxToolCall("lookup", { n: step.call }, { id: `call-${step.call}` }), { stopReason: "toolUse" })
    : fauxAssistantMessage(step.answer)
}
faux.setResponses([respond])

const lookup = defineTool({
  name: "lookup",
  description: "Look up record number n",
  parameters: Type.Object({ n: Type.Number() }),
  execute: async ({ n }) => ({ content: [{ type: "text", text: payload(n) }] }),
})

const registry = createRegistry()
registry.install(defineExtension({ name: "bench", tools: [lookup], sections: [section("preamble", () => SYSTEM, { tag: false })] }))

export class PiDO extends DurableObject implements Bench {
  private root?: Conversation

  private async open() {
    if (this.root) return this.root
    const harness = await Harness.open(await openDurableObjectSqliteStorage(this.ctx.storage), {
      models, registry, settings: { compaction: { enabled: false } },
    }, context)
    const model = faux.getModel()
    return this.root = await harness.root(context, { agent: { model: { provider: model.provider, modelId: model.id } } })
  }

  async wake() { await this.open() }

  async turn({ id, text }: Turn) {
    const root = await this.open()
    const submission = await root.submit({ type: "input", content: text, requestId: id }, context)
    const settled = await submission.wait(context)
    if (settled.status !== "done") throw new Error(JSON.stringify(settled))
    await root.waitForIdle(context)
  }

  async seed(turns: Turn[]) {
    for (const turn of turns) await this.turn(turn)
    return fingerprint(seen)
  }

  async stats() {
    const sql = this.ctx.storage.sql
    return { bytes: sql.databaseSize, tables: tables(sql) }
  }
}

export default serve<{ PI: DurableObjectNamespace<PiDO> }>(env => env.PI.getByName("main"))
