import { AsyncLocalStorage } from "node:async_hooks";
import { Context, Effect, Schema } from "effect";

export const admissionVariants = ["baseline-a", "baseline-b", "clock-sync", "prearm-only", "prearm-only-probe", "prearm-now", "prearm-fast", "prearm-fast-probe"] as const;

export interface AdmissionEvent {
  readonly event: string;
  readonly atMs: number;
  readonly detail?: unknown;
}

export interface AdmissionObservation {
  events: AdmissionEvent[];
  variant: string;
  probe?: (event: string) => void;
}
export const admission = new AsyncLocalStorage<AdmissionObservation>();
export const AdmissionContext = Context.Reference<AdmissionObservation | undefined>(
  "prod-admit/AdmissionContext", { defaultValue: () => undefined },
);

const requests = new WeakMap<object, AdmissionObservation>();
const decodeRequest = Schema.decodeUnknownSync(Schema.Struct({ idempotencyKey: Schema.String }));
const decodeVariant = Schema.decodeUnknownSync(Schema.Literals(admissionVariants));

// The native override registers the actual argument object before calling super.
// effect-cf suspends the endpoint factory; its ambient ALS can belong to a prior event.
export const registerAdmission = (encoded: unknown, observation: AdmissionObservation): (() => void) => {
  if (typeof encoded !== "object" || encoded === null) throw new Error("Expected native submit argument object");
  requests.set(encoded, observation);
  return () => { requests.delete(encoded); };
};

export const observeAdmission = <A, E, R>(encoded: unknown, body: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> => {
  const { idempotencyKey } = decodeRequest(encoded);
  const variant = decodeVariant(idempotencyKey.split(":")[1] ?? "baseline-a");
  const observation = typeof encoded === "object" && encoded !== null ? requests.get(encoded) : undefined;
  if (observation === undefined || observation.variant !== variant)
    throw new Error("Admission observation does not own this native request");
  return body.pipe(Effect.provideService(AdmissionContext, { ...observation, variant }));
};

export const markAdmission = (event: string, detail?: unknown, observation = admission.getStore()): void => {
  observation?.events.push({ event, atMs: Date.now(), ...(detail === undefined ? {} : { detail }) });
};

export const traceAdmission = <A, E, R>(name: string, body: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
  Effect.flatMap(AdmissionContext, (current) => Effect.suspend(() => {
    const observation = current ?? admission.getStore();
    markAdmission(`${name}:start`, undefined, observation);
    observation?.probe?.(`${name}:start`);
    return body.pipe(Effect.onExit((exit) => Effect.sync(() => {
      markAdmission(`${name}:end`, exit._tag, observation);
      observation?.probe?.(`${name}:end`);
    })));
  }));
