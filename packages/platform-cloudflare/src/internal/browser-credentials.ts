import { Cause, Context, Effect, Redacted, Schema } from "effect";
import type { Frame, Page } from "puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js";

import {
  BrowserCredentialAccess,
  type BrowserCredentialMaterial,
  CardCredential,
  CredentialBinding,
  CredentialForm,
  type CredentialDispatch,
  type CredentialFieldRole,
  CredentialFillError,
  CredentialFillResult,
  CredentialOrigin,
  CredentialTarget,
  FillCredentialRequest,
  LoginCredential,
  type CredentialAccessRequest,
} from "../BrowserCredentials.ts";

/** Supplied only under the attachment lock, with the remaining command/session budget. */
export class BrowserSessionPage extends Context.Service<
  BrowserSessionPage,
  {
    readonly page: Page;
    readonly commandTimeoutMillis: number;
  }
>()("@effect-agent/platform-cloudflare/internal/BrowserSessionPage") {}

const TargetReply = Schema.Union([
  Schema.Struct({
    action: Schema.String.check(Schema.isMaxLength(2048)),
    shape: Schema.String.check(Schema.isMaxLength(128 * 1024)),
  }),
  Schema.Struct({ reason: Schema.Literals(["stale-target", "unsupported"]) }),
]);

const WriteReply = Schema.Literals([
  "filled",
  "stale-target",
  "unsupported-before-write",
  "unsupported",
]);

// One invocation-local isolated-world handle. Values never leave the page through this handle.
// This program neither discovers controls nor classifies roles nor observes page content.
const prepareFields = `(() => {
  const doc = document;
  let fields = [];
  let form;
  let snapshots = [];
  const describe = el => {
    if (!el || !el.isConnected || el.ownerDocument !== doc || doc !== document) return null;
    const native = el instanceof HTMLTextAreaElement ||
      (el instanceof HTMLSelectElement && !el.multiple) ||
      (el instanceof HTMLInputElement && ['text','email','password','tel','number','month','search','url','date','time','week','datetime-local'].includes(el.type));
    if (!native || el.matches(':disabled') || el.readOnly || !el.form) return null;
    const values = [el.form.action, el.form.method, el.form.enctype, el.name, el.type, el.autocomplete ?? ''];
    if (values.some(value => value.length > 2048)) return null;
    return JSON.stringify(values);
  };
  const validate = () => fields.length > 0 && fields.every((el, index) =>
    el.form === form && describe(el) !== null && describe(el) === snapshots[index]);
  return {
    prepare(selectors, roles) {
      try {
        fields = selectors.map(selector => {
          const matches = doc.querySelectorAll(selector);
          return matches.length === 1 ? matches[0] : null;
        });
      } catch { return {reason: 'stale-target'}; }
      if (fields.some(el => !el) || new Set(fields).size !== fields.length) return {reason: 'stale-target'};
      if (fields.some((el, index) => roles[index] === 'password' && el.type !== 'password')) return {reason: 'unsupported'};
      snapshots = fields.map(describe);
      form = fields[0].form;
      if (!form || snapshots.some(value => value === null) || fields.some(el => el.form !== form)) return {reason: 'unsupported'};
      return {action: form.action, shape: JSON.stringify(snapshots)};
    },
    target() { return validate() ? {action: form.action, shape: JSON.stringify(snapshots)} : {reason: 'stale-target'}; },
    node(index) { return index === 0 ? doc.documentElement : index === 1 ? form : fields[index - 2]; },
    fill(index, value) {
      if (!validate()) return 'stale-target';
      const el = fields[index];
      if (el instanceof HTMLSelectElement) {
        const options = [...el.options].filter(option => option.value === value);
        if (options.length !== 1 || options[0].matches(':disabled')) return 'unsupported-before-write';
      }
      let prototype = Object.getPrototypeOf(el);
      let setter;
      while (prototype && !setter) { setter = Object.getOwnPropertyDescriptor(prototype, 'value')?.set; prototype = Object.getPrototypeOf(prototype); }
      if (!setter) return 'unsupported-before-write';
      setter.call(el, value);
      if (el.value !== value) return 'unsupported';
      el.dispatchEvent(new Event('input', {bubbles: true}));
      el.dispatchEvent(new Event('change', {bubbles: true}));
      return 'filled';
    }
  };
})()`;

const secretFor = (material: BrowserCredentialMaterial, role: typeof CredentialFieldRole.Type) => {
  if (material._tag === "LoginCredential")
    return role === "username"
      ? material.username
      : role === "password"
        ? material.password
        : undefined;
  switch (role) {
    case "card-name":
      return material.name;
    case "card-number":
      return material.number;
    case "card-expiry":
      return material.expiry;
    case "card-expiry-month":
      return material.expiryMonth;
    case "card-expiry-year":
      return material.expiryYear;
    case "card-security-code":
      return material.securityCode;
    case "username":
    case "password":
      return undefined;
  }
};

/** Acquire exact native controls for this Scope; no credential material is resolved. */
const openForm = Effect.fnUntraced(function* (input: CredentialForm) {
  const { page } = yield* BrowserSessionPage;

  const fail = (reason: CredentialFillError["reason"]) =>
    CredentialFillError.make({
      reason,
      dispatch: "not-dispatched",
      filled: 0,
      cleanup: "not-requested",
    });

  const remote = <A>(run: (signal: AbortSignal) => Promise<A>) =>
    Effect.tryPromise({ try: run, catch: () => fail("provider") });

  const release = (handles: ReadonlyArray<{ dispose(): Promise<void> }>) =>
    Effect.promise(async () => {
      await Promise.all(handles.map((handle) => handle.dispose().catch(() => {})));
    }).pipe(Effect.interruptible, Effect.timeoutOption("1 second"), Effect.asVoid);

  const acquire = <A extends { dispose(): Promise<void> }>(run: () => Promise<ReadonlyArray<A>>) =>
    Effect.acquireRelease(
      remote(async (signal) => {
        const handles = await run();

        if (signal.aborted) {
          await Promise.all(handles.map((handle) => handle.dispose().catch(() => {})));

          return undefined;
        }

        return handles;
      }).pipe(
        Effect.filterOrFail(
          (handles) => handles !== undefined,
          () => fail("provider"),
        ),
        Effect.interruptible,
      ),
      release,
    );

  const origin = Effect.fnUntraced(function* (value: string) {
    const url = yield* Effect.try({
      try: () => new URL(value),
      catch: () => fail("denied"),
    });

    if (url.username || url.password) return yield* fail("denied");

    return yield* Schema.decodeEffect(CredentialOrigin)(url.origin).pipe(
      Effect.mapError(() => fail("denied")),
    );
  });

  const request = yield* Schema.decodeEffect(CredentialForm, {
    onExcessProperty: "error",
  })(input).pipe(Effect.mapError(() => fail("denied")));

  const roles = request.fields.map((field) => field.role);

  if (
    new Set(roles).size !== roles.length ||
    roles.some(
      (role) => (request.kind === "login") !== (role === "username" || role === "password"),
    )
  )
    return yield* fail("denied");
  let frame: Frame = page.mainFrame();

  for (const selector of request.frame ?? []) {
    const matches = yield* acquire(() => frame.$$(selector));
    const element = matches[0];

    if (matches.length !== 1 || element === undefined) return yield* fail("stale-target");
    const child = yield* remote(() => element.contentFrame());

    if (child === null) return yield* fail("unsupported");
    frame = child;
  }

  const handles = yield* acquire(async () => [
    await frame.isolatedRealm().evaluateHandle(prepareFields),
  ]);

  const held = handles[0];

  if (held === undefined) return yield* fail("provider");

  const inspect = (raw: unknown) =>
    Schema.decodeUnknownEffect(TargetReply)(raw).pipe(
      Effect.mapError(() => fail("provider")),
      Effect.filterOrFail(
        (reply) => "action" in reply,
        (reply) => fail("reason" in reply ? reply.reason : "provider"),
      ),
    );

  const prepared = yield* remote(() =>
    held.evaluate(
      (state, selectors, roles) => {
        if (typeof state !== "object" || state === null) return null;

        return Reflect.apply(Reflect.get(state, "prepare"), state, [selectors, roles]);
      },
      request.fields.map((field) => field.selector),
      roles,
    ),
  ).pipe(Effect.flatMap(inspect));

  const target = CredentialTarget.make({
    topOrigin: yield* origin(page.url()),
    frameOrigin: yield* origin(frame.url()),
    recipientOrigin: yield* origin(prepared.action),
  });

  const validate = Effect.gen(function* () {
    if (
      frame.detached ||
      (yield* origin(page.url())) !== target.topOrigin ||
      (yield* origin(frame.url())) !== target.frameOrigin
    )
      return yield* fail("stale-target");

    const current = yield* remote(() =>
      held.evaluate((state) => {
        if (typeof state !== "object" || state === null) return null;

        return Reflect.apply(Reflect.get(state, "target"), state, []);
      }),
    ).pipe(Effect.flatMap(inspect));

    if ((yield* origin(current.action)) !== target.recipientOrigin)
      return yield* fail("stale-target");
  });

  const binding = Effect.gen(function* () {
    const nodes: Array<number> = [];

    for (let index = 0; index <= request.fields.length + 1; index++) {
      const handles = yield* acquire(async () => [
        await held.evaluateHandle((state, index) => {
          if (typeof state !== "object" || state === null) return null;

          return Reflect.apply(Reflect.get(state, "node"), state, [index]);
        }, index),
      ]);

      const element = handles[0]?.asElement();

      if (element === null || element === undefined) return yield* fail("stale-target");
      nodes.push(yield* remote(() => element.backendNodeId()));
    }
    yield* validate;

    const digest = yield* remote(() =>
      crypto.subtle.digest("SHA-256", new TextEncoder().encode(prepared.shape)),
    );

    return CredentialBinding.make({
      version: 1,
      pageId: page.mainFrame()._id,
      frameId: frame._id,
      target,
      nodes,
      shape: Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join(
        "",
      ),
    });
  });

  return { held, target, validate, binding };
});

/** Bind a host-owned request to this exact document and fields without reading their values. */
export const prepareCredential = Effect.fnUntraced(
  function* (input: CredentialForm) {
    const form = yield* openForm(input);

    return yield* form.binding;
  },
  Effect.scoped,
  Effect.withTracerEnabled(false),
);

/**
 * Fill explicit native fields on an existing page without submitting. The owning browser session
 * serializes operations and closes on timeout, interruption or uncertain dispatch; this function releases
 * only its local handles. Ordinary native observations remain available, including credential echoes.
 */
export const fillCredential = Effect.fnUntraced(function* (
  input: FillCredentialRequest,
): Effect.fn.Return<
  CredentialFillResult,
  CredentialFillError,
  BrowserCredentialAccess | BrowserSessionPage
> {
  const { commandTimeoutMillis } = yield* BrowserSessionPage;
  let dispatch: typeof CredentialDispatch.Type = "not-dispatched";
  let filled = 0;

  const fail = (reason: CredentialFillError["reason"]) =>
    CredentialFillError.make({ reason, dispatch, filled, cleanup: "not-requested" });

  const remote = <A>(run: (signal: AbortSignal) => Promise<A>) =>
    Effect.tryPromise({
      try: async (signal) => {
        try {
          return { ok: true as const, value: await run(signal) };
        } catch {
          return { ok: false as const };
        }
      },
      catch: () => fail("provider"),
    }).pipe(
      Effect.flatMap((result) =>
        result.ok ? Effect.succeed(result.value) : Effect.fail(fail("provider")),
      ),
    );

  return yield* Effect.gen(function* () {
    const request = yield* Schema.decodeEffect(FillCredentialRequest, {
      onExcessProperty: "error",
    })(input).pipe(Effect.mapError(() => fail("denied")));

    const form = yield* openForm(
      CredentialForm.make({
        kind: request.kind,
        fields: request.fields,
        ...(request.frame === undefined ? {} : { frame: request.frame }),
      }),
    );

    const { held, target } = form;
    const validate = form.validate.pipe(Effect.mapError((error) => fail(error.reason)));
    const roles = request.fields.map((field) => field.role);

    if (request.binding !== undefined) {
      const actual = yield* form.binding;
      const expected = request.binding;

      if (
        actual.pageId !== expected.pageId ||
        actual.frameId !== expected.frameId ||
        actual.shape !== expected.shape ||
        actual.nodes.length !== expected.nodes.length ||
        actual.nodes.some((node, index) => node !== expected.nodes[index]) ||
        actual.target.topOrigin !== expected.target.topOrigin ||
        actual.target.frameOrigin !== expected.target.frameOrigin ||
        actual.target.recipientOrigin !== expected.target.recipientOrigin
      )
        return yield* fail("stale-target");
    }
    const access = yield* BrowserCredentialAccess;

    const authorization: CredentialAccessRequest = {
      credential: request.credential,
      kind: request.kind,
      target,
      roles,
    };

    const authorize = Effect.suspend(() => access.authorize(authorization)).pipe(
      Effect.mapError((error) => fail(error.reason)),
    );

    yield* authorize;
    yield* validate;

    const raw = yield* access
      .resolve(authorization)
      .pipe(Effect.mapError((error) => fail(error.reason)));

    const material = yield* Schema.decodeEffect(
      request.kind === "login" ? LoginCredential : CardCredential,
    )(raw).pipe(Effect.mapError(() => fail("resolver")));

    for (const role of roles)
      if (secretFor(material, role) === undefined) return yield* fail("missing-credential");
    for (const [index, role] of roles.entries()) {
      yield* authorize;
      yield* validate;
      const value = secretFor(material, role);

      if (value === undefined) return yield* fail("missing-credential");
      const previous = dispatch;

      const result = yield* remote((signal) => {
        if (signal.aborted) return Promise.reject(fail("provider"));
        dispatch = "possibly-dispatched";

        return held.evaluate(
          (state, field, secret) => {
            if (typeof state !== "object" || state === null) return "stale-target";

            return Reflect.apply(Reflect.get(state, "fill"), state, [field, secret]);
          },
          index,
          Redacted.value(value),
        );
      }).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(WriteReply)),
        Effect.mapError(() => fail("provider")),
      );

      if (result === "stale-target" || result === "unsupported-before-write") {
        dispatch = previous;

        return yield* fail(result === "stale-target" ? "stale-target" : "unsupported");
      }
      if (result === "unsupported") return yield* fail("unsupported");
      dispatch = "dispatched";
      filled++;
    }

    return CredentialFillResult.make({ dispatch: "dispatched", filled });
  }).pipe(
    Effect.scoped,
    Effect.timeoutOrElse({
      duration: commandTimeoutMillis,
      orElse: () => Effect.fail(fail("timeout")),
    }),
    Effect.catchCause((cause) => {
      if (Cause.hasInterrupts(cause)) return Effect.interrupt;
      if (
        cause.reasons.some(
          (reason) =>
            reason._tag === "Die" ||
            (reason._tag === "Fail" && !Schema.is(CredentialFillError)(reason.error)),
        )
      )
        return Effect.fail(fail("provider"));

      const expected = cause.reasons.find(
        (reason) => reason._tag === "Fail" && Schema.is(CredentialFillError)(reason.error),
      );

      return Effect.fail(
        fail(
          expected?._tag === "Fail" && Schema.is(CredentialFillError)(expected.error)
            ? expected.error.reason
            : "provider",
        ),
      );
    }),
    Effect.withTracerEnabled(false),
  );
});
