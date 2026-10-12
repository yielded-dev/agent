import {
  EmailAcceptanceUnknown,
  EmailDelivery,
  EmailRenderer,
  type EmailMessage,
  type EmailTemplate,
} from "@yielded/auth/EmailDelivery";
import { DateTime, Effect, Layer, Redacted, Schema } from "effect";

const Code = Schema.String.check(Schema.isPattern(/^[0-9]{6}$/));

export const renderCodeEmail = (code: string, minutes: number) => ({
  subject: "Your Elsewhere sign-in code",
  text: `Your Elsewhere code is ${code}. It expires in ${minutes} minutes. Enter it in the browser where you requested it. If you did not request this code, ignore this email.`,
  html: `<!doctype html><html lang="en"><body style="margin:0;background:#faf8f3;color:#242922;font-family:Arial,sans-serif"><table role="presentation" width="100%"><tr><td align="center" style="padding:48px 16px"><table role="presentation" style="max-width:440px;background:white;border:1px solid #e3e5da;border-radius:16px"><tr><td style="padding:36px"><p style="color:#64764c">ELSEWHERE</p><h1 style="font-size:24px">Your next trip starts here.</h1><p>Enter this code in the browser where you requested it. It expires in ${minutes} minutes.</p><p style="font-family:monospace;font-size:36px;letter-spacing:8px">${code}</p><p style="font-size:13px;color:#677060">If you did not request this code, you can ignore this email.</p></td></tr></table></td></tr></table></body></html>`,
});

export const emailRendererLayer = Layer.succeed(EmailRenderer, {
  render: Effect.fn("Auth.renderEmail")(function* (message: EmailTemplate) {
    if (message.content._tag !== "Code")
      return yield* Effect.die("This application sends numeric email codes");

    const code = yield* Schema.decodeEffect(Code)(Redacted.value(message.content.code)).pipe(
      Effect.orDie,
    );

    const now = DateTime.toEpochMillis(yield* DateTime.now);
    const minutes = Math.max(1, Math.ceil((message.expiresAtMillis - now) / 60_000));
    const rendered = renderCodeEmail(code, minutes);

    return {
      subject: rendered.subject,
      text: Redacted.make(rendered.text),
      html: Redacted.make(rendered.html),
    };
  }),
});

/** Await provider acceptance. An uncertain send is never automatically repeated. */
export const emailDeliveryLayer = (binding: SendEmail, from: string) =>
  Layer.succeed(EmailDelivery, {
    send: Effect.fn("Auth.deliverEmail")(function* (message: EmailMessage) {
      yield* Effect.tryPromise({
        try: () =>
          binding.send({
            from: { name: "Elsewhere", email: from },
            to: message.to,
            subject: message.subject,
            text: Redacted.value(message.text),
            ...(message.html === undefined ? {} : { html: Redacted.value(message.html) }),
          }),
        catch: () => EmailAcceptanceUnknown.make(),
      }).pipe(
        Effect.timeout("10 seconds"),
        Effect.mapError(() => EmailAcceptanceUnknown.make()),
      );
    }),
  });
