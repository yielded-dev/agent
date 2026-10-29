// Implement Effect digests with the Workers native hashing primitive.
// @effect-diagnostics-next-line nodeBuiltinImport:off
import { createHash } from "node:crypto";

import * as BrowserCrypto from "@effect/platform-browser/BrowserCrypto";
import * as CryptoDigest from "effect/crypto/Digest";
import type * as SecureRandom from "effect/crypto/SecureRandom";
import * as WebCrypto from "effect/crypto/WebCrypto";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as Layer from "effect/Layer";

/** Workers use native SHA-256 while retaining BrowserCrypto's services and overrides. */
const digestLayer = Layer.effect(
  CryptoDigest.Digest,
  Effect.gen(function* () {
    const base = yield* CryptoDigest.Digest;
    const webCrypto = yield* WebCrypto.WebCrypto;
    const hostGlobals: object = globalThis;

    const digest: CryptoDigest.Digest["Service"]["digest"] = (algorithm, data) => {
      if (
        algorithm.name !== "SHA-256" ||
        !("crypto" in hostGlobals) ||
        webCrypto !== hostGlobals.crypto ||
        typeof webCrypto.subtle?.digest !== "function"
      )
        return base.digest(algorithm, data);

      return Effect.sync(() => Uint8Array.from(createHash("sha256").update(data).digest()));
    };

    return CryptoDigest.Digest.of({
      digest,
      digestHex: (algorithm, data) => Effect.map(digest(algorithm, data), Hex.encode),
    });
  }),
).pipe(Layer.provide(BrowserCrypto.layerDigest));

export const cloudflareCryptoLayer: Layer.Layer<CryptoDigest.Digest | SecureRandom.SecureRandom> =
  Layer.merge(digestLayer, BrowserCrypto.layerSecureRandom);
