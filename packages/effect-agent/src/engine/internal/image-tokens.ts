import type * as Prompt from "effect/ai/Prompt";
import * as Base64 from "effect/encoding/Base64";
import * as Result from "effect/Result";
import { imageDimensionsFromData } from "image-dimensions";

const HEADER_BYTES = 65_536;
const BASE64_HEADER_CHARS = Math.ceil(HEADER_BYTES / 3) * 4;
const UNKNOWN_IMAGE_TOKENS = 4_096;

const imageHeader = (data: Prompt.FilePart["data"]): Uint8Array | undefined => {
  if (data instanceof Uint8Array) return data.subarray(0, HEADER_BYTES);
  const source = typeof data === "string" ? data : data.href;
  let offset = 0;

  if (/^data:/i.test(source)) {
    const header = source.slice(0, 1_024);
    const comma = header.indexOf(",");

    if (comma === -1 || !/;base64$/i.test(header.slice(0, comma))) return undefined;
    offset = comma + 1;
  } else if (/^[a-z][a-z0-9+.-]*:/i.test(source.slice(0, 32))) {
    return undefined;
  }

  const prefix = source.slice(offset, offset + BASE64_HEADER_CHARS).replace(/\s/g, "");
  const padded = prefix.padEnd(Math.ceil(prefix.length / 4) * 4, "=");

  return Result.getOrUndefined(Base64.decode(padded))?.subarray(0, HEADER_BYTES);
};

/**
 * A generic context reservation, not provider billing: 1.5 tokens per 32px patch,
 * at least 1,024 tokens. Remote, unsupported, or unreadable images reserve 4,096.
 * Inspect only a bounded header; never fetch, decode pixels, or measure encoded payload size.
 */
export const estimateImageTokens = (part: Prompt.FilePart): number => {
  try {
    const header = imageHeader(part.data);
    const size = header === undefined ? undefined : imageDimensionsFromData(header);

    if (
      size === undefined ||
      !Number.isFinite(size.width) ||
      !Number.isFinite(size.height) ||
      size.width <= 0 ||
      size.height <= 0
    )
      return UNKNOWN_IMAGE_TOKENS;

    return Math.max(
      1_024,
      Math.min(
        Number.MAX_SAFE_INTEGER,
        Math.ceil(Math.ceil(size.width / 32) * Math.ceil(size.height / 32) * 1.5),
      ),
    );
  } catch {
    return UNKNOWN_IMAGE_TOKENS;
  }
};
