/** UTF-8 width, including the replacement encoding used for lone UTF-16 surrogates. */
export const codePointUtf8Length = (codePoint: number): number => {
  if (codePoint < 0x80) return 1;
  if (codePoint < 0x800) return 2;
  if (codePoint < 0x10000) return 3;

  return 4;
};

// No Unicode flag: astral characters and lone surrogates both require the exact path.
const nonAscii = /[\u0080-\uFFFF]/;

/** Count encoded bytes without allocating an encoded copy of the text. */
export const utf8ByteLength = (value: string): number => {
  if (!nonAscii.test(value)) return value.length;
  let bytes = 0;
  let index = 0;

  while (index < value.length) {
    const codePoint = value.codePointAt(index) ?? 0;

    bytes += codePointUtf8Length(codePoint);
    index += codePoint > 0xffff ? 2 : 1;
  }

  return bytes;
};
