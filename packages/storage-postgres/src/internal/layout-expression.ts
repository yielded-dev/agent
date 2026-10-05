/** Closed expression vocabulary for the frozen Postgres layout. */
export type LayoutExpression =
  | readonly ["column" | "text" | "json" | "length", string]
  | readonly ["integer", 0 | 1]
  | readonly ["notNull", LayoutExpression]
  | readonly ["eq" | "ne" | "gt", LayoutExpression, LayoutExpression]
  | readonly ["in" | "notIn", LayoutExpression, ReadonlyArray<string>]
  | readonly ["and", ReadonlyArray<LayoutExpression>];

/** Decode deparser syntax into meaning; unknown tokens/forms fail closed. Never execute it. */
const readExpressions = (input: string): ReadonlyArray<LayoutExpression> | undefined => {
  if (input.length > 16_384) return undefined;
  const tokens: Array<{ kind: string; value: string }> = [];

  const token =
    /\s+|"(?:[^"]|"")*"|'(?:[^']|'')*'|->>|::|<>|[a-z_][a-z_0-9]*|[01](?![0-9])|[=(),[\]>.]/giy;

  while (token.lastIndex < input.length) {
    const match = token.exec(input)?.[0];

    if (match === undefined || tokens.length >= 512) return undefined;
    if (/^\s/.test(match)) continue;
    const quote = match[0];

    tokens.push(
      quote === '"' || quote === "'"
        ? { kind: quote, value: match.slice(1, -1).replaceAll(quote + quote, quote) }
        : { kind: "bare", value: match.toLowerCase() },
    );
  }
  let position = 0;

  const take = (value: string) => {
    if (tokens[position]?.kind !== "bare" || tokens[position]?.value !== value) return false;
    position++;

    return true;
  };

  const identifier = (): string | undefined => {
    const next = tokens[position];

    if (next === undefined || next.kind === "'" || !/^[a-z_][a-z_0-9]*$/i.test(next.value))
      return undefined;
    position++;
    if (!take(".")) return next.value;
    const member = identifier();

    return next.value === "pg_catalog" && member !== undefined ? `pg_catalog.${member}` : undefined;
  };

  const operand = (): LayoutExpression | undefined => {
    let value: LayoutExpression | undefined;
    const next = tokens[position];

    if (take("(")) {
      value = expression();
      if (!take(")")) return undefined;
    } else if (next?.kind === "'") {
      position++;
      value = ["text", next.value];
      if (take("::")) {
        const type = identifier();

        if (type !== "text" && type !== "pg_catalog.text") return undefined;
      }
    } else if (take("0")) value = ["integer", 0];
    else if (take("1")) value = ["integer", 1];
    else {
      const name = identifier();

      if (name === undefined) return undefined;
      if ((name === "length" || name === "pg_catalog.length") && take("(")) {
        const column = identifier();

        if (column === undefined || !take(")")) return undefined;
        value = ["length", column];
      } else value = ["column", name];
    }
    if (take("->>")) {
      const key = operand();

      if (value?.[0] !== "column" || value[1] !== "read_metadata" || key?.[0] !== "text")
        return undefined;

      return ["json", key[1]];
    }

    return value;
  };

  const textList = (array: boolean): ReadonlyArray<string> | undefined => {
    if (!take("(")) return undefined;
    if (array && (!take("array") || !take("["))) return undefined;
    const values: Array<string> = [];

    do {
      const item = operand();

      if (item?.[0] !== "text") return undefined;
      values.push(item[1]);
    } while (take(","));
    if (array && !take("]")) return undefined;

    return take(")") ? values : undefined;
  };

  const comparison = (): LayoutExpression | undefined => {
    const left = operand();

    if (left === undefined) return undefined;
    if (take("is")) return take("not") && take("null") ? ["notNull", left] : undefined;
    const negated = take("not");

    if (negated || take("in")) {
      if (negated && !take("in")) return undefined;
      const values = textList(false);

      return values === undefined ? undefined : [negated ? "notIn" : "in", left, values];
    }
    const operator = take("=") ? "eq" : take("<>") ? "ne" : take(">") ? "gt" : undefined;

    if (operator === undefined) return left;
    if ((operator === "eq" && take("any")) || (operator === "ne" && take("all"))) {
      const values = textList(true);

      return values === undefined ? undefined : [operator === "eq" ? "in" : "notIn", left, values];
    }
    const right = operand();

    return right === undefined ? undefined : [operator, left, right];
  };

  const expression = (): LayoutExpression | undefined => {
    const parts: Array<LayoutExpression> = [];

    do {
      const part = comparison();

      if (part === undefined) return undefined;
      parts.push(...(part[0] === "and" ? part[1] : [part]));
    } while (take("and"));

    return parts.length === 1 ? parts[0] : ["and", parts];
  };

  const expressions: Array<LayoutExpression> = [];

  do {
    const value = expression();

    if (value === undefined) return undefined;
    expressions.push(value);
  } while (take(","));

  return position === tokens.length ? expressions : undefined;
};

export const matchesLayoutExpressions = (
  actual: string | null,
  expected: ReadonlyArray<LayoutExpression>,
): boolean =>
  actual === null
    ? expected.length === 0
    : JSON.stringify(readExpressions(actual)) === JSON.stringify(expected);
