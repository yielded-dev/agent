import type { RuleTester } from "vite-plus/lint/plugins-dev";

type Rule = Parameters<RuleTester["run"]>[1];

const nativeErrors = new Set([
  "Error",
  "EvalError",
  "RangeError",
  "ReferenceError",
  "SyntaxError",
  "TypeError",
  "URIError",
  "AggregateError",
]);

const noNativeError = {
  meta: {
    type: "problem",
    schema: [],
    messages: {
      native:
        "Use a tagged error and fail in the Effect error channel. Document native host exceptions at the statement.",
    },
  },
  create(context) {
    const inspect = (node: Parameters<typeof context.sourceCode.getScope>[0]) => {
      if (node.type !== "NewExpression" && node.type !== "CallExpression") return;
      const callee = node.callee;

      const identifier =
        callee.type === "Identifier" && nativeErrors.has(callee.name)
          ? callee
          : callee.type === "MemberExpression" &&
              callee.object.type === "Identifier" &&
              ["globalThis", "window", "self", "global"].includes(callee.object.name) &&
              ((!callee.computed &&
                callee.property.type === "Identifier" &&
                nativeErrors.has(callee.property.name)) ||
                (callee.computed &&
                  callee.property.type === "Literal" &&
                  typeof callee.property.value === "string" &&
                  nativeErrors.has(callee.property.value)))
            ? callee.object
            : undefined;

      if (identifier === undefined) return;

      let scope: ReturnType<typeof context.sourceCode.getScope> | null =
        context.sourceCode.getScope(identifier);

      while (scope !== null) {
        const variable = scope.set.get(identifier.name);

        if (variable !== undefined) {
          if (variable.defs.length > 0) return;
          break;
        }
        scope = scope.upper;
      }
      context.report({ node, messageId: "native" });
    };

    return { NewExpression: inspect, CallExpression: inspect };
  },
} satisfies Rule;

const noUntypedThrow = {
  meta: {
    type: "problem",
    schema: [],
    messages: {
      failure:
        "Return yield* ErrorClass.make(...) or Effect.fail(ErrorClass.make(...)); do not throw inside Effect workflows, including try/tryPromise callbacks.",
    },
  },
  create(context) {
    return {
      ThrowStatement(node) {
        context.report({ node, messageId: "failure" });
      },
    };
  },
} satisfies Rule;

export default {
  meta: { name: "effect-agent-effect" },
  rules: { "no-native-error": noNativeError, "no-untyped-throw": noUntypedThrow },
};
