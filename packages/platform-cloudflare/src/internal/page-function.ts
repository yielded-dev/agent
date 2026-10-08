/**
 * Puppeteer sends a page function's source to the browser. Bundlers that keep function names,
 * including Wrangler by default, insert `__name(...)` calls into that source, and the page has no
 * such helper. The returned function's source declares a local no-op `__name` first.
 */
export const pageFunction = <A extends ReadonlyArray<unknown>, R>(
  fn: (...args: A) => R,
): ((...args: A) => R) =>
  Object.assign((...args: A) => fn(...args), {
    toString: () =>
      `(...args) => { const __name = (target) => target; return (${Function.prototype.toString.call(fn)})(...args); }`,
  });
