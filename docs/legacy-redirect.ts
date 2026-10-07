// The effect-agent.com zone cannot use regex_replace in a dynamic redirect
// (that function needs a Business or WAF Advanced plan). The docs Worker
// applies this mapping and answers with one 301.

const origin = "https://yielded.dev/agent";

const htmlSuffix = /(?:\/index\.html|\.html)$/;
const fileExtension = /\/[^/]*\.[^/]*$/;

export const legacyDocsHosts = new Set(["effect-agent.com", "www.effect-agent.com"]);

/** Canonical docs URL for a legacy-host path. Query strings are copied separately. */
export const legacyDocsTarget = (path: string): string => {
  if (path.endsWith(".html")) return origin + path.replace(htmlSuffix, "/");
  if (!path.endsWith("/") && !fileExtension.test(path)) return `${origin}${path}/`;

  return origin + path;
};
