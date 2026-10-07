// Cloudflare evaluates one dynamic-redirect expression per rule, and
// regex_replace can appear only once in that expression. HTML normalization
// and the trailing-slash addition are separate rules so each stays a single
// replacement. Static files (a dot in the last segment) keep their path.

const origin = "https://yielded.dev/agent";

/** Regex source the Cloudflare regex engine receives for `.html` suffixes. */
const htmlSuffix = String.raw`(?:/index\.html|\.html)$`;

/** Regex source for "the last path segment contains a dot". */
const fileExtension = String.raw`/[^/]*\.[^/]*$`;

export const legacyDocsHost = 'http.host in {"effect-agent.com" "www.effect-agent.com"}';

const redirect = (description: string, expression: string, target: string) => ({
  action: "redirect" as const,
  description,
  expression,
  actionParameters: {
    fromValue: {
      statusCode: 301 as const,
      preserveQueryString: true,
      targetUrl: { expression: target },
    },
  },
});

export const legacyDocsRedirectRules = [
  redirect(
    "Move legacy .html documentation URLs to their trailing-slash pages",
    `${legacyDocsHost} and ends_with(http.request.uri.path, ".html")`,
    `concat("${origin}", regex_replace(http.request.uri.path, ${JSON.stringify(htmlSuffix)}, "/"))`,
  ),
  redirect(
    "Move extensionless legacy documentation URLs to their trailing-slash pages",
    `${legacyDocsHost} and not ends_with(http.request.uri.path, "/") and not http.request.uri.path matches r"${fileExtension}"`,
    `concat("${origin}", http.request.uri.path, "/")`,
  ),
  redirect(
    "Move agent documentation to yielded.dev/agent",
    legacyDocsHost,
    `concat("${origin}", http.request.uri.path)`,
  ),
];

/** Same mapping the redirect rules implement. Query strings are preserved separately. */
export const legacyDocsTarget = (path: string): string => {
  if (path.endsWith(".html")) return origin + path.replace(new RegExp(htmlSuffix), "/");
  if (!path.endsWith("/") && !new RegExp(fileExtension).test(path)) return `${origin}${path}/`;
  return origin + path;
};
