// Docs assets are stored under /agent/, and Cloudflare's HTML handling redirects
// with that prefix already attached. This Worker does not strip it.
//
// HTML handling uses a temporary 307 for directory canonicalization. `/agent`
// and `/agent/index.html` (and the `/agent/index` alias) are answered here with
// 308 so the permanent target is `/agent/`. Other slash redirects stay on the
// asset layer, which keeps the /agent prefix.
//
// `/agent/404` is a real `404.html` asset, so the asset layer would serve it
// with status 200. These paths are forced to 404. Unknown paths stay on
// `not_found_handling: 404-page`.

const AGENT_DIRECTORY = "/agent/";

export const docsDirectoryRedirects = ["/agent", "/agent/index.html", "/agent/index"] as const;

export const docsNotFoundPaths = [
  "/agent/404",
  "/agent/404/",
  "/agent/404.html",
  "/agent/404/index",
  "/agent/404/index.html",
] as const;

export const docsWorkerFirstPaths = [...docsDirectoryRedirects, ...docsNotFoundPaths];

const directoryRedirects = new Set<string>(docsDirectoryRedirects);
const notFoundPaths = new Set<string>(docsNotFoundPaths);

interface DocsAssets {
  readonly ASSETS: {
    fetch(request: Request): Promise<Response>;
  };
}

const permanentDirectoryRedirect = (requestUrl: URL): Response => {
  const target = new URL(requestUrl);
  target.pathname = AGENT_DIRECTORY;
  return Response.redirect(target, 308);
};

export default {
  async fetch(request: Request, env: DocsAssets): Promise<Response> {
    const url = new URL(request.url);

    if (directoryRedirects.has(url.pathname)) return permanentDirectoryRedirect(url);

    if (notFoundPaths.has(url.pathname)) {
      // `/agent/404` is the HTML-handling URL that serves `404.html` with a body.
      // ASSETS.fetch does not re-enter this Worker.
      const pageUrl = new URL(request.url);
      pageUrl.pathname = "/agent/404";
      const page = await env.ASSETS.fetch(new Request(pageUrl, request));
      return new Response(page.body, {
        status: 404,
        statusText: "Not Found",
        headers: page.headers,
      });
    }

    return env.ASSETS.fetch(request);
  },
};
