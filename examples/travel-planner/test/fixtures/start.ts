/** The RPC integration suite leaves SSR rendering outside its boundary. */
export default {
  fetch: (request: Request) =>
    new Response(
      new URL(request.url).pathname === "/travel/login" ||
        new URL(request.url).pathname === "/travel/auth/github/callback"
        ? '<!doctype html><html><head><link rel="stylesheet" href="/travel/assets/login.css"><link rel="modulepreload" href="/travel/assets/login.js"></head><body>Login fixture<script type="module" src="/travel/assets/login.js"></script></body></html>'
        : "SSR fixture",
      {
        headers: { "content-type": "text/html" },
        status: ["/travel/login", "/travel/auth/github/callback"].includes(
          new URL(request.url).pathname,
        )
          ? 200
          : 404,
      },
    ),
};
