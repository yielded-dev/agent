import starlight from "@astrojs/starlight";
import yieldedTheme from "@yielded/starlight-theme";
import { defineConfig } from "astro/config";
import starlightLinksValidator from "starlight-links-validator";

import { snippetPlugins } from "./integrations/snippets.ts";
import { socialImages } from "./integrations/social-images.ts";

export default defineConfig({
  site: "https://yielded.dev",
  base: "/agent",
  trailingSlash: "always",
  integrations: [
    starlight({
      title: "Yielded Agent",
      description: "Durable agent Threads for TypeScript, built on Effect and Effect AI.",
      favicon: "/mark.svg",
      lastUpdated: true,
      plugins: [yieldedTheme({ library: "agent" }), starlightLinksValidator()],
      routeMiddleware: "./src/route-middleware.ts",
      tableOfContents: { minHeadingLevel: 2, maxHeadingLevel: 3 },
      expressiveCode: { plugins: snippetPlugins },
      head: [
        {
          tag: "link",
          attrs: {
            rel: "icon",
            type: "image/png",
            sizes: "32x32",
            href: "/agent/favicon-32x32.png",
          },
        },
        {
          tag: "link",
          attrs: { rel: "apple-touch-icon", sizes: "180x180", href: "/agent/apple-touch-icon.png" },
        },
        { tag: "meta", attrs: { name: "theme-color", content: "#161714" } },
        { tag: "meta", attrs: { property: "og:site_name", content: "Yielded Agent" } },
        { tag: "meta", attrs: { property: "og:locale", content: "en_US" } },
      ],
      sidebar: [
        {
          label: "Understand Yielded Agent",
          items: [
            { label: "Overview", link: "/" },
            { label: "How it works", link: "/concepts/runtime-model/" },
            { label: "Durability guarantees", link: "/concepts/durability/" },
            { label: "Where it runs", link: "/platforms/" },
            { label: "Getting started", link: "/guide/getting-started/" },
          ],
        },
        {
          label: "Build an agent",
          items: [
            { label: "Guide map", link: "/guide/" },
            { label: "Agent definitions", link: "/guide/agents/" },
            { label: "Tools", link: "/guide/tools/" },
            { label: "Run and stream", link: "/guide/run-agents/" },
            { label: "Threads", link: "/guide/threads/" },
            { label: "Context management", link: "/guide/context-management/" },
            { label: "Budgets and limits", link: "/concepts/budgets/" },
          ],
        },
        {
          label: "Add capabilities",
          items: [
            { label: "Subagents", link: "/guide/subagents/" },
            { label: "Messaging", link: "/guide/messaging/" },
            { label: "Sandbox", link: "/guide/sandbox/" },
            { label: "Code Mode", link: "/guide/code-mode/" },
            { label: "Browser tools", link: "/guide/browser/" },
          ],
        },
        {
          label: "Deploy and operate",
          items: [
            { label: "Node.js", link: "/platforms/node/" },
            { label: "Cloudflare", link: "/platforms/cloudflare/" },
            { label: "Choose storage", link: "/storage/" },
            { label: "In-memory", link: "/storage/memory/" },
            { label: "SQLite", link: "/storage/sqlite/" },
            { label: "PostgreSQL", link: "/storage/postgres/" },
            { label: "Cloudflare storage", link: "/storage/cloudflare/" },
            { label: "Testing", link: "/guide/testing/" },
            { label: "Operations", link: "/guide/operations/" },
          ],
        },
        {
          label: "Reference",
          items: [
            { label: "Reference map", link: "/reference/" },
            { label: "Package map", link: "/reference/packages/" },
            { label: "Decision models", link: "/reference/decision-models/" },
            { label: "Subagent options", link: "/reference/subagents/" },
            { label: "Architecture map", link: "/concepts/" },
          ],
        },
      ],
    }),
    socialImages(),
  ],
});
