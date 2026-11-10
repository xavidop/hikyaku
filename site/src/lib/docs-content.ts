// Shared source for the /llms.txt and /llms-full.txt endpoints.
//
// These are rendered as ordinary Astro static endpoints (src/pages/*.txt.ts),
// so the files exist under `astro dev` AND `astro build` alike - unlike a
// build-only integration whose astro:build:done hook never runs in the dev
// server and 404s there. Every doc page's raw Markdown is read at build time
// via import.meta.glob so both endpoints render from one place.

export const LLMS_TITLE = "hikyaku";
export const LLMS_DESCRIPTION =
  "hikyaku (飛脚) is a message relay written in Go and configured with one YAML spec (hikyaku.yaml). " +
  "It reads from one or more endpoints and delivers to one or more endpoints: sources (listen, poll, " +
  "subscribe) over http/https, ws/wss, sse, stdin and file; destinations over the same protocols plus " +
  "stdout, stderr, groups and a fault-injecting test sink. Routes pick destinations with a static, " +
  "rules (CEL), sticky or AI router; the AI router runs on Genkit Go, with System One decision models " +
  "(typed answers, calibrated probabilities) by default and any generative model as an alternative, " +
  "and can route with the history of past calls kept in Redis. Every layer retries and falls back: " +
  "destination retries, breakers, retry budgets, fallback chains, groups, hedging, model and router " +
  "fallbacks, and a dead-letter queue with replay. One binary runs as a CLI, a container image and a Helm chart.";
// No absolute URL here: every link in these files is built with absoluteUrl
// from Astro's own `site` and `base`, and a domain written a second time here
// is one that goes stale the day the site moves.
export const LLMS_NOTES =
  "Auto-generated from the hikyaku documentation. The spec reference page is generated from the Go structs, " +
  "and the JSON Schema of hikyaku.yaml is served next to these files at schema/hikyaku.schema.json.";

// Every doc page, raw (frontmatter included), keyed by project-absolute path.
const rawPages = import.meta.glob("/src/pages/docs/**/*.md", {
  query: "?raw",
  import: "default",
  eager: true,
}) as Record<string, string>;

export interface DocPage {
  /** Route-relative path, e.g. "/docs/routing". */
  path: string;
  title: string;
  /** Markdown body with the frontmatter block stripped. */
  body: string;
}

const FRONTMATTER = /^---\r?\n([\s\S]*?)\r?\n---\r?\n?/;

function routeFor(fileKey: string): string {
  let p = fileKey.replace(/^\/src\/pages/, "").replace(/\.md$/, "");
  if (p.endsWith("/index")) p = p.slice(0, -"/index".length);
  return p || "/";
}

function titleFrom(frontmatter: string, body: string, route: string): string {
  const fm = frontmatter.match(/^title:\s*(.+?)\s*$/m);
  if (fm) return fm[1].replace(/^["']|["']$/g, "");
  const h1 = body.match(/^#\s+(.+?)\s*$/m);
  if (h1) return h1[1];
  return route.split("/").pop() || route;
}

// Pages in the order of the docs sidebar (DocsLayout.astro); anything else
// follows, ordered by title.
const ORDER = [
  "/docs",
  "/docs/install",
  "/docs/quickstart",
  "/docs/concepts",
  "/docs/spec",
  "/docs/spec-reference",
  "/docs/protocols",
  "/docs/routing",
  "/docs/history",
  "/docs/resilience",
  "/docs/transforms",
  "/docs/cli",
  "/docs/deploy",
  "/docs/observability",
  "/docs/security",
  "/docs/examples",
  "/docs/contributing",
];

export function collectDocs(): DocPage[] {
  const pages: DocPage[] = Object.entries(rawPages).map(([key, raw]) => {
    const route = routeFor(key);
    const m = raw.match(FRONTMATTER);
    const frontmatter = m ? m[1] : "";
    const body = (m ? raw.slice(m[0].length) : raw).trim();
    return { path: route, title: titleFrom(frontmatter, body, route), body };
  });
  pages.sort((a, b) => {
    const ia = ORDER.indexOf(a.path);
    const ib = ORDER.indexOf(b.path);
    if (ia !== -1 || ib !== -1) {
      return (ia === -1 ? Infinity : ia) - (ib === -1 ? Infinity : ib);
    }
    return a.title.localeCompare(b.title);
  });
  return pages;
}

// Absolute URL honouring the configured base and site; falls back to a
// root-relative path when the site is not configured.
export function absoluteUrl(route: string, site: URL | undefined): string {
  const base = import.meta.env.BASE_URL || "/";
  const joined = (base.replace(/\/$/, "") + route).replace(/\/{2,}/g, "/");
  return site ? new URL(joined, site).href : joined;
}
