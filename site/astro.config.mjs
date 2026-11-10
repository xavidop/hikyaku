import { defineConfig } from "astro/config";
import sitemap from "@astrojs/sitemap";
import { unified } from "@astrojs/markdown-remark";
import { visit } from "unist-util-visit";
import { SITE, BASE, BASE_PREFIX } from "./base.mjs";
import { hikyakuLight, hikyakuDark } from "./src/lib/shiki-themes.mjs";

// The site's origin and base path live in base.mjs, the ONE place they are
// declared: everything else (llms.txt/llms-full.txt, the sitemap, canonical
// links, robots.txt, the link checker) derives them from Astro's `site`/`base`
// context or from that module rather than hardcoding them a second time.

// remarkTableCode marks short inline code inside a table so CSS can keep it on
// one line. A table cell is a narrow column, and the browser treats every
// hyphen and dot in `--check-secrets` or `retry.max_backoff` as a place it may
// break: the identifier then arrives split across two lines inside a pill
// that looks torn in half. Anything short enough to fit a column is flagged
// here and set nowrap in DocsLayout; anything longer (metric names with
// labels, endpoint URLs) is left wrappable, because forcing those onto one
// line is what pushed whole tables wider than the content column.
// How long "short" is depends on how many columns share the row: a two-column
// table can give an identifier most of a column, a five-column comparison
// table cannot.
const TABLE_CODE_ATOM_MAX = { 2: 34, 3: 30, 4: 22 };
const TABLE_CODE_ATOM_MAX_DENSE = 16;

function remarkTableCode() {
  return (tree) => {
    visit(tree, "table", (table) => {
      const columns = Math.max(
        ...table.children.map((row) => (row.children ? row.children.length : 0)),
      );
      const max = TABLE_CODE_ATOM_MAX[columns] ?? TABLE_CODE_ATOM_MAX_DENSE;
      visit(table, "inlineCode", (node) => {
        const value = String(node.value);
        if (value.length > max || /\s/.test(value)) return;
        node.data = node.data || {};
        node.data.hProperties = {
          ...(node.data.hProperties || {}),
          className: ["code-atom"],
        };
      });
    });
  };
}

// remarkBaseLinks prefixes root-relative Markdown links and images with the
// configured base. Astro rewrites `base` into component hrefs but NOT into
// links written inside Markdown, so on a project Pages site (base "/hikyaku/")
// a page linking "/docs/routing/" resolves to <origin>/docs/routing/ and 404s:
// the docs are at <origin>/hikyaku/docs/routing/. Every cross-link in the docs
// tree is written root-relative, so without this the whole tree breaks the
// moment it is served under a base.
//
// Anchors, external URLs and protocol-relative URLs are left alone, as is
// anything already carrying the base, so the transform is idempotent.
function remarkBaseLinks() {
  const prefix = BASE_PREFIX;
  return (tree) => {
    if (!prefix) return;
    const fix = (node) => {
      const url = node.url;
      if (typeof url !== "string") return;
      if (!url.startsWith("/") || url.startsWith("//")) return;
      if (url === prefix || url.startsWith(prefix + "/")) return;
      node.url = prefix + url;
    };
    visit(tree, "link", fix);
    visit(tree, "image", fix);
    visit(tree, "definition", fix);
  };
}

export default defineConfig({
  // The origin the site is served from. Canonical links, the sitemap and
  // robots.txt's Sitemap line are all derived from it.
  site: SITE,
  base: BASE,
  // Internal doc links are root-relative (/docs/...), so they resolve the same
  // with or without a trailing slash; "ignore" lets the dev server serve both
  // forms rather than 404-ing bare URLs.
  trailingSlash: "ignore",
  devToolbar: { enabled: false },
  build: { format: "directory" },
  // /llms.txt (index) and /llms-full.txt (all docs as one Markdown file) are
  // native static endpoints under src/pages, so they render in dev and build
  // alike. See src/lib/docs-content.ts.
  integrations: [sitemap()],
  markdown: {
    // The remark plugins go to the unified processor itself: Astro 7
    // deprecates the top-level markdown.remarkPlugins key.
    processor: unified({ remarkPlugins: [remarkBaseLinks, remarkTableCode] }),
    shikiConfig: {
      // Two themes, not one: a single light theme in dark mode leaves every
      // code block in light-theme colours, and near-black punctuation on a
      // dark background is invisible. Shiki emits the second theme as a
      // --shiki-dark custom property per token; DocsLayout switches to it
      // under the same three conditions the rest of the palette uses.
      // Both are the brand themes of src/lib/shiki-themes.mjs.
      themes: {
        light: hikyakuLight,
        dark: hikyakuDark,
      },
      wrap: false,
    },
  },
});
