// The site's origin and base path, declared once. astro.config.mjs builds the
// site with them and scripts/check-links.mjs strips the same base before it
// resolves an href against dist/, so the two can never disagree.
//
// The docs are a GitHub Pages project site: https://xavidop.github.io/hikyaku/.
// The Pages workflow passes BASE_PATH (actions/configure-pages reports
// "/hikyaku"); without it the build uses "/hikyaku" too, so a local build and
// `astro dev` serve exactly the URLs production does, schema included
// (/hikyaku/schema/hikyaku.schema.json, the $id of the JSON Schema).
// BASE_PATH=/ builds a root site.
//
// To move to a custom domain, do BOTH: add public/CNAME with the bare domain,
// and change SITE (and the default base) to match. Doing only one leaves the
// built HTML disagreeing with where it is served.
export const SITE = "https://xavidop.github.io";

const rawBase = process.env.BASE_PATH || "/hikyaku";

// Always ends in a slash. import.meta.env.BASE_URL mirrors this value verbatim
// (Astro does not add the slash back), and every page and component builds
// hrefs as `${base}docs/...`: without the slash that concatenation produces
// "/hikyakudocs" instead of "/hikyaku/docs".
export const BASE = rawBase.endsWith("/") ? rawBase : `${rawBase}/`;

// The base without its trailing slash ("" for a root site), the form a
// root-relative href starts with.
export const BASE_PREFIX = BASE.replace(/\/+$/, "");
