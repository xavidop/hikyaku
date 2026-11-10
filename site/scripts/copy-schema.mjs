// Copies the JSON Schema of hikyaku.yaml into public/schema/ before `astro dev`
// and `astro build` (the predev and prebuild scripts), so the site serves it
// at <base>schema/hikyaku.schema.json: the URL of the schema's $id and of the
// `# yaml-language-server: $schema=...` line every spec starts with.
//
// schema/hikyaku.schema.json at the repository root is the source of truth,
// written by `make schema` from the Go structs. The copy under public/schema/
// is a build input only and is not committed (see .gitignore), so it cannot
// drift from the source.

import { copyFileSync, existsSync, mkdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

const here = path.dirname(fileURLToPath(import.meta.url));
const src = path.resolve(here, "../../schema/hikyaku.schema.json");
const dstDir = path.resolve(here, "../public/schema");
const dst = path.join(dstDir, "hikyaku.schema.json");

if (!existsSync(src)) {
  console.error(`copy-schema: ${src} not found; run \`make schema\` at the repository root`);
  process.exit(1);
}
mkdirSync(dstDir, { recursive: true });
copyFileSync(src, dst);
console.log(`copy-schema: ${path.relative(process.cwd(), src)} -> ${path.relative(process.cwd(), dst)}`);
