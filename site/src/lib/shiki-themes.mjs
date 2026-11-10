// Shiki themes in the hikyaku palette, one per site theme. Stock themes paint
// strings or comments under 4.5:1 on the washi code background (vitesse-light
// strings are 3.8:1, catppuccin-latte comments lower still), so the docs
// carry their own: keys and keywords in kakishibu terracotta, strings in
// matcha sage, numbers and constants in hachimitsu honey, functions and
// types in a deep teal, all darkened (light) or lifted (dark) until each holds
// WCAG AA against --code-bg, --bg, --bg-2 and --bg-3 of its theme.
//
// The token colours match the --code-* tokens of src/styles/global.css, which
// the hand-highlighted snippets of the landing page use. Change both together.
//
// astro.config.mjs passes these as shikiConfig.themes; Shiki writes the light
// colour inline and the dark one as --shiki-dark, which DocsLayout switches to
// under the same three conditions the rest of the palette uses.

const palettes = {
  light: {
    bg: "#FCF8F1",
    fg: "#4A3B35",
    key: "#994524",
    str: "#3B6640",
    num: "#7F5207",
    fn: "#2D6470",
    comment: "#736156",
    punct: "#6B5A51",
  },
  dark: {
    bg: "#251B17",
    fg: "#E8DBCA",
    key: "#EBA27F",
    str: "#A9C9A4",
    num: "#F2B84B",
    fn: "#8CC3CC",
    comment: "#A8968A",
    punct: "#C2B09D",
  },
};

function theme(name, type) {
  const p = palettes[type];
  return {
    name,
    type,
    colors: {
      "editor.background": p.bg,
      "editor.foreground": p.fg,
    },
    tokenColors: [
      { settings: { foreground: p.fg, background: p.bg } },
      {
        scope: ["comment", "punctuation.definition.comment", "string.comment"],
        settings: { foreground: p.comment, fontStyle: "italic" },
      },
      {
        scope: ["string", "string.unquoted", "string.quoted", "markup.inline.raw"],
        settings: { foreground: p.str },
      },
      {
        scope: [
          "constant.numeric",
          "constant.language",
          "constant.character",
          "constant.other",
          "variable.other.constant",
          "entity.other.attribute-name",
          "variable.other.alias",
          "entity.name.type.anchor",
        ],
        settings: { foreground: p.num },
      },
      {
        // YAML and JSON keys, HTML tags, keywords and storage: the words a
        // reader scans for. Shell options (--flag) read the same way.
        scope: [
          "entity.name.tag",
          "support.type.property-name",
          "keyword",
          "storage",
          "storage.type",
          "storage.modifier",
          "constant.other.option",
          "markup.heading",
        ],
        settings: { foreground: p.key },
      },
      {
        scope: [
          "entity.name.function",
          "support.function",
          "meta.function-call.generic",
          "entity.name.command",
          "entity.name.type",
          "entity.name.class",
          "support.type",
          "support.class",
          "storage.type.numeric.go",
          "storage.type.string.go",
          "storage.type.boolean.go",
          "storage.type.byte.go",
          "storage.type.error.go",
        ],
        settings: { foreground: p.fn },
      },
      {
        scope: ["variable", "variable.other", "variable.parameter", "meta.definition.variable"],
        settings: { foreground: p.fg },
      },
      {
        scope: [
          "punctuation",
          "meta.brace",
          "keyword.operator",
          "punctuation.separator",
          "punctuation.definition.sequence.item",
        ],
        settings: { foreground: p.punct },
      },
      {
        // A string's quotes stay with the string, so a value reads as one
        // coloured unit.
        scope: ["punctuation.definition.string.begin", "punctuation.definition.string.end"],
        settings: { foreground: p.str },
      },
      {
        scope: ["variable.other.normal.shell", "variable.other.positional.shell", "punctuation.definition.variable.shell"],
        settings: { foreground: p.num },
      },
      { scope: ["markup.bold"], settings: { fontStyle: "bold" } },
      { scope: ["markup.italic"], settings: { fontStyle: "italic" } },
      { scope: ["invalid"], settings: { foreground: p.key, fontStyle: "underline" } },
    ],
  };
}

export const hikyakuLight = theme("hikyaku-light", "light");
export const hikyakuDark = theme("hikyaku-dark", "dark");
