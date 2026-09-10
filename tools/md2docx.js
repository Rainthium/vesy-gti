// md2docx.js — сборка Word-версии контракта из его Markdown (docs/contracts/*.md → .docx).
//
// Подмножество Markdown: заголовки #/##/###, абзацы, списки «- » (два уровня) и «1. »,
// таблицы «| … |», блоки ```…```, инлайн **жирный** и `код`. A4, поля 2 см, Calibri 11 /
// Consolas 9, таблицы с рамками и подкрашенной шапкой, колонтитулы «Стр. X из Y».
//
// Запуск (библиотека docx ставится один раз во временную папку, в репозиторий не кладём):
//   mkdir -p /tmp/md2docx && (cd /tmp/md2docx && npm install docx --no-audit --no-fund)
//   NODE_PATH=/tmp/md2docx/node_modules node tools/md2docx.js docs/contracts/customs-events-v1.md /tmp/customs-events-v1.docx
// Проверка структуры (LibreOffice/Word на Mac нет — визуально смотрит Игорь в Word):
//   uv run --no-project --with python-docx python -c "from docx import Document; d=Document('/tmp/customs-events-v1.docx'); print(len(d.paragraphs), len(d.tables))"
// Примечание: pandoc не распознаёт заголовки docx-js как заголовки (особенность pandoc 3.10,
// в Word всё в порядке — проверено 10.09.2026), так что pandoc для проверки не годится.
// История: 09.09.2026 — черновик 0.1 контракта с таможней (customs-events-v1-draft-0.1.docx).
// Конвертер подмножества Markdown контракта в .docx (docx-js)
const fs = require("fs");
const {
  Document, Packer, Paragraph, TextRun, HeadingLevel, Table, TableRow, TableCell,
  WidthType, BorderStyle, ShadingType, AlignmentType, LevelFormat, Footer, Header,
  PageNumber, TabStopType,
} = require("docx");

const [,, srcPath, outPath] = process.argv;
const md = fs.readFileSync(srcPath, "utf8").split(/\r?\n/);

const FONT = "Calibri";
const MONO = "Consolas";
const PAGE_W = 11906, MARGIN = 1134, CONTENT_W = PAGE_W - 2 * MARGIN; // A4, 2 см поля
const HEAD_COLOR = "1F3864";

// ---------- inline: **bold**, `code` ----------
function inline(text, base = {}) {
  const runs = [];
  const re = /(\*\*[^*]+\*\*|`[^`]+`)/g;
  let last = 0, m;
  while ((m = re.exec(text)) !== null) {
    if (m.index > last) runs.push(new TextRun({ text: text.slice(last, m.index), ...base }));
    const tok = m[0];
    if (tok.startsWith("**")) runs.push(new TextRun({ text: tok.slice(2, -2), bold: true, ...base }));
    else runs.push(new TextRun({ text: tok.slice(1, -1), font: MONO, size: (base.size || 22) - 2, shading: { type: ShadingType.CLEAR, fill: "F2F2F2" }, ...(base.bold ? { bold: true } : {}) }));
    last = m.index + tok.length;
  }
  if (last < text.length) runs.push(new TextRun({ text: text.slice(last), ...base }));
  return runs;
}

// ---------- block parse ----------
const blocks = [];
let i = 0, para = [];
function flushPara() { if (para.length) { blocks.push({ t: "p", text: para.join(" ") }); para = []; } }
while (i < md.length) {
  const line = md[i];
  if (line.startsWith("```")) {
    flushPara();
    const lang = line.slice(3).trim(); const lines = []; i++;
    while (i < md.length && !md[i].startsWith("```")) { lines.push(md[i]); i++; }
    i++; blocks.push({ t: "code", lang, lines }); continue;
  }
  if (line.startsWith("|")) {
    flushPara();
    const rows = [];
    while (i < md.length && md[i].startsWith("|")) {
      const cells = md[i].trim().replace(/^\|/, "").replace(/\|$/, "").split("|").map(c => c.trim());
      if (!cells.every(c => /^-{3,}$/.test(c))) rows.push(cells);
      i++;
    }
    blocks.push({ t: "table", rows }); continue;
  }
  const h = /^(#{1,3})\s+(.*)$/.exec(line);
  if (h) { flushPara(); blocks.push({ t: "h", level: h[1].length, text: h[2] }); i++; continue; }
  const b = /^(\s*)-\s+(.*)$/.exec(line);
  if (b) { flushPara(); blocks.push({ t: "li", level: b[1].length >= 2 ? 1 : 0, text: b[2] }); i++; continue; }
  const n = /^(\d+)\.\s+(.*)$/.exec(line);
  if (n) { flushPara(); blocks.push({ t: "ni", num: Number(n[1]), text: n[2] }); i++; continue; }
  if (line.trim() === "---") { flushPara(); blocks.push({ t: "hr" }); i++; continue; }
  if (line.trim() === "") { flushPara(); i++; continue; }
  para.push(line.trim()); i++;
}
flushPara();

// нумерованные списки: каждый список (начинающийся с «1.») — свой экземпляр нумерации
let numLists = 0;
for (let k = 0; k < blocks.length; k++) {
  if (blocks[k].t === "ni") {
    if (blocks[k].num === 1 || k === 0 || blocks[k - 1].t !== "ni") numLists++;
    blocks[k].ref = "num-" + numLists;
  }
}
const numbering = { config: [
  { reference: "bullets", levels: [
    { level: 0, format: LevelFormat.BULLET, text: "•", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 567, hanging: 284 } } } },
    { level: 1, format: LevelFormat.BULLET, text: "–", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 1134, hanging: 284 } } } },
  ] },
] };
for (let k = 1; k <= numLists; k++) numbering.config.push({ reference: "num-" + k, levels: [
  { level: 0, format: LevelFormat.DECIMAL, text: "%1.", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 567, hanging: 340 } } } },
] });

// ---------- render ----------
const border = { style: BorderStyle.SINGLE, size: 4, color: "A6A6A6" };
const borders = { top: border, bottom: border, left: border, right: border, insideHorizontal: border, insideVertical: border };

function table(rows) {
  const ncol = Math.max(...rows.map(r => r.length));
  const avg = Array.from({ length: ncol }, (_, c) => {
    const lens = rows.slice(1).map(r => (r[c] || "").length);
    return Math.max(8, Math.min(60, lens.length ? lens.reduce((a, b) => a + b, 0) / lens.length : 10));
  });
  const sum = avg.reduce((a, b) => a + b, 0);
  const widths = avg.map(a => Math.round(CONTENT_W * a / sum));
  widths[ncol - 1] += CONTENT_W - widths.reduce((a, b) => a + b, 0);
  const trows = rows.map((r, ri) => new TableRow({
    tableHeader: ri === 0, cantSplit: true,
    children: Array.from({ length: ncol }, (_, c) => new TableCell({
      width: { size: widths[c], type: WidthType.DXA },
      shading: ri === 0 ? { type: ShadingType.CLEAR, fill: "DEEAF6", color: "auto" } : undefined,
      margins: { top: 60, bottom: 60, left: 90, right: 90 },
      children: [new Paragraph({ spacing: { before: 0, after: 0 }, children: inline(r[c] || "", ri === 0 ? { size: 19, bold: true } : { size: 19 }) })],
    })),
  }));
  return new Table({ width: { size: CONTENT_W, type: WidthType.DXA }, columnWidths: widths, borders, rows: trows });
}

function codeBlock(lines) {
  return lines.map((l, idx) => new Paragraph({
    shading: { type: ShadingType.CLEAR, fill: "F2F2F2", color: "auto" },
    spacing: { before: idx === 0 ? 120 : 0, after: idx === lines.length - 1 ? 160 : 0, line: 260 },
    indent: { left: 170, right: 170 },
    children: [new TextRun({ text: l.length ? l : " ", font: MONO, size: 18 })],
  }));
}

const children = [];
let titleDone = false;
for (const b of blocks) {
  switch (b.t) {
    case "h": {
      if (b.level === 1 && !titleDone) {
        titleDone = true;
        children.push(new Paragraph({ spacing: { after: 240 }, children: [new TextRun({ text: b.text, bold: true, size: 36, color: HEAD_COLOR })] }));
      } else {
        const lvl = b.level === 2 ? HeadingLevel.HEADING_1 : HeadingLevel.HEADING_2;
        children.push(new Paragraph({ heading: lvl, spacing: { before: b.level === 2 ? 360 : 240, after: 120 }, keepNext: true, children: inline(b.text) }));
      }
      break;
    }
    case "p": children.push(new Paragraph({ spacing: { after: 120 }, children: inline(b.text) })); break;
    case "li": children.push(new Paragraph({ numbering: { reference: "bullets", level: b.level }, spacing: { after: 60 }, children: inline(b.text) })); break;
    case "ni": children.push(new Paragraph({ numbering: { reference: b.ref, level: 0 }, spacing: { after: 60 }, children: inline(b.text) })); break;
    case "table": children.push(table(b.rows)); children.push(new Paragraph({ spacing: { after: 120 }, children: [] })); break;
    case "code": children.push(...codeBlock(b.lines)); break;
    case "hr": children.push(new Paragraph({ spacing: { before: 120, after: 120 }, border: { bottom: { style: BorderStyle.SINGLE, size: 6, color: "A6A6A6", space: 1 } }, children: [] })); break;
  }
}

const doc = new Document({
  creator: "ОАО «ГТИ» — весовая система",
  title: "Контракт передачи операций в таможенную систему, версия 1 (черновик 0.1)",
  styles: {
    default: {
      document: { run: { font: FONT, size: 22 }, paragraph: { spacing: { line: 276 } } },
      heading1: { run: { size: 28, bold: true, color: HEAD_COLOR, font: FONT }, paragraph: { spacing: { before: 360, after: 120 }, keepNext: true, outlineLevel: 0 } },
      heading2: { run: { size: 24, bold: true, color: HEAD_COLOR, font: FONT }, paragraph: { spacing: { before: 240, after: 120 }, keepNext: true, outlineLevel: 1 } },
    },
  },
  numbering,
  sections: [{
    properties: { page: { margin: { top: MARGIN, bottom: MARGIN, left: MARGIN, right: MARGIN } } },
    headers: { default: new Header({ children: [new Paragraph({ alignment: AlignmentType.RIGHT, children: [new TextRun({ text: "Единая весовая система ОАО «ГТИ» · контракт с таможенной системой, v1 · черновик 0.1 от 09.09.2026", size: 16, color: "7F7F7F" })] })] }) },
    footers: { default: new Footer({ children: [new Paragraph({ alignment: AlignmentType.CENTER, children: [
      new TextRun({ text: "Стр. ", size: 16, color: "7F7F7F" }),
      new TextRun({ children: [PageNumber.CURRENT], size: 16, color: "7F7F7F" }),
      new TextRun({ text: " из ", size: 16, color: "7F7F7F" }),
      new TextRun({ children: [PageNumber.TOTAL_PAGES], size: 16, color: "7F7F7F" }),
    ] })] }) },
    children,
  }],
});

Packer.toBuffer(doc).then(buf => { fs.writeFileSync(outPath, buf); console.log("written", outPath, buf.length, "bytes;", blocks.length, "blocks;", numLists, "numbered lists"); });
