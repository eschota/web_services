#!/usr/bin/env node
// Rebuilds the exact M+ Renderfin keywording prompt (photo and video) from the
// MicrostockPlus source and writes it to mplus_prompt.json next to this file.
//
//   node extract_mplus_prompt.js <MicrostockPlus checkout>
//
// It evaluates the very constants and helpers M+ uses
// (src/routes/ajax/microstockplus/trends_ai_meta.ts): the default "Free AI"
// template prompts, ai_hint, the video action-story hints and the Renderfin
// JSON instruction (RenderfinVisionPromptForPayload). Nothing else of M+ runs.
'use strict';
const fs = require('fs');
const path = require('path');
const vm = require('vm');

const root = process.argv[2] || process.env.MPLUS_ROOT || '';
if (!root) {
    console.error('usage: node extract_mplus_prompt.js <MicrostockPlus checkout>');
    process.exit(2);
}
const sourcePath = path.join(root, 'src', 'routes', 'ajax', 'microstockplus', 'trends_ai_meta.ts');
const source = fs.readFileSync(sourcePath, 'utf8');
const ts = require(path.join(root, 'node_modules', 'typescript'));

// Single-line constants the prompt is built from.
const CONSTS = [
    'AI_META_OUTPUT_LANGUAGE_HINT', 'AI_META_STOCK_METADATA_RULES_MARKER', 'AI_META_STOCK_METADATA_RULES_HINT',
    'AI_META_VISIBLE_STOCK_PROMPT_SUFFIX', 'AI_META_FREESTOCK_UNIQUENESS_PROMPT_SUFFIX',
    'AI_META_VIDEO_ACTION_STORY_MARKER', 'AI_META_VIDEO_ACTION_FRAME_HINT', 'AI_META_VIDEO_ACTION_SUMMARY_HINT',
    'AI_META_BASE_PROMPT_CORE', 'AI_META_PHOTO_BASE_PROMPT', 'AI_META_VIDEO_FRAME_BASE_PROMPT',
    'AI_META_VIDEO_SUMMARY_BASE_PROMPT', 'AI_META_AI_BASE_PROMPT', 'RENDERFIN_KEYWORDS_OVERASK'
];
const FUNCS = ['CompactString', 'AppendVisibleStockRulesToPrompt', 'AppendAIMetaPromptOnce'];

function grabConst(name) {
    const m = new RegExp('^const ' + name + '\\b[^\\n]*$', 'm').exec(source);
    if (!m) throw new Error('constant not found: ' + name);
    return m[0];
}
function grabFunc(name) {
    const start = source.search(new RegExp('^function ' + name + '\\(', 'm'));
    if (start < 0) throw new Error('function not found: ' + name);
    const end = source.indexOf('\n}\n', start);
    return source.slice(start, end + 2);
}
// The Renderfin instruction lives inside RenderfinVisionPromptForPayload.
function grabInstruction() {
    const body = grabFunc('RenderfinVisionPromptForPayload');
    const m = /const instruction = ([\s\S]*?);\n/.exec(body);
    if (!m) throw new Error('Renderfin instruction not found');
    return 'function RenderfinInstruction(keywordsCount) { return ' + m[1] + '; }';
}

const tsCode = [...FUNCS.map(grabFunc), ...CONSTS.map(grabConst), grabInstruction()].join('\n');
const jsCode = ts.transpileModule(tsCode, {compilerOptions: {target: ts.ScriptTarget.ES2020}}).outputText;
const ctx = {};
vm.createContext(ctx);
vm.runInContext(jsCode + `
this.out = (() => {
    const exact = 50;
    const keywordsCount = exact + RENDERFIN_KEYWORDS_OVERASK;
    const aiHint = AI_META_OUTPUT_LANGUAGE_HINT + ' ' + AI_META_STOCK_METADATA_RULES_HINT;
    const photoPrompt = AppendVisibleStockRulesToPrompt(AI_META_PHOTO_BASE_PROMPT);
    const aiPrompt = AppendVisibleStockRulesToPrompt(AI_META_AI_BASE_PROMPT);
    const frame = AppendAIMetaPromptOnce(AppendVisibleStockRulesToPrompt(AI_META_VIDEO_FRAME_BASE_PROMPT), AI_META_VIDEO_ACTION_FRAME_HINT);
    const summary = AppendAIMetaPromptOnce(AppendVisibleStockRulesToPrompt(AI_META_VIDEO_SUMMARY_BASE_PROMPT), AI_META_VIDEO_ACTION_SUMMARY_HINT);
    const join = parts => CompactString(parts.map(p => CompactString(String(p || ''), 8000)).filter(Boolean).join('\\n\\n'), 20000);
    const instruction = RenderfinInstruction(keywordsCount);
    return {
        exact_keywords_count: exact,
        keywords_asked: keywordsCount,
        photo: CompactString(join([photoPrompt, aiHint]) + '\\n\\n' + instruction, 24000),
        ai: CompactString(join([aiPrompt, aiHint]) + '\\n\\n' + instruction, 24000),
        video: CompactString(join([frame, summary, aiHint]) + '\\n\\n' + instruction, 24000),
        salt_format: '\\n\\n[ref <salt>]'
    };
})();`, ctx);

const out = Object.assign({source: 'MicrostockPlus src/routes/ajax/microstockplus/trends_ai_meta.ts',
    generated_at: new Date().toISOString()}, ctx.out);
const target = path.join(__dirname, 'mplus_prompt.json');
fs.writeFileSync(target, JSON.stringify(out, null, 2) + '\n');
console.log('wrote', target, '- photo', out.photo.length, 'ai', out.ai.length, 'video', out.video.length, 'chars');
