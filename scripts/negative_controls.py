#!/usr/bin/env python3
"""Remove one behavior at a time via Go overlays; its regression test must fail.

No tracked file is edited, and compilation failures do not count as controls.
Run from any directory: python3 scripts/negative_controls.py
"""
import concurrent.futures
import json
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
CONTROLS = [
    ('openai-selection', 'provider.go', '\n\tif strings.TrimSpace(c.String(flagOpenAIAPIKey)) != "" {',
     '\n\tif strings.TrimSpace(c.String(flagOpenAIAPIKey)) != "" && false {', 'TestConfiguredProvider/openai$'),
    ('anthropic-priority', 'provider.go', 'if strings.TrimSpace(c.String(flagAPIKey)) != "" {',
     'if strings.TrimSpace(c.String(flagAPIKey)) != "" && strings.TrimSpace(c.String(flagOpenAIAPIKey)) == "" {', 'TestConfiguredProvider/both$'),
    ('credential-whitespace', 'provider.go', 'if strings.TrimSpace(c.String(flagAPIKey)) != "" {',
     'if c.String(flagAPIKey) != "" {', 'TestConfiguredProvider/whitespace$'),
    ('interface-nil', 'provider.go', '\n\treturn nil\n}',
     '\n\treturn (*OpenAITranslator)(nil)\n}', 'TestConfiguredProvider/neither$'),
    ('model-override', 'provider.go', 'if model := c.String(flagModel); model != "" {',
     'if model := c.String(flagModel); model != "" && false {', 'TestConfiguredProvider/openai_override$'),
    ('openai-model-default', 'openai.go', 'translatorModel(c, defaultOpenAIModel)',
     'translatorModel(c, defaultAnthropicModel)', 'TestConfiguredProvider/openai$'),
    ('anthropic-model-default', 'anthropic.go', 'translatorModel(c, defaultAnthropicModel)',
     'translatorModel(c, defaultOpenAIModel)', 'TestConfiguredProvider/anthropic$'),
    ('openai-empty-key', 'openai.go', 'if key == "" {',
     'if key == "" && false {', 'TestOpenAINilWithoutKey$'),
    ('openai-key-trim', 'openai.go', 'key := strings.TrimSpace(c.String(flagOpenAIAPIKey))',
     'key := c.String(flagOpenAIAPIKey); _ = strings.TrimSpace', 'TestOpenAITranslate$'),
    ('response-status', 'openai.go', 'if resp.Status != responses.ResponseStatusCompleted {',
     'if resp.Status != responses.ResponseStatusCompleted && false {', 'TestOpenAIRejectsNonCompletedStatus$'),
    ('truncation-classification', 'openai.go', 'case "max_output_tokens":\n\t\t\treturn nil, false, in, out, errors.Wrapf(ErrTruncated, "batch of %d lines", n)',
     'case "max_output_tokens":\n\t\t\treturn nil, false, in, out, ErrRefused', 'TestOpenAIRetryPolicy/truncated$'),
    ('content-filter-classification', 'openai.go', 'case "content_filter":\n\t\t\treturn nil, false, in, out, ErrRefused',
     'case "content_filter":\n\t\t\treturn nil, false, in, out, ErrTruncated', 'TestOpenAIRetryPolicy/filtered$'),
    ('refusal-detection', 'openai.go', 'if content.Type == "refusal" {',
     'if content.Type == "refusal" && false {', 'TestOpenAIRetryPolicy/refused$'),
    ('http-error-propagation', 'openai.go', 'return nil, false, 0, 0, errors.Wrap(err, "upstream request failed")',
     'return []string{"Olá", "Tchau"}, true, 0, 0, nil', 'TestOpenAIUpstreamError$'),
    ('no-response-storage', 'openai.go', 'Store: openai.Bool(false)',
     'Store: openai.Bool(true)', 'TestOpenAITranslate$'),
    ('output-token-limit', 'openai.go', 'MaxOutputTokens: openai.Int(t.maxTokens)',
     'MaxOutputTokens: openai.Int(t.maxTokens + 1)', 'TestOpenAITranslate$'),
    ('system-prompt', 'openai.go', 'Instructions: openai.String(system)',
     'Instructions: openai.String("")', 'TestOpenAITranslate$'),
    ('parent-cancellation', 'openai.go', 'context.WithTimeout(ctx, t.timeout)',
     'context.WithTimeout(context.Background(), t.timeout)', 'TestOpenAICancellation$'),
    ('batch-timeout', 'openai.go', 'context.WithTimeout(ctx, t.timeout)',
     'context.WithCancel(ctx)', 'TestOpenAITimeout$'),
    ('mismatch-repair', 'translator.go', 'const attempts = 2',
     'const attempts = 1', 'TestOpenAIRetryPolicy/repair$'),
    ('retry-bound', 'translator.go', 'const attempts = 2',
     'const attempts = 3', 'TestOpenAIRetryPolicy/give_up$'),
    ('retry-only-line-mismatch', 'translator.go', '!errors.Is(err, ErrLineMismatch) || !complete',
     '!complete', 'TestOpenAIRetryPolicy/refused$'),
    ('retry-only-complete-reply', 'translator.go', ' || !complete || ',
     ' || (!complete && false) || ', 'TestTranslateDoesNotRepairNonCompleteReply$'),
    ('input-token-accounting', 'translator.go', 'res.InputTokens += in',
     'res.InputTokens = in', 'TestOpenAIRetryPolicy/repair$'),
    ('output-token-accounting', 'translator.go', 'res.OutputTokens += out',
     'res.OutputTokens = out', 'TestOpenAIRetryPolicy/repair$'),
    ('repair-instruction', 'translator.go', 'user += "\\nReminder: output exactly " + strconv.Itoa(len(req.Lines)) + " numbered lines, nothing else.\\n"',
     '_ = strconv.Itoa(len(req.Lines))', 'TestOpenAIRetryPolicy/repair$'),
]


def run_control(control):
    name, filename, before, after, test = control
    source_path = ROOT / 'services' / filename
    source = source_path.read_text()
    if source.count(before) != 1:
        return name, False, 'mutation target changed; update this control'
    with tempfile.TemporaryDirectory(prefix='subtitle-negative-') as directory:
        work = Path(directory)
        edited = work / filename
        edited.write_text(source.replace(before, after, 1))
        overlay = work / 'overlay.json'
        overlay.write_text(json.dumps({'Replace': {str(source_path): str(edited)}}))
        result = subprocess.run(
            ['go', 'test', '-overlay', str(overlay), './services', '-run', '^'+test,
             '-count=1', '-timeout=15s'], cwd=ROOT, capture_output=True, text=True,
        )
        output = result.stdout + result.stderr
        if result.returncode == 0:
            return name, False, 'test stayed green after removing its rule'
        if '[build failed]' in output or '--- FAIL:' not in output:
            return name, False, 'not an assertion failure:\n'+output[-2000:]
        return name, True, 'test failed as expected'


def main():
    baseline = subprocess.run(
        ['go', 'test', './services', '-run', 'TestConfiguredProvider|TestOpenAI|TestTranslate',
         '-count=1', '-timeout=30s'], cwd=ROOT, capture_output=True, text=True,
    )
    if baseline.returncode:
        print('Unmodified baseline failed:\n'+baseline.stdout+baseline.stderr)
        raise SystemExit(1)
    failures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        for name, ok, detail in pool.map(run_control, CONTROLS):
            print(('PASS ' if ok else 'FAIL ')+name+': '+detail, flush=True)
            if not ok:
                failures.append(name)
    print(str(len(CONTROLS)-len(failures))+'/'+str(len(CONTROLS))+' negative controls passed.')
    raise SystemExit(bool(failures))


if __name__ == '__main__':
    main()
