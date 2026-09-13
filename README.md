# subtitle-translate

Translates a WebVTT subtitle track into the viewer's language through an upstream LLM. The source track is fetched once, split into cue batches, and translated progressively: a client polling the same URL sees a growing prefix of translated cues before the job finishes.
In-flight progress and the finished result are cached (Redis, or S3 when configured), keyed by info hash + subtitle path + target language + model + prompt version, so repeat requests for the same track and language are served from cache instead of re-translated.

## Called by torrent-http-proxy

The service is reached as a Matryoshka mod on the subtitle URL:

```
/<...>~tr:<lang>/<name>.vtt
```

`<lang>` is a 2-letter code (see below). THP does not forward the mod extra to the handler, so the target language is parsed back out of the request path.

Request headers (set by the proxy chain):

| Header | Meaning |
|---|---|
| `X-Source-Url` | URL of the original (untranslated) VTT track. Required. |
| `X-Info-Hash` | Torrent info hash, part of the cache key. |
| `X-Path` | Path of the subtitle file within the torrent, part of the cache key. |

Optional query parameters:

- `?srclang=<code>` — source language hint passed to the model.
- `?names=a,b,c` — glossary of proper names/terms to keep untranslated or transliterate consistently; comma-separated, trimmed, capped at 30 entries of 40 runes each.

Response headers:

- `X-Subtitle-Progress: done/total` — cues translated so far out of the total cue count. `HEAD` reports progress without triggering or waiting on a translation.
  - `0/0` means **unknown**: no job has registered for this key yet (nothing started it, or it is still fetching the source). Keep polling — it is not "nothing to translate".
  - The artifact is complete only when `done == total` **and** `total > 0`, or when the response carries the final `Cache-Control: public, max-age=86400`.
- `Cache-Control: public, max-age=86400` on the finished artifact,
  `Cache-Control: no-store` on a partial (in-progress) response.

Status codes:

| Code | Meaning |
|---|---|
| 400 | Missing/unsupported target language in the path, or missing `X-Source-Url`. |
| 404 | Source track could not be fetched or parsed. |
| 413 | Source track exceeds `--max-source-bytes` or `--max-cues`. |
| 501 | No upstream API key configured — translation is disabled. |
| 502 | Upstream/store state unavailable while assembling the response. |

A `GET` starts (or resumes) the background job for the key if one isn't already running, and returns the current snapshot immediately (cached final artifact, or the cues translated so far). Callers poll the same URL until `X-Subtitle-Progress` reports done.

## Supported languages

```
en English     ru Russian    es Spanish     de German     fr French
pt Portuguese  it Italian    pl Polish      tr Turkish    nl Dutch
cs Czech       uk Ukrainian  zh Chinese     ja Japanese   ko Korean
ar Arabic      hi Hindi      id Indonesian  vi Vietnamese th Thai
sv Swedish     no Norwegian  da Danish      fi Finnish    el Greek
he Hebrew      hu Hungarian  ro Romanian    bg Bulgarian  sr Serbian
hr Croatian    sk Slovak     sl Slovenian   lt Lithuanian lv Latvian
et Estonian    fa Persian    ms Malay       bn Bengali    ta Tamil
kk Kazakh      ka Georgian   hy Armenian    az Azerbaijani ca Catalan
```

This is broader than the UI's locale set — the target language comes from the viewer's subtitle preference, not the site's i18n bundle.

## Flags

```
NAME:
   subtitle-translate - translates WebVTT subtitles into the viewer's language

USAGE:
   subtitle-translate [global options] command [command options] [arguments...]

VERSION:
   0.1.0

COMMANDS:
   help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --probe-host value                  probe listening host [$PROBE_HOST]
   --probe-port value                  probe listening port (default: 8081) [$PROBE_PORT]
   --use-probe                         enable probe [$USE_PROBE]
   --prom-host value                   prometheus metrics listening host [$PROM_HOST]
   --prom-port value                   prometheus metrics listening port (default: 8083) [$PROM_PORT]
   --use-prom                          use prometheus metrics [$USE_PROM]
   --host value                        listening host [$WEB_HOST]
   --port value                        http listening port (default: 8080) [$WEB_PORT]
   --anthropic-api-key value           upstream model API key; empty disables translation [$ANTHROPIC_API_KEY]
   --model value                       upstream model id (default: "claude-haiku-4-5-20251001") [$SUBTITLE_TRANSLATE_MODEL]
   --upstream-timeout value            per-batch upstream timeout, seconds (default: 60) [$SUBTITLE_TRANSLATE_UPSTREAM_TIMEOUT]
   --max-tokens value                  max output tokens per batch (default: 4096) [$SUBTITLE_TRANSLATE_MAX_TOKENS]
   --redis-host value                  redis host (default: "localhost") [$REDIS_MASTER_SERVICE_HOST, $ REDIS_SERVICE_HOST]
   --redis-port value                  redis port (default: 6379) [$REDIS_MASTER_SERVICE_PORT, $ REDIS_SERVICE_PORT]
   --redis-pass value                  redis pass [$REDIS_PASS]
   --redis-user value                  redis user (default: "default") [$REDIS_USER]
   --redis-sentinel-port value         redis sentinel port (default: 0) [$REDIS_SERVICE_PORT_REDIS_SENTINEL]
   --redis-sentinel-master-name value  redis sentinel master name (default: "mymaster") [$REDIS_SERVICE_SENTINEL_MASTER_NAME]
   --aws-access-key-id value           AWS Access Key ID [$AWS_ACCESS_KEY_ID]
   --aws-secret-access-key value       AWS Secret Access Key [$AWS_SECRET_ACCESS_KEY]
   --aws-endpoint value                AWS Endpoint [$AWS_ENDPOINT]
   --aws-region value                  AWS Region [$AWS_REGION]
   --aws-no-ssl                         [$AWS_NO_SSL]
   --use-s3                            store finished translations in S3 [$USE_S3]
   --aws-bucket value                  S3 bucket (one bucket per service) (default: "subtitle-translate") [$AWS_BUCKET]
   --s3-prefix value                   optional S3 key prefix [$S3_PREFIX]
   --batch-size value                  (default: 50) [$SUBTITLE_TRANSLATE_BATCH_SIZE]
   --max-cues value                    (default: 5000) [$SUBTITLE_TRANSLATE_MAX_CUES]
   --max-source-bytes value            (default: 1048576) [$SUBTITLE_TRANSLATE_MAX_SOURCE_BYTES]
   --lock-ttl value                    (default: 600) [$SUBTITLE_TRANSLATE_LOCK_TTL]
   --help, -h                          show help
   --version, -v                       print the version
```

## Metrics

Served when `--use-prom` is set.

| Name | Type | Meaning |
|---|---|---|
| `subtitle_translate_batches_total` | counter | Cue batches successfully translated. |
| `subtitle_translate_tokens_input_total` | counter | Upstream input tokens consumed. |
| `subtitle_translate_tokens_output_total` | counter | Upstream output tokens consumed. |
| `subtitle_translate_job_errors_total{code}` | counter | Job errors by cause (`panic`, `store`, `upstream`, `render`). |
| `subtitle_translate_job_seconds` | histogram | End-to-end duration of a finished translation job. |
| `subtitle_translate_line_mismatch_total` | counter | Upstream replies whose line count didn't match the batch (retried once, then the original text is kept). |

## Local run

```bash
ANTHROPIC_API_KEY=… REDIS_SERVICE_HOST=localhost go run . --use-probe=false --use-prom=false
curl -H 'X-Source-Url: http://localhost:8000/sample.vtt' -H 'X-Info-Hash: t' -H 'X-Path: /sample.vtt' \
  -D - 'http://localhost:8080/t/sample.vtt~tr:pt/sample.vtt'
```

## Authorization

The service enforces no authorization of its own — access is gated upstream by web-ui/torrent-http-proxy.

## License

MIT — see [LICENSE](LICENSE).
