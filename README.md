# subtitle-translate

Translates a WebVTT subtitle track into the viewer's language through an upstream LLM. The source track is fetched once, split into cue batches, and translated progressively: a client polling the same URL sees a growing prefix of translated cues before the job finishes.
In-flight progress and the finished result are cached (Redis, or S3 when configured), keyed by info hash + subtitle path + target language + model + prompt version, so repeat requests for the same track and language are served from cache instead of re-translated.

## Called by torrent-http-proxy

The service is reached as a Matryoshka mod on the subtitle URL:

```
/<...>~tr:<lang>/<name>.vtt
```

`<lang>` is a 2-letter code (see below). THP strips the mod segment from the path it forwards and sends its argument as `X-Mod-Extra` (`~tr:pt` → `pt`), which is what the handler reads; parsing the language back out of the request path is the fallback for a direct call, which keeps the full path.

Request headers (set by the proxy chain):

| Header | Meaning |
|---|---|
| `X-Source-Url` | URL of the original (untranslated) VTT track. Required. |
| `X-Info-Hash` | Torrent info hash, part of the cache key. |
| `X-Path` | Path of the subtitle file within the torrent, part of the cache key after `/session/<id>/` is removed. |

Optional query parameters:

- `?srclang=<code>` — source language hint passed to the model. It must be one of the codes below; anything else is dropped (the hint is optional, so a bad one costs the viewer nothing).
- `?names=a,b,c` — glossary of proper names/terms to keep untranslated or transliterate consistently; comma-separated, capped at 30 entries of 40 runes each. Each entry is flattened to a single line — control characters and newlines become spaces, runs of whitespace collapse — and entries that come out empty are dropped.

Neither parameter is part of the cache key, and that is deliberate: the artifact is shared by everyone watching the track, so the first requester's hint and glossary are baked into it. Key stability across viewers is the point — a per-request key would translate the same film once per visitor. It is also why both are validated rather than passed through: what the first requester writes into the prompt is what every later viewer of that file and language reads back, for 24 h from Redis and indefinitely from S3.

Response headers:

- `X-Subtitle-Progress: done/total` — cues translated so far out of the total cue count. `HEAD` reports progress without triggering or waiting on a translation.
  - `0/0` means **unknown**: no job has registered for this key yet (nothing started it, or it is still fetching the source). Keep polling — it is not "nothing to translate".
  - The artifact is complete only when `done == total` **and** `total > 0`, or when the response carries the final `Cache-Control: public, max-age=86400`.
- `Cache-Control: public, max-age=86400` on the finished artifact,
  `Cache-Control: no-store` on a partial (in-progress) response.
- `X-Subtitle-Live: 1` — set only when `X-Source-Url` is a live HLS subtitle
  playlist and the job is not finished yet. See [Live HLS source](#live-hls-source).
- `X-Subtitle-Status: done|stopped` — set only when `X-Source-Url` is a live
  HLS subtitle playlist and the run has reached a terminal state:
  - `done` — the playlist reached `#EXT-X-ENDLIST` and every pending cue was
    translated, whether or not that produced a final artifact (a run that
    joined after a seek reaches this state too — see [Live HLS source](#live-hls-source)
    — and still may not write one).
  - `stopped` — the run was cut short: the transcoder session went away, or
    the source outgrew `--max-source-bytes`/`--max-cues`, before it finished.
  - Absent for every other live response (still running, or paused because
    nobody polled the key for a while — see `--live-idle`) and for every
    offline/file-source response, live or not: a finished offline artifact
    signals through `Cache-Control: public, max-age=86400` instead, the same
    as before this header existed.
  - A later transcoder session on the same key is new work and starts
    without this header, even if the previous session ended `done`.

Status codes:

| Code | Meaning |
|---|---|
| 400 | Missing/unsupported target language in the path, missing `X-Source-Url`, or a source URL whose scheme is not `http`/`https`. |
| 404 | Source track could not be fetched or parsed. |
| 413 | Source track exceeds `--max-source-bytes` or `--max-cues`. |
| 501 | No upstream API key configured — translation is disabled. |
| 502 | Upstream/store state unavailable while assembling the response. |

Error bodies are fixed strings (`bad request`, `source unavailable`, `source too large`, `too many cues`, `upstream state unavailable`). The cause — source URL, dial error, parse error — is logged, never returned.

A `GET` starts (or resumes) the background job for the key if one isn't already running, and returns the current snapshot immediately (cached final artifact, or the cues translated so far). Callers poll the same URL until `X-Subtitle-Progress` reports done. The parsed source is cached in-process for 10 minutes per key, so polling costs one source fetch, not one per poll. Both in-process caches — parsed sources and live sources — hold 16 entries per replica: a parsed 1 MiB track is ~11 MB resident, which is the number that has to fit under the pod's memory limit.

At most `--max-jobs` translations run at once per replica (`--live-max-jobs` for live ones, counted separately); further keys are registered immediately and start as slots free up.

## Live HLS source

When `X-Source-Url` points at a media playlist (`….m3u8`) — the transcoder's
subtitle variant `<file>~hls/session/<id>/s<N>.m3u8` — the job follows the
playlist instead of reading one file: every `--live-poll-interval` it re-reads
the playlist, fetches the segments it has not seen, shifts their cues by
`#EXT-X-SESSION-OFFSET` into movie time and translates what has accumulated —
a batch of `--batch-size` cues, or fewer once the oldest pending cue has waited
`--live-batch-wait`.

- **A live source is identified by its URL without the query**: scheme, host
  and path. The transcoder session id lives in the path
  (`…~hls/session/<id>/s<N>.m3u8`), while the query carries things that change
  within one session — the player re-requests the track as `?…&rev=<n>` every
  ~15 s while cues arrive, and the session token can be renewed. A poll whose
  identity matches the source being followed only updates the URL that source
  fetches from (the newest query is the one upstream will still accept);
  nothing is retired, no document is rebuilt and no segment is fetched twice.
  A different session id is a different session, and swaps the source as
  before.
- One key follows **one** source at a time, and the key deliberately outlives
  a session (`X-Path` is keyed with `/session/<id>/` removed). A second viewer
  watching the same file and language at a different position therefore shares
  that one source and may, for a while, be served a document that does not
  cover where they are — the cues they need arrive once the runs converge on
  the same range, and a translation already paid for is matched by cue
  identity rather than bought again. It self-heals; it is not an error state.
- `X-Subtitle-Live: 1` is set while the playlist is live (no `#EXT-X-ENDLIST`)
  or the job is still writing. Do not read `done == total` as complete while it
  is present. The body carries the translated cues only: a cue not translated
  **yet** is omitted rather than shown in the source language. A cue the model
  refused or answered with the wrong line count is the exception — it keeps its
  source text and counts as done, exactly as on the file path.
- The final artifact is written only for a contiguous run — offset 0 from the
  first read to `#EXT-X-ENDLIST`. A viewer who seeks gets a partial translation
  for the session (kept in Redis for 24 h under the same key, reused by cue
  identity on the next session); the next contiguous viewing completes it.
- After a seek, the cues of the current run are translated before the backlog of earlier runs.
- The job stops on its own when nobody polled the key for `--live-idle`:
  reading the playlist keeps the transcoder session alive, so an unwatched
  translation would otherwise transcode the whole file for nobody. It also stops
  when the session is gone (404/503 from the transcoder) or the source outgrew
  its caps, keeping progress. The idle stop leaves the record live (it is a
  pause, not an end — the next poll resumes it); the other two set
  `X-Subtitle-Status: stopped` on it (see [Response headers](#called-by-torrent-http-proxy)).
- A segment that fails three times in a row is given up on — marked seen,
  counted by `subtitle_translate_live_segments_skipped_total`, and left as a
  hole that stops the run from writing a final artifact — because one segment
  the transcoder has GC'd while still listing it (or one behind a path the
  proxy keeps rate-limiting) is retried at the head of the playlist and would
  otherwise freeze the whole document while the playlist keeps answering 200.
- A poll refreshes the source it is served from only if the last attempt is
  older than `--live-poll-interval` — successful or not, so an upstream that
  is failing costs one playlist read per interval per replica rather than one
  per poll — and never waits for a refresh already in flight, serving what is
  known instead of queueing behind a stalled read.
- Size caps apply to the accumulated document: `--max-source-bytes` to the sum
  of segment bytes, `--max-cues` to the cue count.
- Cue identity is the cue's text plus a 3-second time window **across runs**,
  not an exact timestamp (within one run it stays exact, so a line genuinely
  said twice inside the window is two cues): the transcoder reports the requested (30 s-quantized) seek as
  `#EXT-X-SESSION-OFFSET` but starts each run at the keyframe at or before it,
  so every run's timeline is offset by up to one GOP (measured: 1.657 s between
  two runs of the same file). A translated cue therefore carries the timing of
  the run that first produced it, and after a seek it can sit up to a GOP away
  from the player's own timeline — the same as every side-loaded track on this
  platform today. Without the window the replayed range is translated and
  rendered twice.
- A run that ends without producing a final artifact (it joined after a seek,
  so the document has holes) is finished for good *for that run*: the record
  is written with the live flag cleared and `X-Subtitle-Status: done` (it did
  reach `#EXT-X-ENDLIST` with nothing left pending — a final artifact is a
  separate question from whether the run is done), and no later poll starts
  another job against that source. A seek that brings the same session new
  cues re-arms it — the run is not over, it moved — which is also what clears
  `X-Subtitle-Status` back off until this run reaches its own end. A genuinely
  new transcoder session is new work regardless.
- Live jobs are bounded by `--live-max-jobs`, separately from `--max-jobs`: a
  live job holds its slot for the length of a film while doing almost nothing,
  so queueing it behind offline work (or offline work behind it) is the wrong
  trade.

## What survives the round trip

Cue timings and cue count are never changed: a cue that normalizes to nothing stays as an empty cue at its own timestamp. Cue settings (`align`, `line`, `position`, `size`, `region`), the cue's style and region references, and the document-level `STYLE` and `REGION` blocks are copied from the source onto the translation.

Inline markup **inside** cue text is flattened: voice spans (`<v Speaker>`), `<b>/<i>/<u>`, class and timestamp tags come back as plain text. The model is sent text and returns text, so the tags do not survive. Hearing-impaired markup (`[DOOR SLAMS]`, `(sighs)`) and music-only lines are stripped before translation.

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
   --max-tokens value                  max output tokens per batch (default: 8192) [$SUBTITLE_TRANSLATE_MAX_TOKENS]
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
   --batch-size value                  cues per upstream request (default: 50) [$SUBTITLE_TRANSLATE_BATCH_SIZE]
   --max-cues value                    largest source track accepted, in cues (default: 5000) [$SUBTITLE_TRANSLATE_MAX_CUES]
   --max-source-bytes value            largest source track accepted, in bytes (default: 1048576) [$SUBTITLE_TRANSLATE_MAX_SOURCE_BYTES]
   --lock-ttl value                    how long one replica owns a translation key, seconds; also the per-batch deadline (default: 300) [$SUBTITLE_TRANSLATE_LOCK_TTL]
   --max-jobs value                    translation jobs running at once in this replica (default: 4) [$SUBTITLE_TRANSLATE_MAX_JOBS]
   --live-poll-interval value          how often a live HLS subtitle playlist is re-read, seconds (default: 4) [$SUBTITLE_TRANSLATE_LIVE_POLL_INTERVAL]
   --live-batch-wait value             longest a pending live cue waits before a batch smaller than --batch-size is sent, seconds (default: 10) [$SUBTITLE_TRANSLATE_LIVE_BATCH_WAIT]
   --live-idle value                   a live job stops when nobody polled its key for this long, seconds (default: 90) [$SUBTITLE_TRANSLATE_LIVE_IDLE]
   --live-max-jobs value               live translation jobs running at once in this replica, bounded separately from --max-jobs (default: 16) [$SUBTITLE_TRANSLATE_LIVE_MAX_JOBS]
   --help, -h                          show help
   --version, -v                       print the version
```

## Metrics

Served when `--use-prom` is set.

| Name | Type | Meaning |
|---|---|---|
| `subtitle_translate_batches_total` | counter | Cue batches successfully translated (a batch that fell back to source text is not counted here). |
| `subtitle_translate_tokens_input_total` | counter | Upstream input tokens consumed. |
| `subtitle_translate_tokens_output_total` | counter | Upstream output tokens consumed. |
| `subtitle_translate_batches_fallback_total{reason}` | counter | Batches (or split halves) whose cues kept their source text, by `reason`: `mismatch`, `truncated`, `refusal`. |
| `subtitle_translate_job_errors_total{code}` | counter | Job errors by cause (`panic`, `store`, `upstream`, `render`, `truncated`, `refusal`, `lock_lost`, `too_large`, `source_gone`, `viewer_gone`). `source_gone` and `viewer_gone` are live-job terminations, not failures: the transcoder session ended or nobody polled the key for `--live-idle` — expected outcomes of a live translation, not something to page on. |
| `subtitle_translate_jobs_running` | gauge | Translation jobs holding a concurrency slot (`--max-jobs` bounds the offline ones, `--live-max-jobs` the live ones). |
| `subtitle_translate_job_seconds` | histogram | End-to-end duration of a finished translation job. |
| `subtitle_translate_live_segments_skipped_total` | counter | Live segments given up on after three consecutive failures. Each one is a hole in the document (and blocks the final artifact for that run), so a steady rate on one film means the transcoder or the proxy in front of it is losing segments. |
| `subtitle_translate_line_mismatch_total` | counter | Upstream replies whose line count didn't match the batch (retried once, then the original text is kept). |

## Cost

Output tokens dominate: the model writes roughly as much as it reads, and the source is sent once per batch with a few lines of context. On the default model a 2-hour film (~1500 cues) costs about **$0.3–0.5** to translate into one language. The result is cached per track and language, so the cost is paid once, not per viewer — and `--max-cues` bounds the worst single request.

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
