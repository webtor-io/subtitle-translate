package main

import (
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	cs "github.com/webtor-io/common-services"
	"github.com/webtor-io/subtitle-translate/services"
)

const (
	flagBatchSize      = "batch-size"
	flagMaxCues        = "max-cues"
	flagMaxSourceBytes = "max-source-bytes"
	flagLockTTL        = "lock-ttl"
	flagMaxJobs        = "max-jobs"

	flagLivePollInterval = "live-poll-interval"
	flagLiveBatchWait    = "live-batch-wait"
	flagLiveIdle         = "live-idle"
)

func configure(app *cli.App) {
	app.Flags = []cli.Flag{}
	app.Flags = cs.RegisterProbeFlags(app.Flags)
	app.Flags = cs.RegisterPromFlags(app.Flags)
	app.Flags = services.RegisterWebFlags(app.Flags)
	app.Flags = services.RegisterTranslatorFlags(app.Flags)
	app.Flags = cs.RegisterRedisClientFlags(app.Flags)
	app.Flags = cs.RegisterS3ClientFlags(app.Flags)
	app.Flags = services.RegisterStoreFlags(app.Flags)
	app.Flags = append(app.Flags,
		cli.IntFlag{Name: flagBatchSize, Usage: "cues per upstream request", Value: 50, EnvVar: "SUBTITLE_TRANSLATE_BATCH_SIZE"},
		cli.IntFlag{Name: flagMaxCues, Usage: "largest source track accepted, in cues", Value: 5000, EnvVar: "SUBTITLE_TRANSLATE_MAX_CUES"},
		cli.Int64Flag{Name: flagMaxSourceBytes, Usage: "largest source track accepted, in bytes", Value: 1 << 20, EnvVar: "SUBTITLE_TRANSLATE_MAX_SOURCE_BYTES"},
		cli.IntFlag{Name: flagLockTTL, Usage: "how long one replica owns a translation key, seconds; also the per-batch deadline", Value: 300, EnvVar: "SUBTITLE_TRANSLATE_LOCK_TTL"},
		cli.IntFlag{Name: flagMaxJobs, Usage: "translation jobs running at once in this replica", Value: 4, EnvVar: "SUBTITLE_TRANSLATE_MAX_JOBS"},
		cli.IntFlag{Name: flagLivePollInterval, Usage: "how often a live HLS subtitle playlist is re-read, seconds", Value: 4, EnvVar: "SUBTITLE_TRANSLATE_LIVE_POLL_INTERVAL"},
		cli.IntFlag{Name: flagLiveBatchWait, Usage: "longest a pending live cue waits before a batch smaller than --batch-size is sent, seconds", Value: 10, EnvVar: "SUBTITLE_TRANSLATE_LIVE_BATCH_WAIT"},
		cli.IntFlag{Name: flagLiveIdle, Usage: "a live job stops when nobody polled its key for this long, seconds", Value: 90, EnvVar: "SUBTITLE_TRANSLATE_LIVE_IDLE"},
	)
	app.Action = run
}

func run(c *cli.Context) error {
	var servers []cs.Servable
	if probe := cs.NewProbe(c); probe != nil {
		servers = append(servers, probe)
		defer probe.Close()
	}
	if prom := cs.NewProm(c); prom != nil {
		servers = append(servers, prom)
		defer prom.Close()
	}
	var handler http.Handler = services.NotConfiguredHandler()
	if tr := services.NewAnthropicTranslator(c); tr != nil {
		rc := cs.NewRedisClient(c)
		defer rc.Close()
		s3c := cs.NewS3Client(c, &http.Client{Timeout: 60 * time.Second})
		store := services.NewRedisStore(c, rc, s3c)
		model := tr.Model()
		runner := services.NewRunner(store, services.Translator(tr), c.Int(flagBatchSize), c.Int(flagMaxJobs), time.Duration(c.Int(flagLockTTL))*time.Second)
		runner.SetLive(services.LiveConfig{
			PollInterval: time.Duration(c.Int(flagLivePollInterval)) * time.Second,
			BatchWait:    time.Duration(c.Int(flagLiveBatchWait)) * time.Second,
			Idle:         time.Duration(c.Int(flagLiveIdle)) * time.Second,
		})
		// Deferred before the web server, so it runs after it: requests stop
		// first, then the running jobs are canceled and drained, and only
		// then do the store clients above go away.
		defer runner.Close()
		// Redirects are not followed: a 3xx is returned as-is and falls into
		// the non-200 branch of fetchDoc, which maps it to 404. This keeps
		// the source fetch from being pointed at an arbitrary host via a
		// redirect chain. The deadline for the whole fetch is the 30s
		// context set in fetchDoc, so the client itself carries no separate
		// (and previously inconsistent) Timeout.
		sourceClient := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}}
		handler = &services.Handler{Runner: runner, Model: model, Client: sourceClient, MaxSourceBytes: c.Int64(flagMaxSourceBytes), MaxCues: c.Int(flagMaxCues)}
	}
	web := services.NewWeb(c, handler)
	servers = append(servers, web)
	defer web.Close()
	if err := cs.NewServe(servers...).Serve(); err != nil {
		log.WithError(err).Error("got serve error")
		return err
	}
	return nil
}
