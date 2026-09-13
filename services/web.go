package services

import (
	"fmt"
	"net"
	"net/http"

	logrusmiddleware "github.com/bakins/logrus-middleware"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

const (
	webHostFlag = "host"
	webPortFlag = "port"
)

func RegisterWebFlags(f []cli.Flag) []cli.Flag {
	return append(f,
		cli.StringFlag{Name: webHostFlag, Usage: "listening host", Value: "", EnvVar: "WEB_HOST"},
		cli.IntFlag{Name: webPortFlag, Usage: "http listening port", Value: 8080, EnvVar: "WEB_PORT"},
	)
}

// NotConfiguredHandler is served when no API key is configured: the
// capability is absent and says so instead of failing silently.
func NotConfiguredHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "translation is not configured", http.StatusNotImplemented)
	})
}

type Web struct {
	host string
	port int
	h    http.Handler
	ln   net.Listener
}

func NewWeb(c *cli.Context, h http.Handler) *Web {
	return &Web{host: c.String(webHostFlag), port: c.Int(webPortFlag), h: h}
}

func (s *Web) Serve() error {
	addr := fmt.Sprintf("%s:%d", s.host, s.port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.Wrap(err, "failed to listen to tcp connection")
	}
	s.ln = ln
	logger := log.New()
	m := logrusmiddleware.Middleware{Logger: logger}
	srv := &http.Server{Handler: m.Handler(s.h, ""), MaxHeaderBytes: 50 << 20}
	log.Infof("serving web at %v", addr)
	return srv.Serve(ln)
}

func (s *Web) Close() {
	if s.ln != nil {
		_ = s.ln.Close()
	}
}
