package main

import (
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	cs "github.com/webtor-io/common-services"
	"github.com/webtor-io/subtitle-translate/services"
)

func configure(app *cli.App) {
	app.Flags = []cli.Flag{}
	app.Flags = cs.RegisterProbeFlags(app.Flags)
	app.Flags = cs.RegisterPromFlags(app.Flags)
	app.Flags = services.RegisterWebFlags(app.Flags)
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
	web := services.NewWeb(c, services.NotConfiguredHandler())
	servers = append(servers, web)
	defer web.Close()
	if err := cs.NewServe(servers...).Serve(); err != nil {
		log.WithError(err).Error("got serve error")
		return err
	}
	return nil
}
