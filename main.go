package main

import (
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

func main() {
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
	app := cli.NewApp()
	app.Name = "subtitle-translate"
	app.Usage = "translates WebVTT subtitles into the viewer's language"
	app.Version = "0.1.0"
	configure(app)
	if err := app.Run(os.Args); err != nil {
		log.WithError(err).Fatal("failed to serve application")
	}
}
