package main

import (
	"flag"
	"os"

	"github.com/pancake-lee/pgo/pkg/plogger"

	"backup-sentinel/internal/app"
)

func main() {
	consumerMode := flag.Bool("consumer", false, "run in consumer mode to process pending file events")
	checkMode := flag.Bool("check", false, "when in consumer mode, only print pending events instead of processing them")
	logLevel := flag.String("log-level", "debug", "set the logging level (debug|info|warn|error)")
	isLogConsole := flag.Bool("l", false, "log to console instead of file")
	flag.Parse()

	if *checkMode {
		*isLogConsole = true
	}

	lv := plogger.GetLoggerLevel(*logLevel)

	mode := app.ModeProducer
	if *consumerMode {
		plogger.InitLogger(*isLogConsole, lv, "./logs/consumer/")
		mode = app.ModeConsumer
	} else {
		plogger.InitLogger(*isLogConsole, lv, "./logs/producer/")
	}

	application := app.New(app.Options{Mode: mode, Check: *checkMode})
	if err := application.Run(flag.Args()); err != nil {
		plogger.Errorf("backup sentinel stopped: %v", err)
		os.Exit(1)
	}
}
