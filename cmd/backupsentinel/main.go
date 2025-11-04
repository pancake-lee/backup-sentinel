package main

import (
	"flag"
	"os"

	"github.com/pancake-lee/pgo/pkg/plogger"
	"go.uber.org/zap/zapcore"

	"backup-sentinel/internal/app"
)

func main() {
	consumerMode := flag.Bool("consumer", false, "run in consumer mode to process pending file events")
	flag.Parse()

	plogger.InitLogger(false, zapcore.DebugLevel, "./logs/")

	mode := app.ModeProducer
	if *consumerMode {
		mode = app.ModeConsumer
	}

	application := app.New(app.Options{Mode: mode})
	if err := application.Run(flag.Args()); err != nil {
		plogger.Errorf("backup sentinel stopped: %v", err)
		os.Exit(1)
	}
}
