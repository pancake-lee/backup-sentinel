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
	dbPath := flag.String("db", "./backupSentinel.db", "path to sqlite database file")
	cmdTemplate := flag.String("cmd", "", "command template to execute for each event; use %fullfile% placeholder")
	cmdFile := flag.String("f", "", "path to JSON file containing per-event commands")
	flag.Parse()

	if *checkMode {
		*isLogConsole = true
	}

	lv := plogger.StrToLoggerLevel(*logLevel)

	mode := app.ModeProducer
	if *consumerMode {
		mode = app.ModeConsumer
		plogger.InitLogger(*isLogConsole, lv, "./logs/consumer/")
		plogger.Debugf("cmd[%v]", *cmdTemplate)
	} else {
		plogger.InitLogger(*isLogConsole, lv, "./logs/producer/")
	}

	application := app.New(app.Options{Mode: mode, Check: *checkMode, DBPath: *dbPath, Cmd: *cmdTemplate, CmdFile: *cmdFile})
	if err := application.Run(flag.Args()); err != nil {
		plogger.Errorf("backup sentinel stopped: %v", err)
		os.Exit(1)
	}
}
