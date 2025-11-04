package app

import (
	"os"
	"testing"
	"time"

	"github.com/pancake-lee/pgo/pkg/plogger"
	"go.uber.org/zap/zapcore"
)

func TestAppProducerConsumerCheck(t *testing.T) {
	plogger.InitLogger(false, zapcore.DebugLevel, "./logs/")

	// use test DB path in testdata to ensure isolation
	_ = os.MkdirAll("./testdata", 0o755)
	dbPath := "./testdata/backupsentinel.db"
	// if defaultDBPath points inside testdata, remove it; otherwise ignore
	_ = os.Remove(dbPath)
	defer func() {
		_ = os.Remove(dbPath)
		_ = os.RemoveAll("./testdata")
	}()

	// --------------------------------------------------
	// construct a raw payload similar to Directory Monitor
	payload := `{"t":"` + time.Now().Format("2006/1/2 15:04:05") + `", "e":"创建", "d":"\\\\host\\dir", "f":"\\\\host\\dir\\1.jpg"}`

	// --------------------------------------------------
	// run producer to insert
	prod := New(Options{Mode: ModeProducer, DBPath: dbPath})
	if err := prod.Run([]string{payload}); err != nil {
		t.Fatalf("producer Run: %v", err)
	}

	// --------------------------------------------------
	// open storage directly to verify a record exists
	st, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer st.Close()
	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	pending, err := st.GetPendingEvents(10)
	if err != nil {
		t.Fatalf("get pending: %v", err)
	}
	if len(pending) == 0 {
		t.Fatalf("expected at least one pending event")
	}

	// --------------------------------------------------
	// run consumer in check mode (should not fail)
	cons := New(Options{Mode: ModeConsumer, Check: true, DBPath: dbPath})
	if err := cons.Run(nil); err != nil {
		t.Fatalf("consumer Run: %v", err)
	}
}
