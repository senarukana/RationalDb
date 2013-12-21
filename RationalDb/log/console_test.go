package log

import (
	"github.com/senarukana/dldb/conf"
	"testing"
)

func TestConsole(t *testing.T) {
	// dldbLogger.SetLogger("console", nil)
	c := conf.InitDldbConfiguration("testlog.conf")
	SetLogger("console", &c.LogConfiguration)
	Trace("trace")
	Critical("critical")
	DelLogger("console")
	Trace("trace")
}
