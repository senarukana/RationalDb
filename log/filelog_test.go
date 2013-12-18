package log

import (
	"github.com/senarukana/dldb/conf"
	"testing"
)

func TestFile(t *testing.T) {
	c := conf.InitDldbConfiguration("testlog.conf")
	SetLogger("file", &c.LogConfiguration)
	Trace("test")
	Debug("debug")

}
