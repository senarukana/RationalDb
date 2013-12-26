package log

import (
	"testing"
)

func TestConsole(t *testing.T) {
	// dldbLogger.SetLogger("console", nil)
	Trace("trace")
	Critical("critical")
	DelLogger("console")
	Trace("trace")
}
