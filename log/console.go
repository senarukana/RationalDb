package log

import (
	syslog "log"
	"os"
)

type ConsoleLogWriter struct {
	*syslog.Logger
	level int
}

func NewConsoleLogWriter() LoggerInterface {
	cw := new(ConsoleLogWriter)
	cw.Logger = syslog.New(os.Stdout, "", syslog.Ldate|syslog.Ltime|syslog.Lshortfile)
	cw.level = LevelTrace
	return cw
}

func (self *ConsoleLogWriter) Init(conf interface{}) error {
	return nil
}

func (self *ConsoleLogWriter) WriteMsg(msg string, level int) error {
	if level < self.level {
		return nil
	}
	self.Logger.Println(msg)
	return nil
}

func (c *ConsoleLogWriter) Destroy() {}
