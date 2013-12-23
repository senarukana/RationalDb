package log

import (
	"github.com/senarukana/dldb/conf"
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

func (self *ConsoleLogWriter) Init(conf *conf.LogConfiguration) error {
	if conf == nil {
		return nil
	}
	//read from conf file
	self.level = conf.ConsoleLogConfiguration.Level
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

func init() {
	Register("console", NewConsoleLogWriter)
}
