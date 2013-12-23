package log

import (
	"fmt"
	"github.com/senarukana/dldb/conf"
	"sync"
)

const (
	LevelTrace = iota
	LevelDebug
	LevelInfo
	LevelWarn
	LevelError
	LevelCritical
)

type loggerType func() LoggerInterface

type LoggerInterface interface {
	Init(config *conf.LogConfiguration) error
	WriteMsg(msg string, level int) error
	Destroy()
}

//init
var Logger *DldbLogger

// we don't initiate the logger at the beginning
var logAdapters = make(map[string]loggerType)

type DldbLogger struct {
	lock    sync.Mutex
	level   int
	msgChan chan *logMsg
	outputs map[string]LoggerInterface
}

type logMsg struct {
	level int
	msg   string
}

// create a logger background thread to log
// we should firstly use this method and then setlogger
func NewLogger(channelLen int64) *DldbLogger {
	log := new(DldbLogger)
	log.msgChan = make(chan *logMsg, channelLen)
	log.outputs = make(map[string]LoggerInterface)
	go log.startLogger()
	return log
}

//register log, we don't initial the logger at this time
func Register(logtype string, log loggerType) {
	if log == nil {
		panic("logs: Register provide is nil")
	}
	if _, dup := logAdapters[logtype]; dup {
		panic("logs: Register called twice for provider " + logtype)
	}
	logAdapters[logtype] = log
}

// set different log type
// now it supports: file, conn, smtp
// you can set multiple logger at the same time
func SetLogger(logName string, conf *conf.LogConfiguration) error {
	Logger.lock.Lock()
	defer Logger.lock.Unlock()
	if log, ok := logAdapters[logName]; ok {
		lg := log()
		lg.Init(conf)
		Logger.outputs[logName] = lg
		return nil
	} else {
		return fmt.Errorf("logs: unknown log type %q (forgotten Register?)", logName)
	}
}

func DelLogger(logName string) error {
	Logger.lock.Lock()
	defer Logger.lock.Unlock()
	if log, ok := Logger.outputs[logName]; ok {
		log.Destroy()
		delete(Logger.outputs, logName)
		return nil
	} else {
		return fmt.Errorf("logs: unknown log type %q (forgotten Register?)", logName)
	}
}

func (self *DldbLogger) writeMsg(level int, msg string) error {
	if level < self.level {
		return nil
	}
	lm := new(logMsg)
	lm.level = level
	lm.msg = msg
	self.msgChan <- lm
	return nil
}

func (self *DldbLogger) startLogger() {
	for {
		select {
		case lm := <-self.msgChan:
			for _, v := range self.outputs {
				v.WriteMsg(lm.msg, lm.level)
			}
		}
	}
}

func SetLevel(level int) {
	Warn("System Log level has changed to %d", level)
	Logger.level = level
}

func Trace(format string, v ...interface{}) {
	msg := fmt.Sprintf("[T] "+format, v...)
	Logger.writeMsg(LevelTrace, msg)
}

func Debug(format string, v ...interface{}) {
	msg := fmt.Sprintf("[D] "+format, v...)
	Logger.writeMsg(LevelDebug, msg)
}

func Info(format string, v ...interface{}) {
	msg := fmt.Sprintf("[I] "+format, v...)
	Logger.writeMsg(LevelInfo, msg)
}

func Warn(format string, v ...interface{}) {
	msg := fmt.Sprintf("[W] "+format, v...)
	Logger.writeMsg(LevelWarn, msg)
}

func Error(format string, v ...interface{}) {
	msg := fmt.Sprintf("[E] "+format, v...)
	Logger.writeMsg(LevelError, msg)
}

func Critical(format string, v ...interface{}) {
	msg := fmt.Sprintf("[C] "+format, v...)
	Logger.writeMsg(LevelCritical, msg)
}

func Close() {
	for _, l := range Logger.outputs {
		l.Destroy()
	}
}

func init() {
	Logger = NewLogger(100000)
	SetLogger("console", nil)
}
