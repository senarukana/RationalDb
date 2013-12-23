package log

import (
	"fmt"
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
	Init() error
	WriteMsg(msg string, level int) error
	Destroy()
}

//init
var logger *Logger

// we don't initiate the logger at the beginning
var logAdapters = make(map[string]loggerType)

type Logger struct {
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
func NewLogger(channelLen int64) *Logger {
	log := new(Logger)
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
func SetLogger(logName string) error {
	logger.lock.Lock()
	defer logger.lock.Unlock()
	if log, ok := logAdapters[logName]; ok {
		lg := log()
		lg.Init()
		logger.outputs[logName] = lg
		return nil
	} else {
		return fmt.Errorf("logs: unknown log type %q (forgotten Register?)", logName)
	}
}

func DelLogger(logName string) error {
	logger.lock.Lock()
	defer logger.lock.Unlock()
	if log, ok := logger.outputs[logName]; ok {
		log.Destroy()
		delete(logger.outputs, logName)
		return nil
	} else {
		return fmt.Errorf("logs: unknown log type %q (forgotten Register?)", logName)
	}
}

func (self *Logger) writeMsg(level int, msg string) error {
	if level < self.level {
		return nil
	}
	lm := new(logMsg)
	lm.level = level
	lm.msg = msg
	self.msgChan <- lm
	return nil
}

func (self *Logger) startLogger() {
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
	logger.level = level
}

func Trace(format string, v ...interface{}) {
	msg := fmt.Sprintf("[T] "+format, v...)
	logger.writeMsg(LevelTrace, msg)
}

func Debug(format string, v ...interface{}) {
	msg := fmt.Sprintf("[D] "+format, v...)
	logger.writeMsg(LevelDebug, msg)
}

func Info(format string, v ...interface{}) {
	msg := fmt.Sprintf("[I] "+format, v...)
	logger.writeMsg(LevelInfo, msg)
}

func Warn(format string, v ...interface{}) {
	msg := fmt.Sprintf("[W] "+format, v...)
	logger.writeMsg(LevelWarn, msg)
}

func Error(format string, v ...interface{}) {
	msg := fmt.Sprintf("[E] "+format, v...)
	logger.writeMsg(LevelError, msg)
}

func Critical(format string, v ...interface{}) {
	msg := fmt.Sprintf("[C] "+format, v...)
	logger.writeMsg(LevelCritical, msg)
}

func Close() {
	for _, l := range logger.outputs {
		l.Destroy()
	}
}

func init() {
	logger = NewLogger(100000)
	SetLogger("console")
}
