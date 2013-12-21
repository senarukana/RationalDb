package log

import (
	"fmt"
	"github.com/senarukana/dldb/conf"
	syslog "log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	logFilePrimitives = 0660
)

type FileLogWriter struct {
	*syslog.Logger
	mw       *MutexWriter
	filepath string
	//current log file name format is : yyyy-mm-dd.log.n
	filename string
	//current log file num
	filenum int

	//set to 0 if you don't care about it
	// Rotate at lines
	maxlines int
	curlines int

	// Rotate at size
	maxsize int
	cursize int

	// Rotate daily
	daily bool
	// longgest preserved time
	maxdays int64
	// Opend time of the current log file
	curlog_opendate int

	rotate bool
	//lock for rotate
	rotateLock sync.Mutex
	level      int
}

// we need lock to write the file
type MutexWriter struct {
	sync.Mutex
	fd *os.File
}

func makeLogFileName(filepath string, filenum int) string {
	filename := fmt.Sprintf("%s.%s.log.%03d", "dldb", time.Now().Format("2006-01-02"), filenum)
	return path.Join(filepath, filename)
}

func (self *MutexWriter) Write(b []byte) (int, error) {
	self.Lock()
	defer self.Unlock()
	return self.fd.Write(b)
}

func (self *MutexWriter) SetFd(fd *os.File) {
	if self.fd != nil {
		self.fd.Close()
	}
	self.fd = fd
}

func NewFileWriter() LoggerInterface {
	flog := &FileLogWriter{
		maxlines: 1000000,
		maxsize:  1 << 28, //256 MB
		daily:    true,
		maxdays:  7,
		rotate:   true,
		level:    LevelTrace,
	}

	// use MutexWriter instead direct use os.File for lock write when rotate
	flog.mw = new(MutexWriter)
	// Set MutexWriter as Logger's io.Writer
	flog.Logger = syslog.New(flog.mw, "", syslog.Ldate|syslog.Ltime)

	return flog
}

func (self *FileLogWriter) Init(conf *conf.LogConfiguration) error {
	self.level = conf.FileLogConfiguration.Level
	self.filepath = conf.FileLogConfiguration.Filepath
	self.rotate = conf.FileLogConfiguration.Rotate
	self.daily = conf.FileLogConfiguration.RotateDaily
	self.maxlines = conf.FileLogConfiguration.Maxlines
	self.maxdays = conf.FileLogConfiguration.Maxdays
	// open a log file, we need to make sure the file is not exist
	var filename string
	// we make sure that 1000 is enough to find a suitable log file
	var i = 1
	for ; i <= 1000; i++ {
		filename = makeLogFileName(self.filepath, i)
		//file not exists
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			self.filenum = i
			self.filename = filename
			break
		} else {
			continue
		}
	}
	if i > 1000 {
		panic("log file init error, may be there is something wrong with log system")
	}
	err := self.StartLogger()
	return err
}

func (self *FileLogWriter) StartLogger() error {
	fd, err := self.createLogFile()
	if err != nil {
		return err
	}
	self.mw.SetFd(fd)
	self.curlog_opendate = time.Now().Day()
	return nil
}

//open the logfile
func (self *FileLogWriter) createLogFile() (*os.File, error) {
	fd, err := os.OpenFile(self.filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, logFilePrimitives)
	return fd, err
}

// rorate the log file
func (self *FileLogWriter) DoRotate() error {
	self.mw.Lock()
	defer self.mw.Unlock()

	// close the current log file
	self.mw.fd.Close()

	// increase the file num and change the current file name
	self.filenum++
	self.filename = makeLogFileName(self.filepath, self.filenum)

	// restart log
	err := self.StartLogger()
	if err != nil {
		return fmt.Errorf("Rotate logger file err: %s\n", err)
	}

	go self.deleteObsoleteLogFiles()
	return nil
}

// delete the log file if it is old enough, according to the maxdays
func (self *FileLogWriter) deleteObsoleteLogFiles() {
	dir := path.Dir(self.filename)
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() && info.ModTime().Unix() < (time.Now().Unix()-60*60*24*self.maxdays) {
			if strings.HasPrefix(filepath.Base(path), filepath.Base(self.filename)) {
				os.Remove(path)
			}
		}
		return nil
	})
}

//check if we need to rotate the log file
func (self *FileLogWriter) docheck(size int) {
	self.rotateLock.Lock()
	defer self.rotateLock.Unlock()

	if (self.maxlines > 0 && self.curlines >= self.maxlines) ||
		(self.maxsize > 0 && self.cursize >= self.maxsize) ||
		(self.daily && time.Now().Day() != self.curlog_opendate) {
		if err := self.DoRotate(); err != nil {
			fmt.Fprintf(os.Stderr, "FileLogWriter(%q): %s\n", self.filename, err)
			return
		}
	}
	self.curlines++
	self.cursize += size
}

func (self *FileLogWriter) WriteMsg(msg string, level int) error {
	if level < self.level {
		return nil
	}
	n := 24 + len(msg) // 24 stand for the length "2013/06/23 21:00:22 [T] "
	self.docheck(n)
	self.Logger.Println(msg)
	return nil
}

// just close the current log file
func (self *FileLogWriter) Destroy() {
	self.mw.fd.Close()
}

func init() {
	Register("file", NewFileWriter)
}
