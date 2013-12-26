package log

import (
	"fmt"
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
	logPathPrimitives = 0770
)

type FileLogConfiguration struct {
	Level       int
	Filepath    string
	Rotate      bool  // Rotate or not
	RotateDaily bool  // Rotate daily
	Maxlines    int   // Rotate at lines, // Set to 0 if you don't care about it
	Maxsize     int   // Rotate at size
	Maxdays     int64 // longgest preserved time
	Test        bool  // remove all log before init, use for test
}

var defaultFileConfiguration = &FileLogConfiguration{
	Level:       LevelTrace,
	Rotate:      true,
	RotateDaily: true,
	Maxlines:    1 << 20,
	Maxsize:     1 << 24, //16mb
	Test:        true,
}

type FileLogWriter struct {
	*syslog.Logger
	mw *MutexWriter
	//current log file name format is : yyyy-mm-dd.log.n
	filename string
	//current log file num
	filenum         int
	curlines        int
	cursize         int
	curlog_opendate int // Opend time of the current log file

	//lock for rotate
	rotateLock sync.Mutex
	conf       *FileLogConfiguration
}

// we need lock to write the file
type MutexWriter struct {
	sync.Mutex
	fd *os.File
}

func makeLogFileName(filepath string, filenum int) string {
	filename := fmt.Sprintf("%s.%s.log.%03d", "rationaldb-op-log", time.Now().Format("2006-01-02"), filenum)
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
	flw := &FileLogWriter{}
	flw.conf = defaultFileConfiguration

	// use MutexWriter instead direct use os.File for lock write when rotate
	flw.mw = new(MutexWriter)
	// Set MutexWriter as Logger's io.Writer
	flw.Logger = syslog.New(flw.mw, "", syslog.Ldate|syslog.Ltime)
	return flw
}

func (self *FileLogWriter) Init(conf interface{}) error {
	if conf != nil {
		self.conf = conf.(*FileLogConfiguration)
	}
	if self.conf.Filepath == "" {
		logPath, err := os.Getwd()
		if err != nil {
			return err
		}
		self.conf.Filepath = filepath.Join(logPath, "rationaldb-op-log")
	}

	if self.conf.Test {
		os.RemoveAll(self.conf.Filepath)
	}

	// check if filepath existed
	if stat, err := os.Stat(self.conf.Filepath); os.IsNotExist(err) {
		if e := os.Mkdir(self.conf.Filepath, logPathPrimitives); e != nil {
			return e
		}
	} else if !stat.IsDir() {
		return fmt.Errorf("Filepath %v exists a file", self.conf.Filepath)
	}
	// open a log file, we need to make sure the file is not exist
	var filename string
	// we make sure that 1000 is enough to find a suitable log file
	var i = 1
	for ; i <= 10; i++ {
		filename = makeLogFileName(self.conf.Filepath, i)
		//file not exists
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			self.filenum = i
			self.filename = filename
			break
		} else {
			fmt.Println(err)
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
	fmt.Println(self.curlog_opendate)
	return nil
}

//open the logfile
func (self *FileLogWriter) createLogFile() (*os.File, error) {
	file, err := os.OpenFile(self.filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, logFilePrimitives)
	return file, err
}

// rorate the log file
func (self *FileLogWriter) DoRotate() error {
	self.mw.Lock()
	defer self.mw.Unlock()

	// close the current log file
	self.mw.fd.Close()

	// increase the file num and change the current file name
	self.filenum++
	self.filename = makeLogFileName(self.conf.Filepath, self.filenum)

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
		if !info.IsDir() && info.ModTime().Unix() < (time.Now().Unix()-60*60*24*self.conf.Maxdays) {
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

	if (self.conf.Maxlines > 0 && self.curlines >= self.conf.Maxlines) ||
		(self.conf.Maxsize > 0 && self.cursize >= self.conf.Maxsize) ||
		(self.conf.RotateDaily && time.Now().Day() != self.curlog_opendate) {
		fmt.Println(self.conf.Maxlines, self.conf.Maxsize, self.curlog_opendate)
		if err := self.DoRotate(); err != nil {
			panic(fmt.Sprintf("FileLogWriter(%q): %s\n", self.filename, err))
		}
	}
	self.curlines++
	self.cursize += size
}

func (self *FileLogWriter) WriteMsg(msg string, level int) error {
	if level < self.conf.Level {
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
