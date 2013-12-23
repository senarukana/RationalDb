package log

/* this file is used to send log to the remote server
   you can define the configuration in dldb.conf, the configuration is like this:
   [logConn]
	reconnect = 0
	level = 3
	address = 192.168.0.2
	protocol = "tcp"
	port = 3080
*/

/*import (
	"io"
	syslog "log"
	"net"
)

const (
	sectionName = "logConn"
)

type ConnLogWriter struct {
	*syslog.Logger
	innerWirter io.WriteCloser

	//if the remote server is down, will it retry to connect the server
	maxRetries  int
	isConnected bool
	level       int
	protocol    string
	addr        string
	port        int
}

func NewConnLog() LoggerInterface {
	conn := new(ConnLogWriter)
	conn.level = LevelTrace
	conn.maxRetries = 1
	conn.isConnected = false
	return conn
}

func (self *ConnLogWriter) Init(conf *conf.LogConfiguration) error {
	self.level = conf.RemoteConnLogConfiguration.Level
	self.maxRetries = conf.RemoteConnLogConfiguration.MaxRetries
	self.addr = conf.RemoteConnLogConfiguration.Host
	self.port = conf.RemoteConnLogConfiguration.Port
	return nil
}

func (self *ConnLogWriter) WriteMsg(msg string, level int) error {
	if level < self.level {
		return nil
	}
	// the connnection is not established
	if !self.isConnected {
		i := 0
		for ; i < self.maxRetries; i++ {
			err := self.connect()
			// connection is ok
			if err == nil {
				break
			}
		}
	}
	if self.isConnected {
		self.Logger.Println(msg)
	}
	return nil
}

func (self *ConnLogWriter) Destroy() {
	if self.innerWirter == nil {
		return
	}
	self.innerWirter.Close()
}

func (self *ConnLogWriter) connect() error {
	if self.innerWirter != nil {
		self.innerWirter.Close()
		self.innerWirter = nil
	}

	conn, err := net.Dial(self.protocol, self.addr)
	if err != nil {
		return err
	}
	self.isConnected = true
	// if protocol is tcp, set keep alive options
	tcpConn, ok := conn.(*net.TCPConn)
	if ok {
		tcpConn.SetKeepAlive(true)
	}
	self.innerWirter = conn
	self.Logger = syslog.New(conn, "", syslog.Ldate|syslog.Ltime)
	return nil
}

func init() {
	Register("conn", NewConnLog)
}
*/
