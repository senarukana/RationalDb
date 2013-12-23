package log

/*import (
	"fmt"
	"github.com/senarukana/dldb/conf"
	"net/smtp"
	"strings"
	"time"
)

const (
	subjectPhrase = "dldb server CRITICAL ERROR"
)

// is used to send email via given SMTP-server
type SmtpLogWriter struct {
	username           string
	password           string
	host               string
	subject            string
	port               int
	recipientAddresses []string
	level              int
}

func NewSmtpLogWriter() LoggerInterface {
	return &SmtpLogWriter{level: LevelCritical}
}

func (self *SmtpLogWriter) Init(conf *conf.LogConfiguration) error {
	self.level = conf.SmtpLogConfiguration.Level
	return nil
}
func (self *SmtpLogWriter) WriteMsg(msg string, level int) error {
	if level < self.level {
		return nil
	}

	// Connect to the server, authenticate, set the sender and recipient,
	// and send the email all in one step.
	auth := smtp.PlainAuth("", self.username, self.password, self.host)

	content_type := "Content-Type: text/plain" + "; charset=UTF-8"
	mailmsg := []byte("To: " + strings.Join(self.recipientAddresses, ";") + "\r\nFrom: " + self.username + "<" + self.username +
		">\r\nSubject: " + self.subject + "\r\n" + content_type + "\r\n\r\n" + fmt.Sprintf(".%self", time.Now().Format("2006-01-02 15:04:05")) + msg)

	err := smtp.SendMail(
		self.host,
		auth,
		self.username,
		self.recipientAddresses,
		mailmsg,
	)

	return err
}

func (self *SmtpLogWriter) Destroy() {}

func init() {
	Register("smtp", NewSmtpLogWriter)
}
*/
