package servenv

import (
	"fmt"
	"net/http"

	"github.com/senarukana/rationaldb/log"
	"github.com/senarukana/rationaldb/proc"
)

var (
	onCloseHooks hooks
)

// Run starts listening for RPC and HTTP requests on the given port,
// and blocks until it the process gets a signal.
func Run(port int) {
	onRunHooks.Fire()
	RunSecure(port, 0, "", "", "")
}

// RunSecure is like Run, but it additionally listens for RPC and HTTP
// requests using TLS on securePort, using the passed certificate,
// key, and CA certificate.
func RunSecure(port int, securePort int, cert, key, caCert string) {
	onRunHooks.Fire()
	ServeRPC()

	l, err := proc.Listen(fmt.Sprintf("%v", port))
	if err != nil {
		log.Critical(err.Error())
	}

	go http.Serve(l, nil)

	if securePort != 0 {
		log.Info("listening on secure port %v", securePort)
		SecureServe(fmt.Sprintf(":%d", securePort), cert, key, caCert)
	}
	proc.Wait()
	Close()
}

// Close runs any registered exit hooks in parallel.
func Close() {
	onCloseHooks.Fire()
}

// OnClose registers f to be run at the end of the app lifecycle. All
// hooks are run in parallel.
func OnClose(f func()) {
	onCloseHooks.Add(f)
}
