package conf

import (
	"testing"
)

func TestBalancerConfiguration(t *testing.T) {
	configure := InitDldbConfiguration("")
	receiveBalancer := configure.BalancersConfiguration.GetBalancerConfiguration("engine")
	if receiveBalancer.InitRoutineNum != 500 {
		t.Critical("receive balancer read error, initroutineNum = %d, want %d", receiveBalancer.InitRoutineNum, 500)
	}
	if receiveBalancer.ManagerAutoDetect {
		t.Critical("receive balancer read error, ManagerAutoDetect is true, want false")
	}
	if configure.ServerConfiguration.Addr != "localhost" {
		t.Critical("server configuration init err!")
	}
}
