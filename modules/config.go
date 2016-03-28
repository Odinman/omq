package modules

import (
	"errors"
	"time"

	"github.com/Odinman/ogo"
)

const (
	HEARTBEAT_LIVENESS = 4                        //  3-5 is reasonable
	HEARTBEAT_INTERVAL = 1000 * time.Millisecond  //  msecs
	INTERVAL_INIT      = 1000 * time.Millisecond  //  Initial reconnect
	INTERVAL_MAX       = 32000 * time.Millisecond //  After exponential backoff
	BTASK_TIMEOUT      = 10 * time.Second

	PPP_READY     = "\001" //  Signals worker is ready
	PPP_HEARTBEAT = "\002" //  Signals worker heartbeat

	//cmd
	COMMAND_GET      = "GET"
	COMMAND_SET      = "SET"
	COMMAND_DEL      = "DEL"
	COMMAND_PUSH     = "PUSH"
	COMMAND_TASK     = "TASK"
	COMMAND_BTASK    = "BTASK"    //阻塞任务
	COMMAND_COMPLETE = "COMPLETE" //完成阻塞任务
	COMMAND_POP      = "POP"
	COMMAND_BPOP     = "BPOP"
	COMMAND_SCHEDULE = "SCHEDULE" //定时任务
	COMMAND_TIMING   = "TIMING"   //定时触发

	//response
	RESPONSE_OK      = "OK"
	RESPONSE_ERROR   = "ERROR"
	RESPONSE_NIL     = "NIL"
	RESPONSE_UNKNOWN = "UNKNOWN"
)

//config var
var (
	service    string
	basePort   int
	remotePort int
	pubAddr    string
	mqBuffer   int

	responseNodes int // 回复节点的个数

	ErrNil = errors.New(RESPONSE_NIL)
)

//get worker config from ogo
func (o *OMQ) getConfig() {
	workerConfig := ogo.Config()
	// service name
	if sn := workerConfig.String("service_name"); sn != "" {
		service = sn
	} else {
		service = "omqd"
	}
	//base port
	if port, err := workerConfig.Int("base_port"); err == nil {
		basePort = port
	} else {
		basePort = 7000 //default is 7000
	}

	// remote publisher
	if rp, err := workerConfig.Int("remote_port"); err == nil {
		remotePort = rp
	} else {
		remotePort = basePort //default is basePort
	}
	if pub := workerConfig.String("remote_publisher"); pub != "" {
		pubAddr = pub
	}

	if rns, err := workerConfig.Int("responser_nodes"); err == nil {
		responseNodes = rns
	} else {
		responseNodes = 1024 // default is 1024
	}

	if mqb, err := workerConfig.Int("msgqueue_buffer"); err == nil {
		mqBuffer = mqb
	} else {
		mqBuffer = 1000 // default is 1000
	}
}
