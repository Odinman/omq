package workers

import (
	"time"

	"github.com/Odinman/ogo"
)

const (
	HEARTBEAT_LIVENESS = 4                        //  3-5 is reasonable
	HEARTBEAT_INTERVAL = 1000 * time.Millisecond  //  msecs
	INTERVAL_INIT      = 1000 * time.Millisecond  //  Initial reconnect
	INTERVAL_MAX       = 32000 * time.Millisecond //  After exponential backoff

	PPP_READY     = "\001" //  Signals worker is ready
	PPP_HEARTBEAT = "\002" //  Signals worker heartbeat
)

//config var
var (
	basePort      int
	remotePort    int
	redisAddr     string
	redisSentinel string
	redisPwd      string
	redisDB       string
	redisMTag     string
	pubAddr       string
	mqBuffer      int

	responseNodes int // 回复节点的个数
)

//get worker config from ogo
func (w *OmqWorker) getConfig() {
	workerConfig := ogo.Config()
	//base port
	if port, err := workerConfig.Int("base_port"); err == nil {
		basePort = port
	} else {
		basePort = 7000 //default is 7000
	}
	// local redis addr
	if raddr := workerConfig.String("redis_addr"); raddr != "" {
		redisAddr = raddr
	}
	// local redis sentinels
	if saddr := workerConfig.String("redis_sentinel"); saddr != "" {
		redisSentinel = saddr
	}
	// local redis passwd
	if rpwd := workerConfig.String("redis_pwd"); rpwd != "" {
		redisPwd = rpwd
	}
	// local redis db
	if rdb := workerConfig.String("redis_db"); rdb != "" {
		redisDB = rdb
	}
	// sentinel master tag
	if mtag := workerConfig.String("redis_mtag"); mtag != "" {
		redisMTag = mtag
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
		responseNodes = 10 // default is 10
	}

	if mqb, err := workerConfig.Int("msgqueue_buffer"); err == nil {
		mqBuffer = mqb
	} else {
		mqBuffer = 1000 // default is 1000
	}
}
