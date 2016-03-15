package modules

import (
	"fmt"

	"github.com/Odinman/ogo"
	"github.com/Odinman/omq/utils"
	zmq "github.com/pebbe/zmq4"
	"gopkg.in/redis.v3"
)

type OMQ struct {
	blockTasks map[string](chan string)
	ogo.Worker
}

var (
	publisher *utils.Socket
	mqpool    *utils.MQPool
	cc        *redis.ClusterClient
)

func init() {
	ogo.AddWorker(&OMQ{})
}

func (o *OMQ) Main() error {
	//read config
	o.getConfig()

	// connect local storage
	if cc = ogo.ClusterClient(); cc == nil {
		o.Error("localstorage unreachable")
	} else {
		o.Info("found cluster, gooood!")
	}

	// Socket to pub
	publisher = utils.NewSocket(zmq.PUB, 50000)
	defer publisher.Close()
	publisher.Bind(fmt.Sprint("tcp://*:", basePort+1))
	o.Debug("publisher bind port: %v", basePort+1)

	mqpool = utils.NewMQPool()
	defer mqpool.Destroy()

	// 订阅其他server发布的内容
	if pubAddr != "" {
		go o.newSubscriber()
	}

	return o.serve()
}
