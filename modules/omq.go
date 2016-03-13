package modules

import (
	"fmt"
	"strings"

	"github.com/Odinman/goutils/zredis"
	"github.com/Odinman/ogo"
	"github.com/Odinman/omq/utils"
	zmq "github.com/pebbe/zmq4"
	"gopkg.in/redis.v3"
)

type OMQ struct {
	ogo.Worker
}

var (
	publisher *utils.Socket
	mqpool    *utils.MQPool
	Redis     *zredis.ZRedis
	cc        *redis.ClusterClient
)

func init() {
	ogo.AddWorker(&OMQ{})
}

func (o *OMQ) Main() error {
	//read config
	o.getConfig()

	// block tasks
	blockTasks = make(map[string](chan string))

	// connect local storage
	if cc = ogo.ClusterClient(); cc == nil {
		o.Info("not found cluster, use redis")
		var e error
		servers := strings.Split(redisAddr, ",")       //支持多个地址,逗号分隔
		sentinels := strings.Split(redisSentinel, ",") //支持多个地址,逗号分隔
		if Redis, e = zredis.InitZRedis(servers, sentinels, redisPwd, redisDB, redisMTag); e != nil {
			//panic(e)
			o.Error("localstorage unreachable: %s", e)
		}
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

	o.serve()

	return nil
}
