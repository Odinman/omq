package workers

import (
	"fmt"
	"strings"

	"github.com/Odinman/goutils/zredis"
	"github.com/Odinman/ogo"
	"github.com/Odinman/omq/utils"
	//"../utils"
	zmq "github.com/pebbe/zmq4"
	"gopkg.in/redis.v3"
)

type OmqWorker struct {
	ogo.Worker
}

var (
	publisher *utils.Socket
	mqpool    *utils.MQPool
	Redis     *zredis.ZRedis
	cc        *redis.ClusterClient
)

func init() {
	ogo.AddWorker(&OmqWorker{})
}

func (w *OmqWorker) Main() error {
	//read config
	w.getConfig()

	// block tasks
	blockTasks = make(map[string](chan int))

	// connect local storage
	if cc = ogo.ClusterClient(); cc == nil {
		w.Info("not found cluster, use redis")
		var e error
		servers := strings.Split(redisAddr, ",")       //支持多个地址,逗号分隔
		sentinels := strings.Split(redisSentinel, ",") //支持多个地址,逗号分隔
		if Redis, e = zredis.InitZRedis(servers, sentinels, redisPwd, redisDB, redisMTag); e != nil {
			//panic(e)
			w.Error("localstorage unreachable: %s", e)
		}
	} else {
		w.Info("found cluster, gooood!")
	}

	// Socket to pub
	publisher = utils.NewSocket(zmq.PUB, 50000)
	defer publisher.Close()
	publisher.Bind(fmt.Sprint("tcp://*:", basePort+1))
	w.Debug("publisher bind port: %v", basePort+1)

	// Socket to message queuing service
	// DEALER至少需要一个连接, 否则SendMassage会被block
	//pusher = NewSocket(zmq.DEALER, 5000)
	//defer pusher.Close()
	//pusher.Bind("inproc://pusher")

	// mqueuer 连接pusher, 这样可以把消息任务缓存到队列
	//mqueuer = NewSocket(zmq.DEALER, 5000)
	//defer mqueuer.Close()
	//mqueuer.Connect("inproc://pusher")
	mqpool = utils.NewMQPool()
	defer mqpool.Destroy()

	// 订阅其他server发布的内容
	if pubAddr != "" {
		go w.newSubscriber()
	}

	w.serve()

	return nil
}
