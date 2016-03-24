package modules

import (
	"fmt"

	"github.com/Odinman/ogo"
	"github.com/Odinman/omq/utils"
	"gopkg.in/redis.v3"
)

type OMQ struct {
	server     *ZServer
	wp         *WorkerPool
	pub        *ZSocket
	mqPool     *utils.MQPool
	blockTasks map[string](chan string)
	ogo.Worker
}

type Request struct {
	Client  string `json:"client,omitempty"`
	act     string
	Command []string `json:"command,omitempty"`
}

/* {{{ func (o *OMQ) serveHandler(msg []string, writer *ZSocket) error
 *
 */
func (o *OMQ) serveHandler(msg []string, writer *ZSocket) {
	o.wp.queue <- NewJob(msg, writer)
}

/* }}} */

/* {{{ func NewRequest(msg []string) *Request
 *
 */
func NewRequest(msg []string) *Request {
	r := new(Request)
	client, cmd := utils.Unwrap(msg)
	r.Client = client
	if len(cmd) > 1 {
		r.act = cmd[0]
		r.Command = cmd
	}
	return r
}

/* }}} */

var (
	cc *redis.ClusterClient
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

	// 订阅其他server发布的内容
	if pubAddr != "" {
		o.newSubscriber()
	}

	// publisher
	o.pub, _ = NewZSocket("PUB", 50000, fmt.Sprint("tcp://*:", basePort+1))
	defer o.pub.Close()

	// block tasks
	o.blockTasks = make(map[string](chan string))
	// mq pool
	o.mqPool = utils.NewMQPool()
	defer o.mqPool.Destroy()

	// create response pool, and regist job function
	o.wp = NewWorkerPool(responseNodes, o.response)
	o.wp.Run()

	o.server, _ = NewZServer(o.serveHandler, fmt.Sprint("tcp://*:", basePort))
	defer o.server.Close()

	return o.server.Serve()
}
