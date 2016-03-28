package modules

import (
	"fmt"
	"time"

	"github.com/Odinman/ogo"
	"github.com/Odinman/omq/utils"
	"github.com/dustin/randbo"
	"gopkg.in/redis.v3"
)

type OMQ struct {
	server     *utils.ZServer
	wp         *utils.WorkerPool
	pub        *utils.ZSocket
	mqPool     *utils.MQPool
	blockTasks map[string](chan string)
	ogo.Worker
}

type Request struct {
	Client  string `json:"client,omitempty"`
	act     string
	Command []string `json:"command,omitempty"`
	conn    *utils.ZSocket
	session string
	access  *ogo.Access
}

var (
	cc *redis.ClusterClient
)

func init() {
	ogo.AddWorker(&OMQ{})
}

/* {{{ func (o *OMQ) inHandler(msg []string, writer *utils.ZSocket) error
 *
 */
func (o *OMQ) inHandler(msg []string, writer *utils.ZSocket) {
	// build payload
	client, cmd := utils.Unwrap(msg)
	request := NewRequest(client, cmd, writer)
	// create a job
	job := utils.NewJob(request)
	// save job in access
	request.access.App = job
	// push job to worker pool
	o.wp.Push(job)
}

/* }}} */

/* {{{ func (o *OMQ) outHandler(msg []stroutg, writer *utils.ZSocket) error
 *
 */
func (o *OMQ) outHandler(msg []string, writer *utils.ZSocket) {
	l := len(msg)
	start, _ := time.Parse(time.RFC3339Nano, msg[l-1])
	writer.SendMessage(msg[0 : l-1])
	o.Debug("total duration: %s", time.Now().Sub(start))
}

/* }}} */

/* {{{ func NewRequest(client string,cmd []string, writer *utils.ZSocket) *Request
 *
 */
func NewRequest(client string, cmd []string, writer *utils.ZSocket) *Request {
	r := new(Request)
	// session
	buf := make([]byte, 16)
	randbo.New().Read(buf)
	r.session = fmt.Sprintf("%x", buf)
	r.Client = client
	if len(cmd) > 1 {
		r.act = cmd[0]
		r.Command = cmd
	}
	if writer != nil {
		r.conn = writer
	}
	r.access = ogo.NewAccess(service, r.session)
	return r
}

/* }}} */

/* {{{ func (r *Request) SaveAccess(rt []string)
 *
 */
func (r *Request) SaveAccess(rt []string) {
	if rt[0] == RESPONSE_NIL && (r.act == COMMAND_POP || r.act == COMMAND_BPOP) {
		// BPOP&POP操作没有返回时, 不记录
	} else {
		r.access.Save()
	}
}

/* }}} */

/* {{{ func (o *OMQ) Main() error
 *
 */
func (o *OMQ) Main() error {
	//read config
	o.getConfig()

	// connect local storage
	if cc = ogo.ClusterClient(); cc == nil {
		o.Error("localstorage unreachable")
	} else {
		o.Info("found cluster, gooood!")
	}

	// create worker pool, and regist job function
	o.wp = utils.NewWorkerPool(responseNodes, o.response)
	o.wp.Run()

	// 订阅其他server发布的内容
	if pubAddr != "" {
		o.newSubscriber()
	}

	// publisher, port = baseport + 1
	o.pub, _ = utils.NewZSocket("PUB", 65536, fmt.Sprint("tcp://*:", basePort+1))
	defer o.pub.Close()

	// block tasks
	o.blockTasks = make(map[string](chan string))
	// mq pool
	o.mqPool = utils.NewMQPool()
	defer o.mqPool.Destroy()

	o.server, _ = utils.NewZServer(o.inHandler, o.outHandler, fmt.Sprint("tcp://*:", basePort))
	defer o.server.Close()

	return o.server.Serve()
}

/* }}} */
