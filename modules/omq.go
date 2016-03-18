package modules

import (
	"github.com/Odinman/ogo"
	"github.com/Odinman/omq/utils"
	"gopkg.in/redis.v3"
)

type OMQ struct {
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

	return o.serve()
}
