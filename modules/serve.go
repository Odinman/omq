package modules

import (
	"fmt"

	"github.com/Odinman/omq/utils"
)

/* {{{ func (o *OMQ) serve() error {
 *
 */
func (o *OMQ) serve() error {
	// publisher
	o.pub, _ = NewZSocket("PUB", 50000, fmt.Sprint("tcp://*:", basePort+1))
	defer o.pub.Close()
	// block tasks
	o.blockTasks = make(map[string](chan string))
	// mq pool
	o.mqPool = utils.NewMQPool()
	defer o.mqPool.Destroy()
	// create response pool
	rp := NewWorkerPool(responseNodes, o.response)
	rp.Run()

	s, _ := NewZSocket("ROUTER", 50000, fmt.Sprint("tcp://*:", basePort))
	defer s.Close()

	// loop
	for {
		if msg, err := s.Accept(); err == nil && len(msg) > 0 {
			//o.Trace("[serve] recv msg: [%s]", msg)
			rp.queue <- NewJob(msg, s)
		} else if err != nil {
			// Interrupted
			o.Error("E: %s", err)
			return err
		}
	}
}

/* }}} */

/* {{{ func (o *OMQ) response(j *Job)
 * 回复
 */
func (o *OMQ) response(j *Job) {
	conn := j.conn
	client := j.Payload.(*Request).Client
	if cmd := j.Payload.(*Request).Command; len(cmd) >= 2 { //命令应该大于5帧(包含信封以及空帧)

		o.Trace("recv cmd: %s, from client: %q", cmd, client)

		result := o.execCommand(cmd)
		conn.SendMessage(client, "", result)
		j.Result = result
	} else { //命令错误
		o.Info("invalid command: %q", j.Payload.(*Request).Command)
		conn.SendMessage(client, "", RESPONSE_UNKNOWN)
		j.Result = RESPONSE_UNKNOWN
	}

	// access log
	j.SaveAccess()
}

/* }}} */
