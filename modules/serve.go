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
			rp.queue <- Job{Request: msg, Conn: s}
		} else if err != nil {
			// Interrupted
			o.Error("E: %s", err)
			return err
		}
	}
}

/* }}} */
