package modules

import (
	"fmt"
)

/* {{{ func (o *OMQ) serve() error {
 *
 */
func (o *OMQ) serve() error {
	// block tasks
	o.blockTasks = make(map[string](chan string))
	// create response pool
	rp := NewWorkerPool(responseNodes, o.response)
	rp.Run()

	s, _ := NewZSocket("ROUTER", 50000, fmt.Sprint("tcp://*:", basePort), "")
	defer s.Close()

	// loop
	for {
		if msg, err := s.Accept(); err == nil && len(msg) > 0 {
			//o.Debug("[serve] recv msg: [%s]", msg)
			rp.queue <- Job{Request: msg, Conn: s}
		}
	}
}

/* }}} */
