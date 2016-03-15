package modules

import (
	"fmt"
)

/* {{{ func (o *OMQ) serve() error {
 *
 */
func (o *OMQ) serve() error {
	// create response pool
	o.ResponsePool = NewWorkerPool(responseNodes, o.response)
	o.ResponsePool.Run()

	s, _ := NewZSocket("ROUTER", 50000, fmt.Sprint("tcp://*:", basePort), "")
	defer s.Close()

	o.Server = s

	// loop
	for {
		if msg, err := s.Accept(); err == nil && len(msg) > 0 {
			//o.Debug("[serve] recv msg: [%s]", msg)
			o.ResponsePool.Queue <- Job{Request: msg, Conn: s}
		}
	}
}

/* }}} */
