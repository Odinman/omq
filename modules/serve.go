package modules

import ()

/* {{{ func (o *OMQ) serve() error {
 *
 */
func (o *OMQ) serve() error {
	s := o.Server
	defer s.Close()

	// loop
	for {
		if msg, err := s.Accept(); err == nil && len(msg) > 0 {
			//o.Debug("[serve] recv msg: [%s]", msg)
			o.ResponsePool.Queue <- Job{Request: msg, Conn: s}
		}
	}
}

/* }}} */
