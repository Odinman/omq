package modules

import ()

/* {{{ func (o *OMQ) serve(s *ZSocket) error {
 *
 */
func (o *OMQ) serve(s *ZSocket) error {
	defer s.Close()

	// loop
	for {
		if msg, err := s.Accept(); err == nil && len(msg) > 0 {
			//o.Debug("[serve] recv msg: [%s]", msg)
			JobQueue <- Job{Request: msg, Conn: s}
		}
	}
}

/* }}} */
