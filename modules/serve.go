package modules

import ()

/* {{{ func (o *OMQ) serve(s *ZSocket) error {
 *
 */
func (o *OMQ) serve(s *ZSocket) error {
	defer s.Close()

	// loop
	for {
		if msg, err := s.Accept(); err == nil {
			o.Debug("recv msg: [%s]", msg)
		}
	}
}

/* }}} */
