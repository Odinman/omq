//这里提供Mutex锁, 如果需要多线程访问zmq的socket,使用这个
package utils

import (
	"sync"

	zmq "github.com/pebbe/zmq4"
)

type Socket struct {
	lock *sync.Mutex
	soc  *zmq.Socket
	hwm  int
}

/* {{{  func NewSocket() *Socket
 *
 */
func NewSocket(zt zmq.Type, hwm int) *Socket {
	soc, _ := zmq.NewSocket(zt)
	soc.SetSndhwm(hwm)
	soc.SetRcvhwm(hwm)
	return &Socket{
		lock: new(sync.Mutex),
		soc:  soc,
		hwm:  hwm,
	}
}

/* }}} */

/* {{{ func (s *Socket) Close() error
 *
 */
func (s *Socket) Close() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.soc.Close()
}

/* }}} */

/* {{{ func (s *Socket) Bind(endpoint string) error
 *
 */
func (s *Socket) Bind(endpoint string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.soc.Bind(endpoint)
}

/* }}} */

/* {{{ func (s *Socket) Connect(endpoint string) error
 *
 */
func (s *Socket) Connect(endpoint string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.soc.Connect(endpoint)
}

/* }}} */

/* {{{ func (s *Socket) RecvMessage(flags int) (msg []string, err error) {
	return s.soc.RecvMessage(zmq4.Flag(flags))
 *
*/
func (s *Socket) RecvMessage(flags int) (msg []string, err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.soc.RecvMessage(zmq.Flag(flags))
}

/* }}} */

/* {{{ func (s *Socket) SendMessage(parts ...interface{}) (total int, err error) {
 *
 */
func (s *Socket) SendMessage(parts ...interface{}) (total int, err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.soc.SendMessage(parts...)
}

/* }}} */
