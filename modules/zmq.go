// Package modules provides ...
package modules

import (
	"fmt"
	"strings"
	"sync"
	"time"

	zmq "github.com/pebbe/zmq4"
)

type ZSocket struct {
	socket *zmq.Socket
	poller *zmq.Poller
	mu     sync.Mutex
	hwm    int
	bind   string
	conn   string
}

var ztMapping = map[string]zmq.Type{
	"REQ":    zmq.REQ,
	"REP":    zmq.REP,
	"DEALER": zmq.DEALER,
	"ROUTER": zmq.ROUTER,
	"PUB":    zmq.PUB,
	"SUB":    zmq.SUB,
	"XPUB":   zmq.XPUB,
	"XSUB":   zmq.XSUB,
	"PUSH":   zmq.PUSH,
	"PULL":   zmq.PULL,
	"PAIR":   zmq.PAIR,
	"STREAM": zmq.STREAM,
}

/* {{{ func NewZSocket(t string, hwm int, bind string,conn string) (*ZSocket, error)
 *
 */
func NewZSocket(t string, hwm int, bind string, conn string) (zs *ZSocket, err error) {
	zs = new(ZSocket)
	t = strings.ToUpper(t)
	if zt, ok := ztMapping[t]; !ok {
		err = fmt.Errorf("wrong type: %s", t)
		return
	} else {
		zs.socket, _ = zmq.NewSocket(zt)
		zs.socket.SetSndhwm(hwm)
		zs.socket.SetRcvhwm(hwm)
	}
	if bind != "" {
		err = zs.socket.Bind(bind)
	}
	if err == nil && conn != "" {
		err = zs.socket.Connect(conn)
	}
	// poller(in)
	if err == nil {
		zs.poller = zmq.NewPoller()
		zs.poller.Add(zs.socket, zmq.POLLIN)
	}
	return
}

/* }}} */

/* {{{ func (zs *ZSocket) Close() (err error)
 *
 */
func (zs *ZSocket) Close() (err error) {
	zs.mu.Lock()
	defer zs.mu.Unlock()
	return zs.socket.Close()
}

/* }}} */

/* {{{ func (zs *ZSocket) Accept() (msg []string, err error) {
 *
 */
func (zs *ZSocket) Accept() (msg []string, err error) {
	zs.mu.Lock()
	defer zs.mu.Unlock()
	if sockets, e := zs.poller.Poll(2500 * time.Millisecond); e != nil {
		err = fmt.Errorf("big wrong: %s", e)
	} else if len(sockets) > 0 {
		return zs.socket.RecvMessage(0)
	}
	return
}

/* }}} */

/* {{{ func (zs *ZSocket) RecvMessage(flags int) (msg []string, err error) {
	return s.soc.RecvMessage(zmq4.Flag(flags))
 *
*/
func (zs *ZSocket) RecvMessage(flags int) (msg []string, err error) {
	zs.mu.Lock()
	defer zs.mu.Unlock()
	return zs.socket.RecvMessage(zmq.Flag(flags))
}

/* }}} */

/* {{{ func (zs *ZSocket) SendMessage(parts ...interface{}) (total int, err error) {
 *
 */
func (zs *ZSocket) SendMessage(parts ...interface{}) (total int, err error) {
	zs.mu.Lock()
	defer zs.mu.Unlock()
	return zs.socket.SendMessage(parts...)
}

/* }}} */
