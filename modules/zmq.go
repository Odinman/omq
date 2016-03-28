// Package modules provides ...
package modules

import (
	"fmt"
	"strings"
	"sync"
	"time"

	zmq "github.com/pebbe/zmq4"
)

var (
	counter = 0
	lock    sync.Mutex

	ztMapping = map[string]zmq.Type{
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
)

/* {{{ func nextId() int
 *
 */
func nextId() int {
	lock.Lock()
	defer lock.Unlock()
	counter++
	return counter
}

/* }}} */

type ZSocket struct {
	mu        sync.RWMutex
	typ       zmq.Type
	socket    *zmq.Socket
	poller    *zmq.Poller // POLLIN
	hwm       int
	bind      string // bind 地址
	connect   string // 连接地址
	identity  string // identity
	subFilter string // subscribe 过滤
}

type ServeFunc func([]string, *ZSocket)

type ZPair struct {
	addr string
	push *ZSocket
	pop  *ZSocket
}

type ZServer struct {
	In       *ZSocket
	Out      *ZPair
	incoming ServeFunc
	outgoing ServeFunc
}

/* {{{ func NewZServer(in ServeFunc, out ServeFunc, addr string) (*ZServer, error)
 *
 */
func NewZServer(in ServeFunc, out ServeFunc, addr string) (os *ZServer, err error) {
	os = new(ZServer)
	// 对外是一个ROUTER
	if os.In, err = NewZSocket("ROUTER", 65536, addr); err != nil {
		return
	}
	// 建立一个内置的pair通道, 作为response专用通道
	os.Out = NewZPair()
	// 自定义handler
	os.incoming = in
	os.outgoing = out
	return
}

/* }}} */

/* {{{ func (s *ZServer) Serve() (err error)
 *
 */
func (s *ZServer) Serve() (err error) {
	in := s.In
	outPop := s.Out.pop   // output read queue
	outPush := s.Out.push // output write queue
	poller := zmq.NewPoller()
	poller.Add(in.socket, zmq.POLLIN)
	poller.Add(outPop.socket, zmq.POLLIN)

	for {
		if sockets, e := poller.Poll(100 * time.Millisecond); e == nil {
			if len(sockets) > 0 {
				for _, socket := range sockets {
					switch socket.Socket {
					case in.socket: // request
						msg, _ := in.RecvMessage(0)
						s.incoming(msg, outPush)
					case outPop.socket: // response
						msg, _ := outPop.RecvMessage(0)
						s.outgoing(msg, in)
					}
				}
			}
		} else {
			err = fmt.Errorf("poll error: %s", e)
		}
	}

	return
}

/* }}} */

/* {{{ func (os *ZServer) Close() (err error)
 *
 */
func (os *ZServer) Close() (err error) {
	if err = os.In.Close(); err != nil {
		return
	}
	return os.Out.Close()
}

/* }}} */

/* {{{ func NewZSocket(t string, hwm int, opts ...string) (*ZSocket, error)
 *
 */
func NewZSocket(t string, hwm int, opts ...string) (zs *ZSocket, err error) {
	zs = new(ZSocket)
	t = strings.ToUpper(t)
	if zt, ok := ztMapping[t]; !ok {
		err = fmt.Errorf("wrong type: %s", t)
		return
	} else {
		zs.typ = zt
	}
	zs.hwm = hwm
	//var bind, connect, identity, subscribe string
	if len(opts) > 0 {
		for i, opt := range opts {
			switch i {
			case 0:
				zs.bind = opt
			case 1:
				zs.connect = opt
			case 2:
				zs.identity = opt
			case 3:
				zs.subFilter = opt
			default:
				break
			}
		}
	}
	if err = zs.New(); err == nil {
		// bind
		if err = zs.Bind(); err == nil {
			// connect
			err = zs.Connect()
		}
	}
	return
}

/* }}} */

/* {{{ func (zs *ZSocket) New() (err error)
 *
 */
func (zs *ZSocket) New() (err error) {
	zs.mu.Lock()
	defer zs.mu.Unlock()
	if zs.socket, err = zmq.NewSocket(zs.typ); err == nil {
		if zs.identity != "" {
			if err = zs.socket.SetIdentity(zs.identity); err != nil {
				return
			}
		}
		if zs.hwm > 0 {
			zs.socket.SetSndhwm(zs.hwm)
			zs.socket.SetRcvhwm(zs.hwm)
		}
		if zs.typ == zmq.SUB {
			zs.socket.SetSubscribe(zs.subFilter)
		}
	}
	return
}

/* }}} */

/* {{{ func (zs *ZSocket) Bind() (err error)
 *
 */
func (zs *ZSocket) Bind() (err error) {
	if zs.bind != "" {
		zs.mu.Lock()
		defer zs.mu.Unlock()
		return zs.socket.Bind(zs.bind)
	}
	return
}

/* }}} */

/* {{{ func (zs *ZSocket) Connect() (err error)
 *
 */
func (zs *ZSocket) Connect() (err error) {
	if zs.connect != "" {
		zs.mu.Lock()
		defer zs.mu.Unlock()
		return zs.socket.Connect(zs.connect)
	}
	return
}

/* }}} */

/* {{{ func (zs *ZSocket) GetPoller(event string) (err error)
 *
 */
func (zs *ZSocket) GetPoller(event string) (poller *zmq.Poller, err error) {
	if zs.poller == nil {
		poller = zmq.NewPoller()
		if strings.ToUpper(event) == "POLLOUT" {
			poller.Add(zs.socket, zmq.POLLOUT)
		} else {
			poller.Add(zs.socket, zmq.POLLIN)
		}
		return
	} else {
		return zs.poller, nil
	}
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
	zs.mu.RLock()
	defer zs.mu.RUnlock()
	poller, _ := zs.GetPoller("POLLIN")
	if sockets, e := poller.Poll(1 * time.Millisecond); e != nil {
		//if sockets, e := poller.Poll(0 * time.Millisecond); e != nil {
		err = e
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

/* {{{ func NewZPair() *ZPair
 * 建立一个连接的PAIR对
 */
func NewZPair() *ZPair {
	zp := new(ZPair)
	zp.addr = fmt.Sprintf("inproc://socket-pair-%d", nextId())
	zp.pop, _ = NewZSocket("PAIR", 65536, zp.addr)
	zp.push, _ = NewZSocket("PAIR", 65536, "", zp.addr)
	return zp
}

/* }}} */

/* {{{ func (zp *ZPair) Close() (err error)
 *
 */
func (zp *ZPair) Close() (err error) {
	if err = zp.push.Close(); err != nil {
		return
	}
	return zp.pop.Close()
}

/* }}} */
