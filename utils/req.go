// Package utils provides ...
package utils

import (
	"fmt"
	"time"

	zmq "github.com/pebbe/zmq4"
)

/* {{{ func ReqNewer(endpoint string) func() (*zmq.Socket, error)
 * 为了连接池准备
 */
func ReqNewer(endpoint string) func() (*zmq.Socket, error) {
	return func() (*zmq.Socket, error) {
		soc, _ := zmq.NewSocket(zmq.REQ)
		err := soc.Connect(endpoint)

		return soc, err
	}
}

/* }}} */

/* {{{ func RequestAndReply(soc *zmq.Socket, msg interface{}) (reply []string, err error) {
 * 支持超时时间,但要注意如果一个socket失败之后，因为req严格同步,连接池模式下这个socket最好销毁
 */
func RequestAndReply(soc *zmq.Socket, timeout time.Duration, msg ...interface{}) (reply []string, err error) {
	poller := zmq.NewPoller()
	poller.Add(soc, zmq.POLLIN)
	if _, err := soc.SendMessage(msg...); err != nil {
		return nil, err
	}

	if sockets, err := poller.Poll(timeout); err != nil {
		return nil, err
	} else if len(sockets) == 1 {
		return soc.RecvMessage(zmq.DONTWAIT)
	} else {
		return nil, fmt.Errorf("time out!")
	}

	return
}

/* }}} */
