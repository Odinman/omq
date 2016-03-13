package modules

import (
	"fmt"
	"time"

	zmq "github.com/pebbe/zmq4"
)

/* {{{ func (o *OMQ) serve() {
 *
 */
func (o *OMQ) serve() {
	//  接受请求的socket
	frontend, _ := zmq.NewSocket(zmq.ROUTER)
	defer frontend.Close()
	frontend.Bind(fmt.Sprint("tcp://*:", basePort))
	o.Debug("frontend bind port: %v", basePort)

	// 心跳
	heartbeat_at := time.Tick(HEARTBEAT_INTERVAL)

	poller := zmq.NewPoller()
	poller.Add(frontend, zmq.POLLIN)

	// workers
	o.Info("create %d responsers", responseNodes)
	for i := 1; i <= responseNodes; i++ {
		go o.newResponser(i)
	}
	// loop
	for {
		//  Poll frontend only if we have available nodes
		sockets, err := poller.Poll(HEARTBEAT_INTERVAL)
		if err != nil {
			o.Critical("big wrong: %s", err)
			break //  Interrupted
		}

		if len(sockets) > 0 {
			//  Now get next client request, route to next worker
			msg, err := frontend.RecvMessage(0)
			if err != nil {
				o.Error("frontend wrong: %s", err)
				break //  Interrupted
			}
			o.Trace("frontend recv: %q", msg)

		}

		select {
		case <-heartbeat_at:
			//向订阅者发送心跳
			publisher.SendMessage(PPP_HEARTBEAT)
		default:
		}

	}
}

/* }}} */
