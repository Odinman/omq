package workers

import (
	"fmt"
	"time"

	"github.com/Odinman/omq/utils"
	zmq "github.com/pebbe/zmq4"
)

type Node struct {
	identity  string    //  Identity of worker
	id_string string    //  Printable identity
	expire    time.Time //  Expires at this time
}

/* {{{ func newNode(identity string) node
 *  Construct new worker
 */
func newNode(identity string) Node {
	return Node{
		identity:  identity,
		id_string: fmt.Sprintf("%q", identity),
		expire:    time.Now().Add(HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS),
	}
}

/* }}} */

/* {{{ func nodeReady(self Node, nodes []Node) []Node
 * 把一个ready的节点放到队列最后
 */
func nodeReady(self Node, nodes []Node) []Node {
	for i, worker := range nodes {
		if self.identity == worker.identity {
			if i == 0 { //第一个
				nodes = nodes[1:]
			} else if i == len(nodes)-1 { //最后一个
				nodes = nodes[:i]
			} else { //中间
				nodes = append(nodes[:i], nodes[i+1:]...)
			}
			break
		}
	}
	return append(nodes, self)
}

/* }}} */

/* {{{ func purgeNodes(nodes []Node) []Node
 *  The purge method looks for and kills expired nodes. We hold nodes
 *  from oldest to most recent, so we stop at the first alive worker:
 */
func purgeNodes(nodes []Node) []Node {
	now := time.Now()
	for i, worker := range nodes {
		if now.Before(worker.expire) {
			return nodes[i:] //  Worker is alive, we're done here
		}
	}
	return nodes[0:0]
}

/* }}} */

/* {{{ func (w *OmqWorker) serve() {
 *
 */
func (w *OmqWorker) serve() {
	//  接受请求的socket
	frontend, _ := zmq.NewSocket(zmq.ROUTER)
	defer frontend.Close()
	frontend.Bind(fmt.Sprint("tcp://*:", basePort))
	w.Debug("frontend bind port: %v", basePort)

	// backend
	backend, _ := zmq.NewSocket(zmq.ROUTER)
	defer backend.Close()
	backend.Bind("inproc://backend")

	//  可用节点列表,LRU算法,最空的节点保持在队列最前
	nodes := make([]Node, 0)

	// 心跳
	heartbeat_at := time.Tick(HEARTBEAT_INTERVAL)

	poller1 := zmq.NewPoller()
	poller1.Add(backend, zmq.POLLIN)

	poller2 := zmq.NewPoller()
	poller2.Add(backend, zmq.POLLIN)
	poller2.Add(frontend, zmq.POLLIN)

	// spawn responser
	w.Info("create %d responsers", responseNodes)
	for i := 1; i <= responseNodes; i++ {
		go w.newResponser(i)
	}
	// loop
	for {
		//  Poll frontend only if we have available nodes
		var sockets []zmq.Polled
		var err error
		if nl := len(nodes); nl > 0 {
			//w.Info("nodes len: %d", nl)
			sockets, err = poller2.Poll(HEARTBEAT_INTERVAL)
		} else {
			w.Info("nodes empty")
			sockets, err = poller1.Poll(HEARTBEAT_INTERVAL)
		}
		if err != nil {
			w.Critical("big wrong: %s", err)
			break //  Interrupted
		}

		for _, socket := range sockets {
			switch socket.Socket {
			case backend:
				//  Handle worker activity on backend
				//  Use worker identity for load-balancing
				msg, err := backend.RecvMessage(0)
				if err != nil {
					w.Error("backend wrong: %s", err)
					break //  Interrupted
				}

				//  Any sign of life from worker means it's ready
				identity, msg := utils.Unwrap(msg)
				nodes = nodeReady(newNode(identity), nodes)

				//  Validate control message, or return reply to client
				if len(msg) == 1 {
					// 控制信息, ready or heartbeat
					if msg[0] != PPP_READY && msg[0] != PPP_HEARTBEAT {
						w.Info("Error: invalid message from worker, %s", msg)
					}
				} else {
					// 任务处理完毕的回复(带信封), 直接返回前台
					w.Trace("backend recv: %s", msg)
					frontend.SendMessage(msg)
				}
			case frontend:
				//  Now get next client request, route to next worker
				msg, err := frontend.RecvMessage(0)
				if err != nil {
					w.Error("frontend wrong: %s", err)
					break //  Interrupted
				}
				w.Trace("frontend recv: %s", msg)

				//定向发送到后台(带信封), 将来可以有多组后台, 分别处理不同的任务
				backend.SendMessage(nodes[0].identity, msg)
				nodes = nodes[1:]
			}
		}

		//  We handle heartbeating after any socket activity. First we send
		//  heartbeats to any idle nodes if it's time. Then we purge any
		//  dead nodes:

		select {
		case <-heartbeat_at:
			//向节点发送心跳
			for _, worker := range nodes {
				backend.SendMessage(worker.identity, PPP_HEARTBEAT)
			}
			//向订阅者发送心跳
			publisher.SendMessage(PPP_HEARTBEAT)
		default:
		}
		nodes = purgeNodes(nodes)

	}
}

/* }}} */
