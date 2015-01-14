package workers

import (
	"math/rand"
	"strings"
	"time"

	"github.com/Odinman/omq/utils"
	zmq "github.com/pebbe/zmq4"
)

/* {{{ func connectQueue() (*zmq.Socket, *zmq.Poller)
 *  Helper function that returns a new configured socket
 *  connected to the Paranoid Pirate queue
 */
func (w *OmqWorker) connectQueue() (*zmq.Socket, *zmq.Poller) {
	soc, _ := zmq.NewSocket(zmq.DEALER)
	soc.Connect("inproc://backend")

	//  Tell queue we're ready for work
	soc.Send(PPP_READY, 0)

	poller := zmq.NewPoller()
	poller.Add(soc, zmq.POLLIN)

	return soc, poller
}

/* }}} */

/* {{{ func (w *OmqWorker) newResponser(i int)
 * 回复节点
 */
func (w *OmqWorker) newResponser(i int) {
	node, poller := w.connectQueue()
	w.Trace("%d node ready", i)

	//  If liveness hits zero, queue is considered disconnected
	liveness := HEARTBEAT_LIVENESS
	interval := INTERVAL_INIT

	//  Send out heartbeats at regular intervals
	heartbeat_at := time.Tick(HEARTBEAT_INTERVAL)

	rand.Seed(time.Now().UnixNano())
	lastCycles := 0
	for cycles := 0; true; {
		sockets, err := poller.Poll(HEARTBEAT_INTERVAL)
		if err != nil {
			w.Error("polling error: %s", err)
			break //  Interrupted
		}

		if len(sockets) == 1 {
			//  Get message
			//  - n-part envelope + content -> request
			//  - 1-part HEARTBEAT -> heartbeat
			msg, err := node.RecvMessage(0)
			if err != nil {
				w.Error("recv error: %s", err)
				break //  Interrupted
			}

			if len(msg) >= 4 { //命令应该大于5帧(包含信封以及空帧)
				cycles++

				client, cmd := utils.Unwrap(msg)

				w.Trace("recv cmd: %s, from client: %q", cmd, client)

				// 假设每个node花了10毫秒做事(测试用,这个可以充分证明多个node的好处)
				//time.Sleep(10 * time.Millisecond)

				act := strings.ToUpper(cmd[0])
				key := cmd[1]
				switch act {
				case "GET", "SET", "DEL": //key-value命令
					// 存到本地存储(同步)
					//回复结果(带信封, 否则找不到发送者), 因为是异步的, 可以先回复, 再做事
					if err := w.localStorage(cmd); err != nil {
						w.Debug("error: %s", err)
						node.SendMessage(client, "", "ERROR") //回复REQ,因此要加上一个空帧
					} else {
						node.SendMessage(client, "", "OK") //回复REQ,因此要加上一个空帧
					}

					// 发布(目标是跨IDC多点发布)
					publisher.SendMessage(cmd)

				case "PUSH", "TASK": //任务队列命令
					value := cmd[2:]
					if err := mqpool.Push(key, value); err == nil {
						node.SendMessage(client, "", "OK")
					} else {
						w.Debug("push %s failed: %s", key, err)
						node.SendMessage(client, "", err.Error())
					}
				case "POP":
					//cmd, _ := mqueuer.RecvMessage(0)
					if value, err := mqpool.Pop(key); err == nil {
						w.Trace("pop value from mqueue: %s", value)
						node.SendMessage(client, "", "OK", value) //回复REQ,因此要加上一个空帧
					} else {
						w.Debug("pop %s from mqueue failed: %s", key, err)
						node.SendMessage(client, "", err.Error()) //回复REQ,因此要加上一个空帧
					}

				default:
					// unknown action
					w.Debug("unkown action: %s", act)
					node.SendMessage(client, "", "UNKOWN")
				}

				// 回满血, 结束
				liveness = HEARTBEAT_LIVENESS
			} else if len(msg) == 1 {
				//  When we get a heartbeat message from the queue, it means the
				//  queue was (recently) alive, so reset our liveness indicator:
				if msg[0] == PPP_HEARTBEAT {
					liveness = HEARTBEAT_LIVENESS
				} else {
					w.Debug("invalid message: %q", msg)
				}
			} else {
				w.Debug("invalid message: %q", msg)
			}
			interval = INTERVAL_INIT
		} else {
			//  If the queue hasn't sent us heartbeats in a while, destroy the
			//  socket and reconnect. This is the simplest most brutal way of
			//  discarding any messages we might have sent in the meantime://
			liveness--
			if liveness == 0 {
				w.Debug("W: heartbeat failure, can't reach queue, reconnecting in %s", interval)

				time.Sleep(interval)

				if interval < INTERVAL_MAX { //每次重试都加大重试间隔
					interval = 2 * interval
				}
				// reconnect
				node, poller = w.connectQueue()
				liveness = HEARTBEAT_LIVENESS
			}
		}

		//  Send heartbeat to queue if it's time
		select {
		case <-heartbeat_at:
			if cycles > lastCycles {
				w.Debug("node%d worked cycles: %d", i, cycles)
				lastCycles = cycles
			}
			node.Send(PPP_HEARTBEAT, 0)
		default:
		}
	}
}

/* }}} */
