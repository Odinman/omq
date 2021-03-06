package workers

import (
	"fmt"
	"time"

	"github.com/Odinman/omq/utils"
	zmq "github.com/pebbe/zmq4"
)

/* {{{ func connectPub() (*zmq.Socket, *zmq.Poller)
 *  Helper function that returns a new configured socket
 *  connected to the Paranoid Pirate queue
 */
func (w *OmqWorker) connectPub() (*zmq.Socket, *zmq.Poller) {
	soc, _ := zmq.NewSocket(zmq.SUB)

	//get identity
	identity, _ := utils.GetLocalIdentity(fmt.Sprint(basePort)) //防止同一台机器得到相同的identity
	soc.SetIdentity(identity)

	soc.SetRcvhwm(50000)
	soc.SetSubscribe("")

	remotePub := fmt.Sprint("tcp://", pubAddr, ":", remotePort+1)
	soc.Connect(remotePub)
	w.Debug("identity(%s) connect remote pub: %v", identity, remotePub)

	poller := zmq.NewPoller()
	poller.Add(soc, zmq.POLLIN)

	return soc, poller
}

/* }}} */

/* {{{ func (w *OmqWorker) newSubscriber()
 * 订阅者, 订阅其他机房的信息
 */
func (w *OmqWorker) newSubscriber() {

	subscriber, poller := w.connectPub()

	//  If liveness hits zero, queue is considered disconnected
	liveness := HEARTBEAT_LIVENESS
	interval := INTERVAL_INIT

	//  Send out heartbeats at regular intervals
	heartbeat_at := time.Tick(HEARTBEAT_INTERVAL)

	lastCycles := 0
	for cycles := 0; true; {
		sockets, err := poller.Poll(HEARTBEAT_INTERVAL)
		if err != nil {
			w.Error("sub error: %s", err)
			break //  Interrupted
		}

		if len(sockets) == 1 {
			//  Get message
			//  - 3-part envelope + content -> request
			//  - 1-part HEARTBEAT -> heartbeat
			msg, err := subscriber.RecvMessage(0)
			if err != nil {
				w.Error("recv error: %s", err)
				break //  Interrupted
			}

			if len(msg) > 1 {
				cycles++

				//subscriber收到的信息应该是不包含信封的
				w.Trace("recv msg: %q", msg)

				// 存到本地存储(同步)
				if err := w.localStorage(msg); err != nil {
					w.Debug("error: %s", err)
				}

				liveness = HEARTBEAT_LIVENESS
			} else if len(msg) == 1 {
				//  When we get a heartbeat message from the queue, it means the
				//  queue was (recently) alive, so reset our liveness indicator:
				if msg[0] == PPP_HEARTBEAT {
					w.Trace("recv heartbeat, refresh liveness")
					liveness = HEARTBEAT_LIVENESS
				} else {
					w.Debug("Error: invalid message, %q", msg)
				}
			} else {
				w.Debug("E: invalid message: %q", msg)
			}
			interval = INTERVAL_INIT
		} else {
			//  If the queue hasn't sent us heartbeats in a while, destroy the
			//  socket and reconnect. This is the simplest most brutal way of
			//  discarding any messages we might have sent in the meantime://
			liveness--
			if liveness == 0 {
				w.Error("W: heartbeat failure, can't reach pub, reconnecting in %s", interval)
				time.Sleep(interval)

				if interval < INTERVAL_MAX { //每次重试都加大重试间隔
					interval = 2 * interval
				}
				// reconnect
				subscriber, poller = w.connectPub()
				liveness = HEARTBEAT_LIVENESS
			}
		}
		//  Send heartbeat to queue if it's time
		select {
		case <-heartbeat_at:
			if cycles > lastCycles {
				w.Debug("subscriber worked cycles: %d", cycles)
				lastCycles = cycles
			}
		default:
		}
	}
}

/* }}} */
