package modules

import (
	"fmt"
	"time"

	"github.com/Odinman/omq/utils"
)

/* {{{ func (o *OMQ) newSubscriber()
 * 订阅者, 订阅其他机房的信息
 */
func (o *OMQ) newSubscriber() {

	go func() {
		identity, _ := utils.GetLocalIdentity(fmt.Sprint(basePort)) //防止同一台机器得到相同的identity
		subscriber, _ := NewZSocket("SUB", 50000, "", fmt.Sprint("tcp://", pubAddr, ":", remotePort+1), identity)

		//  If liveness hits zero, queue is considered disconnected
		liveness := HEARTBEAT_LIVENESS
		interval := INTERVAL_INIT

		//  Send out heartbeats at regular intervals
		heartbeat_at := time.Tick(HEARTBEAT_INTERVAL)

		lastCycles := 0
		for cycles := 0; true; {
			if msg, err := subscriber.Accept(); err == nil {
				//  Get message
				//  - 2+ part content -> request
				//  - 1-part HEARTBEAT -> heartbeat
				if len(msg) > 1 {
					cycles++

					//subscriber收到的信息应该是不包含信封的
					o.Trace("recv msg: %q", msg)

					o.execCommand(msg)
					//// 存到本地存储(同步)
					//if err := o.localStorage(msg); err != nil {
					//	o.Debug("error: %s", err)
					//}

					liveness = HEARTBEAT_LIVENESS
				} else if len(msg) == 1 {
					//  When we get a heartbeat message from the queue, it means the
					//  queue was (recently) alive, so reset our liveness indicator:
					if msg[0] == PPP_HEARTBEAT {
						o.Trace("recv heartbeat, refresh liveness")
						liveness = HEARTBEAT_LIVENESS
					} else {
						o.Debug("E: invalid message, %q", msg)
					}
				} else {
					o.Debug("E: invalid message: %q", msg)
				}
				interval = INTERVAL_INIT
			} else {
				//  If the queue hasn't sent us heartbeats in a while, destroy the
				//  socket and reconnect. This is the simplest most brutal way of
				//  discarding any messages we might have sent in the meantime://
				liveness--
				if liveness == 0 {
					o.Error("W: heartbeat failure, can't reach pub, reconnecting in %s", interval)
					time.Sleep(interval)

					if interval < INTERVAL_MAX { //每次重试都加大重试间隔
						interval = 2 * interval
					}
					// reconnect
					subscriber, _ = NewZSocket("SUB", 50000, "", fmt.Sprint("tcp://", pubAddr, ":", remotePort+1), identity)
					liveness = HEARTBEAT_LIVENESS
				}
			}
			//  Send heartbeat to queue if it's time
			select {
			case <-heartbeat_at:
				if cycles > lastCycles {
					o.Debug("subscriber worked cycles: %d", cycles)
					lastCycles = cycles
				}
			default:
			}
		}
	}()
}

/* }}} */
