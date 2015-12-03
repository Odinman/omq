package workers

import (
	"strconv"
	"strings"
	"time"

	ogoutils "github.com/Odinman/ogo/utils"
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

			client, cmd := utils.Unwrap(msg)
			if len(msg) >= 4 { //命令应该大于5帧(包含信封以及空帧)
				cycles++

				w.Trace("recv cmd: %s, from client: %q", cmd, client)

				act := strings.ToUpper(cmd[0])
				key := cmd[1]
				switch act {
				case COMMAND_GET, COMMAND_TIMING: //获取key内容
					if r, err := w.localGet(cmd); err != nil {
						w.Debug("error: %s", err)
						if err == ErrNil {
							node.SendMessage(client, "", RESPONSE_NIL) //没有内容,返回空
						} else {
							node.SendMessage(client, "", RESPONSE_ERROR, err.Error()) //回复REQ,因此要加上一个空帧
						}
					} else {
						w.Trace("response: %s, len: %d", r, len(r))
						node.SendMessage(client, "", RESPONSE_OK, r) //回复REQ,因此要加上一个空帧
					}
				case COMMAND_SET, COMMAND_DEL, COMMAND_SCHEDULE: //key-value命令
					// 存到本地存储(同步)
					//回复结果(带信封, 否则找不到发送者), 因为是异步的, 可以先回复, 再做事
					if err := w.localStorage(cmd); err != nil {
						w.Debug("error: %s", err)
						node.SendMessage(client, "", RESPONSE_ERROR, err.Error()) //回复REQ,因此要加上一个空帧
					} else {
						node.SendMessage(client, "", RESPONSE_OK) //回复REQ,因此要加上一个空帧
					}

					// 发布(目标是跨IDC多点发布)
					publisher.SendMessage(cmd)

				case COMMAND_PUSH, COMMAND_TASK: //任务队列命令
					value := cmd[2:]
					if err := mqpool.Push(key, value); err == nil {
						w.Debug("push %s successful", key)
						node.SendMessage(client, "", RESPONSE_OK)
					} else {
						w.Debug("push %s failed: %s", key, err)
						node.SendMessage(client, "", RESPONSE_ERROR, err.Error())
					}
				case COMMAND_BTASK: //阻塞任务队列命令
					value := cmd[2:]
					taskId := ogoutils.NewShortUUID()
					value = append([]string{taskId}, value...) //放前面
					if err := mqpool.Push(key, value); err == nil {
						w.Debug("push block task %s successful, task id: %s [%s]", key, taskId, time.Now())
						blockTasks[taskId] = make(chan string, 1)
						bto := time.Tick(BTASK_TIMEOUT)
						//go w.newBlocker(client)
						select {
						case <-bto: //超时
							w.Info("waiting time out")
							node.SendMessage(client, "", RESPONSE_ERROR)
						case result := <-blockTasks[taskId]:
							w.Debug("block task result: %s [%s]", result, time.Now())
							if result == "0" {
								node.SendMessage(client, "", RESPONSE_ERROR)
							} else {
								node.SendMessage(client, "", RESPONSE_OK, result)
							}
						}
						delete(blockTasks, taskId)
					} else {
						w.Debug("push %s failed: %s", key, err)
						node.SendMessage(client, "", RESPONSE_ERROR, err.Error())
					}
				case COMMAND_COMPLETE: // 完成阻塞任务
					if len(cmd) > 3 {
						if taskId := cmd[2]; taskId != "" {
							if _, ok := blockTasks[taskId]; ok {
								blockTasks[taskId] <- cmd[3]
								node.SendMessage(client, "", RESPONSE_OK)
							} else {
								node.SendMessage(client, "", RESPONSE_ERROR)
							}
						} else {
							node.SendMessage(client, "", RESPONSE_ERROR)
						}
					} else {
						node.SendMessage(client, "", RESPONSE_ERROR)
					}
				case COMMAND_POP, COMMAND_BPOP: //pop或者阻塞式pop
					bt := 0 * time.Second
					if len(cmd) > 2 && act == COMMAND_BPOP {
						if bs, _ := strconv.Atoi(cmd[2]); bs > 0 {
							bt = time.Duration(bs) * time.Second
							w.Trace("pop block dura: %s", bt)
						}
					}
					if value, err := mqpool.Pop(key, bt); err == nil {
						w.Debug("pop %s: %s [%s]", key, value, time.Now())
						node.SendMessage(client, "", RESPONSE_OK, value) //回复REQ,因此要加上一个空帧
					} else if err.Error() == RESPONSE_NIL {
						w.Trace("pop %s nil: %s", key, err)
						node.SendMessage(client, "", RESPONSE_NIL) //没有内容,返回空
					} else {
						w.Trace("pop %s failed: %s", key, err)
						node.SendMessage(client, "", RESPONSE_ERROR, err.Error()) //回复REQ,因此要加上一个空帧
					}
				default:
					// unknown action
					w.Info("unkown action: %s", act)
					node.SendMessage(client, "", RESPONSE_UNKNOWN)
				}

				// 回满血, 结束
				liveness = HEARTBEAT_LIVENESS
			} else if len(msg) == 1 {
				//  When we get a heartbeat message from the queue, it means the
				//  queue was (recently) alive, so reset our liveness indicator:
				if msg[0] == PPP_HEARTBEAT {
					liveness = HEARTBEAT_LIVENESS
				} else {
					w.Info("invalid message: %q", msg)
					node.SendMessage(client, "", RESPONSE_UNKNOWN)
				}
			} else {
				w.Info("invalid message: %q", msg)
				node.SendMessage(client, "", RESPONSE_UNKNOWN)
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
				w.Trace("node%d worked cycles: %d", i, cycles)
				lastCycles = cycles
			}
			node.Send(PPP_HEARTBEAT, 0)
		default:
		}
	}
}

/* }}} */

/* {{{ func (w *OmqWorker) newBlocker(client string)
 * 异步阻塞节点
 */
func (w *OmqWorker) newBlocker(client string) {
	//node, _ := w.connectQueue()
	//w.Trace("block node for client: %q", client)

	//  Send out heartbeats at regular intervals
	heartbeat_at := time.Tick(5 * time.Second)

	//  Send heartbeat to queue if it's time
	select {
	case <-heartbeat_at:
		w.Debug("recv taskman response")
		//node.SendMessage(client, "", "BTASK")
	}
}

/* }}} */
