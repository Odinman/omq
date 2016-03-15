package modules

import (
	"strconv"
	"strings"
	"time"

	ogoutils "github.com/Odinman/ogo/utils"
	"github.com/Odinman/omq/utils"
)

/* {{{ func (o *OMQ) response(j Job)
 * 回复节点
 */
func (o *OMQ) response(j Job) {
	msg := j.Request
	conn := j.Conn

	//o.Debug("[response] msg: %s", msg)
	client, cmd := utils.Unwrap(msg)
	if len(msg) >= 4 { //命令应该大于5帧(包含信封以及空帧)

		o.Trace("recv cmd: %s, from client: %q", cmd, client)

		act := strings.ToUpper(cmd[0])
		key := cmd[1]
		switch act {
		case COMMAND_GET, COMMAND_TIMING: //获取key内容
			if r, err := o.localGet(cmd); err != nil {
				o.Debug("error: %s", err)
				if err == ErrNil {
					conn.SendMessage(client, "", RESPONSE_NIL) //没有内容,返回空
				} else {
					conn.SendMessage(client, "", RESPONSE_ERROR, err.Error()) //回复REQ,因此要加上一个空帧
				}
			} else {
				o.Trace("response: %s, len: %d", r, len(r))
				conn.SendMessage(client, "", RESPONSE_OK, r) //回复REQ,因此要加上一个空帧
			}
		case COMMAND_SET, COMMAND_DEL, COMMAND_SCHEDULE: //key-value命令
			// 存到本地存储(同步)
			//回复结果(带信封, 否则找不到发送者), 因为是异步的, 可以先回复, 再做事
			if err := o.localStorage(cmd); err != nil {
				o.Debug("error: %s", err)
				conn.SendMessage(client, "", RESPONSE_ERROR, err.Error()) //回复REQ,因此要加上一个空帧
			} else {
				conn.SendMessage(client, "", RESPONSE_OK) //回复REQ,因此要加上一个空帧
			}

			// 发布(目标是跨IDC多点发布)
			o.pub.SendMessage(cmd)

		case COMMAND_PUSH, COMMAND_TASK: //任务队列命令
			value := cmd[2:]
			if err := o.mqPool.Push(key, value); err == nil {
				o.Debug("push %s successful", key)
				conn.SendMessage(client, "", RESPONSE_OK)
			} else {
				o.Debug("push %s failed: %s", key, err)
				conn.SendMessage(client, "", RESPONSE_ERROR, err.Error())
			}
		case COMMAND_BTASK: //阻塞任务队列命令
			value := cmd[2:]
			taskId := ogoutils.NewShortUUID()
			value = append([]string{taskId}, value...) //放前面
			if err := o.mqPool.Push(key, value); err == nil {
				o.Debug("push block task %s successful, task id: %s [%s]", key, taskId, time.Now())
				o.blockTasks[taskId] = make(chan string, 1)
				bto := time.Tick(BTASK_TIMEOUT)
				select {
				case <-bto: //超时
					o.Info("waiting time out")
					conn.SendMessage(client, "", RESPONSE_ERROR)
				case result := <-o.blockTasks[taskId]:
					o.Debug("block task result: %s [%s]", result, time.Now())
					if result == "0" {
						conn.SendMessage(client, "", RESPONSE_ERROR)
					} else {
						conn.SendMessage(client, "", RESPONSE_OK, result)
					}
				}
				delete(o.blockTasks, taskId)
			} else {
				o.Debug("push %s failed: %s", key, err)
				conn.SendMessage(client, "", RESPONSE_ERROR, err.Error())
			}
		case COMMAND_COMPLETE: // 完成阻塞任务
			if len(cmd) > 3 {
				if taskId := cmd[2]; taskId != "" {
					if _, ok := o.blockTasks[taskId]; ok {
						o.blockTasks[taskId] <- cmd[3]
						conn.SendMessage(client, "", RESPONSE_OK)
					} else {
						conn.SendMessage(client, "", RESPONSE_ERROR)
					}
				} else {
					conn.SendMessage(client, "", RESPONSE_ERROR)
				}
			} else {
				conn.SendMessage(client, "", RESPONSE_ERROR)
			}
		case COMMAND_POP, COMMAND_BPOP: //pop或者阻塞式pop
			bt := 0 * time.Second
			if len(cmd) > 2 && act == COMMAND_BPOP {
				if bs, _ := strconv.Atoi(cmd[2]); bs > 0 {
					bt = time.Duration(bs) * time.Second
					o.Trace("pop block dura: %s", bt)
				}
			}
			if value, err := o.mqPool.Pop(key, bt); err == nil {
				o.Debug("pop %s: %s [%s]", key, value, time.Now())
				conn.SendMessage(client, "", RESPONSE_OK, value) //回复REQ,因此要加上一个空帧
			} else if err.Error() == RESPONSE_NIL {
				o.Trace("pop %s nil: %s", key, err)
				conn.SendMessage(client, "", RESPONSE_NIL) //没有内容,返回空
			} else {
				o.Trace("pop %s failed: %s", key, err)
				conn.SendMessage(client, "", RESPONSE_ERROR, err.Error()) //回复REQ,因此要加上一个空帧
			}
		default:
			// unknown action
			o.Info("unkown action: %s", act)
			conn.SendMessage(client, "", RESPONSE_UNKNOWN)
		}

	} else {
		o.Info("invalid message: %q", msg)
		conn.SendMessage(client, "", RESPONSE_UNKNOWN)
	}

}

/* }}} */
