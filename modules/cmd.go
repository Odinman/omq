package modules

import (
	"strconv"
	"strings"
	"time"
)

/* {{{ func (o *OMQ) execCommand(cmd []string, opts ...string) (result []string)
 *
 */
func (o *OMQ) execCommand(cmd []string, opts ...string) (result []string) {
	result = make([]string, 0)
	act := strings.ToUpper(cmd[0])
	key := cmd[1]
	var session string
	if len(opts) > 0 {
		for i, opt := range opts {
			switch i {
			case 0:
				session = opt
			default:
				break
			}
		}
	}
	switch act {
	case COMMAND_GET, COMMAND_TIMING: //获取key内容
		if r, err := o.localGet(cmd); err != nil {
			o.Debug("error: %s", err)
			if err == ErrNil {
				result = append(result, RESPONSE_NIL)
			} else {
				result = append(result, RESPONSE_ERROR, err.Error())
			}
		} else {
			o.Trace("response: %s, len: %d", r, len(r))
			result = append(result, RESPONSE_OK)
			result = append(result, r...)
		}
	case COMMAND_SET, COMMAND_DEL, COMMAND_SCHEDULE: //key-value命令
		// 存到本地存储(同步)
		//回复结果(带信封, 否则找不到发送者), 因为是异步的, 可以先回复, 再做事
		if err := o.localStorage(cmd); err != nil {
			o.Debug("error: %s", err)
			result = append(result, RESPONSE_ERROR, err.Error())
		} else {
			result = append(result, RESPONSE_OK)
		}

		// 发布(目标是跨IDC多点发布)
		o.pub.SendMessage(cmd)

	case COMMAND_PUSH, COMMAND_TASK: //任务队列命令
		value := cmd[2:]
		if err := o.mqPool.Push(key, value); err == nil {
			o.Debug("push %s successful", key)
			result = append(result, RESPONSE_OK)
		} else {
			o.Debug("push %s failed: %s", key, err)
			result = append(result, RESPONSE_ERROR, err.Error())
		}
	case COMMAND_BTASK: //阻塞任务队列命令
		value := cmd[2:]
		//taskId := ogoutils.NewShortUUID()
		taskId := session
		value = append([]string{taskId}, value...) //放前面
		if err := o.mqPool.Push(key, value); err == nil {
			o.Debug("push block task %s successful, task id: %s [%s]", key, taskId, time.Now())
			o.blockTasks[taskId] = make(chan string, 1)
			bto := time.Tick(BTASK_TIMEOUT)
			select {
			case <-bto: //超时
				o.Info("waiting time out")
				result = append(result, RESPONSE_ERROR, "time out")
			case r := <-o.blockTasks[taskId]:
				o.Debug("block task result: %s [%s]", r, time.Now())
				if r == "0" {
					result = append(result, RESPONSE_ERROR, "exec failed")
				} else {
					result = append(result, RESPONSE_OK, r)
				}
			}
			delete(o.blockTasks, taskId)
		} else {
			o.Debug("push %s failed: %s", key, err)
			result = append(result, RESPONSE_ERROR, err.Error())
		}
	case COMMAND_COMPLETE: // 完成阻塞任务
		if len(cmd) > 3 {
			if taskId := cmd[2]; taskId != "" {
				if _, ok := o.blockTasks[taskId]; ok {
					o.blockTasks[taskId] <- cmd[3]
					result = append(result, RESPONSE_OK)
				} else {
					result = append(result, RESPONSE_ERROR, "not found task")
				}
			} else {
				result = append(result, RESPONSE_ERROR, "task id empty")
			}
		} else {
			result = append(result, RESPONSE_ERROR, "invalid cmd")
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
			result = append(result, RESPONSE_OK)
			result = append(result, value...)
		} else if err.Error() == RESPONSE_NIL {
			o.Trace("pop %s nil: %s", key, err)
			result = append(result, RESPONSE_NIL)
		} else {
			o.Trace("pop %s failed: %s", key, err)
			result = append(result, RESPONSE_ERROR, err.Error())
		}
	default:
		// unknown action
		o.Info("unkown action: %s", act)
		result = append(result, RESPONSE_UNKNOWN)
	}
	return result
}

/* }}} */
