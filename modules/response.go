package modules

import (
	"time"

	"github.com/Odinman/omq/utils"
)

/* {{{ func (o *OMQ) response(j *utils.Job)
 * 回复
 */
func (o *OMQ) response(j *utils.Job) {
	r := j.Payload.(*Request)
	rt := []string{RESPONSE_UNKNOWN}
	if cmd := r.Command; len(cmd) >= 2 { //命令应该大于1帧(包含信封以及空帧)

		o.Trace("recv cmd: %s, from client: %q", cmd, r.Client)

		rt = o.execCommand(cmd)
		j.Result = rt
	} else { //命令错误
		o.Info("invalid command: %q", r.Command)
		j.Result = rt
	}
	// 最后一帧加上开始时间, 以统计最终耗时
	r.conn.SendMessage(r.Client, "", rt, r.access.Time.Format(time.RFC3339Nano))

	// save access
	if rt[0] == RESPONSE_NIL && (r.act == COMMAND_POP || r.act == COMMAND_BPOP) {
		// BPOP&POP操作没有返回时, 不记录
	} else {
		r.access.Save()
	}
}

/* }}} */
