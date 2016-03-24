package modules

import ()

/* {{{ func (o *OMQ) response(j *Job)
 * 回复
 */
func (o *OMQ) response(j *Job) {
	conn := j.conn
	client := j.Payload.(*Request).Client
	if cmd := j.Payload.(*Request).Command; len(cmd) >= 2 { //命令应该大于5帧(包含信封以及空帧)

		o.Trace("recv cmd: %s, from client: %q", cmd, client)

		result := o.execCommand(cmd)
		conn.SendMessage(client, "", result)
		j.Result = result
	} else { //命令错误
		o.Info("invalid command: %q", j.Payload.(*Request).Command)
		conn.SendMessage(client, "", RESPONSE_UNKNOWN)
		j.Result = RESPONSE_UNKNOWN
	}

	// access log
	j.SaveAccess()
}

/* }}} */
