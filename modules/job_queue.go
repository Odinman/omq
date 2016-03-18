// Package modules provides ...
// reference: http://marcio.io/2015/07/handling-1-million-requests-per-minute-with-golang/
package modules

import (
	"github.com/Odinman/ogo"
)

type Job struct {
	Payload interface{} `json:"payload,omitempty"`
	Result  interface{} `json:"result,omitempty"`
	conn    *ZSocket
	access  *ogo.Access
}

/* {{{ func NewJob(r []string, c *ZSocket) *Job
 *
 */
func NewJob(r []string, c *ZSocket) *Job {
	return &Job{
		Payload: NewRequest(r),
		conn:    c,
		access:  ogo.NewAccess(),
	}
}

/* }}} */

/* {{{ func (j *Job) SaveAccess(payload interface{})
 *
 */
func (j *Job) SaveAccess() {
	if r, ok := j.Result.([]string); ok && r[0] == RESPONSE_NIL && (j.Payload.(*Request).act == COMMAND_POP || j.Payload.(*Request).act == COMMAND_BPOP) {
		// BPOP&POP操作没有返回时, 不记录
	} else {
		j.access.App = j
		j.access.Save()
	}
}

/* }}} */

type JobFunc func(j *Job)

type JobWorker struct {
	pool       chan chan *Job
	jobChannel chan *Job
	quit       chan bool
	Handler    JobFunc
}

/* {{{ func newJobWorker(wp chan chan Job, jf JobFunc) *JobWorker
 *
 */
func newJobWorker(wp chan chan *Job, jf JobFunc) *JobWorker {
	return &JobWorker{
		pool:       wp,
		jobChannel: make(chan *Job),
		quit:       make(chan bool),
		Handler:    jf,
	}
}

/* }}} */

/* {{{ func (jw *JobWorker) Start(sn int)
 *
 */
func (jw *JobWorker) Start(sn int) {
	go func() {
		for {
			// register the current worker into the worker queue.
			jw.pool <- jw.jobChannel

			select {
			case job := <-jw.jobChannel:
				// we have received a work request.
				//client, cmd := utils.Unwrap(job.Request)
				////ogo.Debug("[worker %d] [client: %q] [cmd: %s]", sn, client, cmd)
				//if c, e := job.conn.SendMessage(client, "", RESPONSE_OK); e != nil {
				//	ogo.Debug("send failed: %s", e)
				//} else {
				//	ogo.Debug("send success: %d", c)
				//}
				jw.Handler(job)

			case <-jw.quit:
				// we have received a signal to stop
				return
			}
		}
	}()
}

/* }}} */

/* {{{ func (jw *JobWorker) Stop()
 *
 */
func (jw *JobWorker) Stop() {
	go func() {
		jw.quit <- true
	}()
}

/* }}} */

type WorkerPool struct {
	// A pool of workers channels that are registered with the dispatcher
	queue   chan *Job
	pool    chan chan *Job
	max     int
	handler JobFunc
}

/* {{{ func NewWorkerPool(maxWorkers int, jf JobFunc) *WorkerPool
 *
 */
func NewWorkerPool(maxWorkers int, jf JobFunc) *WorkerPool {
	pool := make(chan chan *Job, maxWorkers)
	queue := make(chan *Job)
	return &WorkerPool{
		queue:   queue,
		pool:    pool,
		max:     maxWorkers,
		handler: jf,
	}
}

/* }}} */

/* {{{ func (wp *WorkerPool) Run()
 *
 */
func (wp *WorkerPool) Run() {
	// starting n number of workers
	for i := 0; i < wp.max; i++ {
		worker := newJobWorker(wp.pool, wp.handler)
		worker.Start(i)
	}

	go wp.dispatch()
}

/* }}} */

/* {{{ func (wp *WorkerPool) dispatch()
 *
 */
func (wp *WorkerPool) dispatch() {
	for {
		select {
		case job := <-wp.queue:
			//ogo.Debug("[dispatch] recv job: %s", job)
			// a job request has been received
			go func(job *Job) {
				// try to obtain a worker job channel that is available.
				// this will block until a worker is idle
				jobChannel := <-wp.pool

				// dispatch the job to the worker job channel
				jobChannel <- job
			}(job)
		}
	}
}

/* }}} */
