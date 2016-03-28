// Package utils provides ...
// job queue
// reference: http://marcio.io/2015/07/handling-1-million-requests-per-minute-with-golang/
package utils

type Job struct {
	Payload interface{} `json:"payload,omitempty"`
	Result  interface{} `json:"result,omitempty"`
}

type JobFunc func(j *Job)

type JobWorker struct {
	pool       chan chan *Job
	jobChannel chan *Job
	quit       chan bool
	handler    JobFunc
}

type WorkerPool struct {
	// A pool of workers channels that are registered with the dispatcher
	queue   chan *Job
	pool    chan chan *Job
	max     int
	handler JobFunc
}

/* {{{ func NewJob(pl interface{}) *Job
 *
 */
func NewJob(pl interface{}) *Job {
	return &Job{
		Payload: pl,
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
				// we have received a job
				jw.handler(job)
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
		worker := &JobWorker{
			pool:       wp.pool,
			jobChannel: make(chan *Job),
			quit:       make(chan bool),
			handler:    wp.handler,
		}
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

/* {{{ func (wp *WorkerPool) Push(j *Job)
 *
 */
func (wp *WorkerPool) Push(j *Job) {
	wp.queue <- j
}

/* }}} */
