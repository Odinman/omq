// Package modules provides ...
// reference: http://marcio.io/2015/07/handling-1-million-requests-per-minute-with-golang/
package modules

import (
	"fmt"
)

type Job struct {
	Request []string
}

var JobQueue chan Job

type JobWorker struct {
	WorkerPool chan chan Job
	JobChannel chan Job
	quit       chan bool
}

/* {{{ func NewJobWorker(wp chan chan Job) *JobWorker
 *
 */
func NewJobWorker(wp chan chan Job) *JobWorker {
	return &JobWorker{
		WorkerPool: wp,
		JobChannel: make(chan Job),
		quit:       make(chan bool),
	}
}

/* }}} */

/* {{{ func (jw *JobWorker) Start()
 *
 */
func (jw *JobWorker) Start() {
	go func() {
		for {
			// register the current worker into the worker queue.
			jw.WorkerPool <- jw.JobChannel

			select {
			case job := <-jw.JobChannel:
				// we have received a work request.
				fmt.Println(job)

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

type Dispatcher struct {
	// A pool of workers channels that are registered with the dispatcher
	pool       chan chan Job
	maxWorkers int
}

/* {{{ func NewDispatcher(maxWorkers int) *Dispatcher
 *
 */
func NewDispatcher(maxWorkers int) *Dispatcher {
	pool := make(chan chan Job, maxWorkers)
	return &Dispatcher{
		pool:       pool,
		maxWorkers: maxWorkers,
	}
}

/* }}} */

/* {{{ func NewDispatcher(maxWorkers int) *Dispatcher
 *
 */
func (d *Dispatcher) Run() {
	// starting n number of workers
	for i := 0; i < d.maxWorkers; i++ {
		worker := NewJobWorker(d.pool)
		worker.Start()
	}

	go d.dispatch()
}

/* }}} */

/* {{{ func (d *Dispatcher) dispatch()
 *
 */
func (d *Dispatcher) dispatch() {
	for {
		select {
		case job := <-JobQueue:
			// a job request has been received
			go func(job Job) {
				// try to obtain a worker job channel that is available.
				// this will block until a worker is idle
				jobChannel := <-d.pool

				// dispatch the job to the worker job channel
				jobChannel <- job
			}(job)
		}
	}
}

/* }}} */
