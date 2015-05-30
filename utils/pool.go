// Package utils provides zmq connection pool
package utils

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	zmq "github.com/pebbe/zmq4"
)

type Pool struct {
	// Stack of idleConn with most recently used at the front.
	pool list.List

	// mu protects fields defined below.
	mu     sync.Mutex
	cond   *sync.Cond
	closed bool

	// logger
	logger PoolLogger
	prefix string

	// 获取新连接的方法
	New func() (*zmq.Socket, error)

	Max int

	Wait bool //当连接池满的时候是否等待

	Life time.Duration
}

type PoolLogger interface {
	Printf(format string, v ...interface{})
}

type PooledSocket struct {
	Soc    *zmq.Socket
	expire time.Time
	undone bool //未完成
	inUse  bool
	pool   *Pool
	ele    *list.Element //在list中位置
}

/* {{{ func NewPool(newFn func() (*zmq.Socket, error), ext ...interface{}) *Pool
 * NewPool creates a new pool. This function is deprecated. Applications should
 * initialize the Pool fields directly as shown in example.
 */
//func NewPool(newFn func() (*zmq.Socket, error), max int, life time.Duration) *Pool {
func NewPool(newFn func() (*zmq.Socket, error), ext ...interface{}) *Pool {
	var Max int = 100
	var Life time.Duration = 60 * time.Second
	var Logger PoolLogger
	var Prefix string

	if len(ext) > 0 {
		if max, ok := ext[0].(int); ok {
			Max = max
		}
	}
	if len(ext) > 1 {
		if life, ok := ext[1].(time.Duration); ok {
			Life = life
		}
	}
	if len(ext) > 2 {
		if logger, ok := ext[2].(PoolLogger); ok {
			Logger = logger
			Prefix = "[OgoPool]"
		}
	}
	return &Pool{New: newFn, Max: Max, Life: Life, logger: Logger, prefix: Prefix}
}

/* }}} */

/* {{{ func (p *Pool) Debug(format string, v ...interface{})
 *
 */
func (p *Pool) Debug(format string, v ...interface{}) {
	if p.logger == nil {
		return
	}
	p.logger.Printf(p.prefix+" "+format, v...)
}

/* }}} */

/* {{{ func (p *Pool) Close()
 *
 */
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for e := p.pool.Front(); e != nil; e = e.Next() {
		e.Value.(*PooledSocket).Soc.Close()
	}
	p.pool.Init() // clear
	p.closed = true
	if p.cond != nil { //唤醒所有等待者
		p.cond.Broadcast()
	}
}

/* }}} */

/* {{{ func (p *Pool) Get() *Socket
 *
 */
func (p *Pool) Get() (*PooledSocket, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, fmt.Errorf("pool closed")
	}

	for { //不get到誓不罢休
		// Get pooled item.
		//if e := p.pool.Front(); e != nil { //如果存在未使用的item,第一个肯定是
		//	ps := e.Value.(*PooledSocket)
		//	if !ps.inUse {
		//		ps.inUse = true
		//		p.pool.MoveToBack(e) //移到最后
		//		return ps, nil
		//	}
		//}
		depth := 0
		for e := p.pool.Front(); e != nil; e = e.Next() {
			depth++
			ps := e.Value.(*PooledSocket)
			if !ps.inUse {
				ps.inUse = true
				p.pool.MoveToBack(e) //移到最后
				p.Debug("find depth: %d, pool len: %d", depth, p.pool.Len())
				return ps, nil
			}
		}

		if p.Max <= 0 || p.pool.Len() < p.Max { //池子无限制或者还没满
			// create new.
			if soc, err := p.New(); err != nil { //无法新建
				return nil, err
			} else {
				ps := &PooledSocket{
					Soc:    soc,
					expire: time.Now().Add(p.Life),
					inUse:  true,
					pool:   p,
				}
				ps.ele = p.pool.PushBack(ps)
				return ps, nil
			}
		}

		if !p.Wait { //不等就结束
			return nil, fmt.Errorf("Pool full at %d", p.Max)
		}

		//等待被唤醒
		if p.cond == nil {
			p.cond = sync.NewCond(&p.mu)
		}
		p.cond.Wait()
	}
}

/* }}} */

/* {{{ func (ps *PooledSocket) Close()
 *
 */
func (ps *PooledSocket) Close() {
	p := ps.pool
	p.mu.Lock()
	defer p.mu.Unlock()
	if ps.undone == true {
		//指示需要销毁
		ps.Soc.Close()
		p.pool.Remove(ps.ele)
	} else {
		ps.inUse = false
		ps.expire = time.Now().Add(p.Life) //过期时间延长
		p.pool.MoveToFront(ps.ele)         //放到最前
	}
	if p.cond != nil { //唤醒一个, 如果有的话
		p.cond.Signal()
	}
}

/* }}} */

/* {{{ func (ps *PooledSocket) Do(timeout time.Duration, msg ...interface{}) (reply []string, err error)
 *
 */
func (ps *PooledSocket) Do(timeout time.Duration, msg ...interface{}) (reply []string, err error) {
	p := ps.pool
	p.mu.Lock()
	defer p.mu.Unlock()

	soc := ps.Soc
	poller := zmq.NewPoller()
	poller.Add(soc, zmq.POLLIN)

	// send
	if _, err := soc.SendMessage(msg...); err != nil {
		ps.undone = true
		return nil, err
	}

	// recv
	if sockets, err := poller.Poll(timeout); err != nil {
		ps.undone = true
		return nil, err
	} else if len(sockets) == 1 {
		return soc.RecvMessage(zmq.DONTWAIT)
	} else {
		ps.undone = true
		return nil, fmt.Errorf("time out!")
	}

	return
}

/* }}} */
