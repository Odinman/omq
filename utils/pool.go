// Package utils provides zmq connection pool
// reference github.com/garyburd/redigo/redis
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

	// 获取连接的方法
	New func() (*zmq.Socket, error)

	Max int

	Wait bool //当连接池满的时候是否等待

	Life time.Duration
}

type PooledSocket struct {
	Soc    *zmq.Socket
	expire time.Time
	inUse  bool
	pool   *Pool
	ele    *list.Element //在list中位置
}

/* {{{ func NewPool(newFn func() (Conn, error), maxIdle int) *Pool
 * NewPool creates a new pool. This function is deprecated. Applications should
 * initialize the Pool fields directly as shown in example.
 */
func NewPool(newFn func() (*zmq.Socket, error), max int, life time.Duration) *Pool {
	return &Pool{New: newFn, Max: max, Life: life}
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
		for i, n := 0, p.pool.Len(); i < n; i++ { //最差情况是遍历全部还没找到
			if e := p.pool.Front(); e != nil { //LRU,队列最前面的是最少使用的
				ps := e.Value.(*PooledSocket)
				if !ps.inUse {
					ps.inUse = true
					//ps.expire = time.Now().Add(p.Life) //取的时候不延长, 归还的时候延长
					p.pool.MoveToBack(e) //移到最后
					return ps, nil
				}
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
			p.mu.Unlock()
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

/* {{{ func (ps *PooledSocket) Close() {
 *
 */
func (ps *PooledSocket) Close(destory ...bool) {
	//归还, 保留销毁的选项,由客户端决定
	p := ps.pool
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(destory) > 0 && destory[0] == true {
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
