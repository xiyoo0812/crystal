// Worker pool is a pool of go-routines running for executing callbacks,
// each client's message handler is permanently hashed into one specified
// worker to execute, so it is in-order for each client's perspective.
package crystal

// WorkerPool is a pool of go-routines running functions.
type WorkerPool struct {
	workers   []*worker
	closeChan chan struct{}
}

var globalWorkerPool *WorkerPool
// WorkerPoolInstance returns the global pool.
func WorkerPoolInstance() *WorkerPool {
	if globalWorkerPool == nil {
		globalWorkerPool = NewWorkerPool(defaultWorkersNum)
	}
	return globalWorkerPool
}

func NewWorkerPool(vol uint32) *WorkerPool {
	if globalWorkerPool == nil {
		if vol <= 0 {
			vol = defaultWorkersNum
		}
		pool := &WorkerPool{
			workers:   make([]*worker, vol),
			closeChan: make(chan struct{}),
		}
		for i := range pool.workers {
			pool.workers[i] = newWorker(i, 1024, pool.closeChan)
			if pool.workers[i] == nil {
				panic("worker nil")
			}
		}
		globalWorkerPool = pool
	}
	return globalWorkerPool
}

// Put appends a function to some worker's channel.
func (wp *WorkerPool) Put(k interface{}, cb workerFunc) error {
	code := HashCode(k)
	return wp.workers[code & uint32(len(wp.workers)-1)].put(cb)
}

// Close closes the pool, stopping it from executing functions.
func (wp *WorkerPool) Close() {
	close(wp.closeChan)
}

// Size returns the size of pool.
func (wp *WorkerPool) Size() int {
	return len(wp.workers)
}

type worker struct {
	index      	int
	cbChan 		chan workerFunc
	closeChan  	chan struct{}
}

func (w *worker) start() {
	for {
		select {
		case <-w.closeChan:
			return
		case cb := <-w.cbChan:
			cb()
		}
	}
}

func (w *worker) put(cb workerFunc) error {
	select {
	case w.cbChan <- cb:
		return nil
	default:
		return ErrWouldBlock
	}
}

func newWorker(i int, c int, closeChan chan struct{}) *worker {
	w := &worker{index : i, cbChan : make(chan workerFunc, c), closeChan : closeChan}
	go w.start()
	return w
}
