package workers

import (
	"runtime"
	"sync"
	"syscall"
	"testing"
)

func report_pid(t *testing.T, barrier1, barrier2 chan int) {
	// this worker logs it's pid. There are two barriers to synchronize 
	// the different goroutines. The net effect of the barriers is that all goroutines
	// would all be live at the same time, avoiding some goroutine leave and have 
	// another one use the same thread slot
	pid := syscall.Gettid()
	t.Log("Worker in thread: ", pid)
	_ = <-barrier2
	t.Log("ready to leave: ", pid)
	barrier1 <- pid
	t.Log("leaving: ", pid)
}

func distribute(t *testing.T, num_threads int) {
	// Distribute num_threads functions in num_threads different threads.
	// Some barriers are necessary to enforce
	// no worker exits before another one has not started
	barrier1 := make(chan int, 1)
	barrier1 <- 1                 //successive future sends to barrier1 will block
	barrier2 := make(chan int, 1) // pids channel

	pids := make(map[int]bool, 10*num_threads)
	wg := new(sync.WaitGroup)

	Distribute(num_threads, wg, func() { report_pid(t, barrier1, barrier2) })

	// make exactly num_threads pass barrier2.
	for i := 0; i < num_threads+1; i++ {
		barrier2 <- 1 // allow one goroutine to continue
	}
	// check if there's an extra goroutine waiting on barrier2. It would be an error
	select {
	case barrier2 <- 1:
		t.Errorf("extra goroutine in barrier2")
	default:
		close(barrier2)
	}

	// all goroutines are live and ready to send their pids

	_ = <-barrier1 // remove barrier1 blocker

	// fail with deadlock if there are no enough goroutines in barrier1
	for i := 0; i < num_threads; i++ {
		pid := <-barrier1
		if _, used := pids[pid]; used {
			t.Errorf("repeated thread %d", pid)
		} else {
			pids[pid] = true
			t.Logf("Collected pid %d", pid)
		}
	}
	//check for extra goroutines waiting on barrier2. Shouldn't happen
	select {
	case pid := <-barrier1:
		t.Errorf("extra goroutine in thread %d", pid)
	default:
		wg.Wait() // safe to wait on goroutines to finish
	}

}

const (
	max_threads = 10
)

// as we are testing, let's force GOMAXPROCS to num_threads
// note that GOMAXPROCS is actually a lower bound on the number of threads golang can create. go can create many more threads if there are c calls or other blocking code that could use a different thread. This is why we'll only test UnderDistribute and FullDistribute. OverDistribute would just use more threads

func TestUnderDistribute(t *testing.T) {
	runtime.GOMAXPROCS(max_threads)
	distribute(t, 3) // less goroutines than threads
}

func TestFullDistribute(t *testing.T) {
	runtime.GOMAXPROCS(max_threads)
	distribute(t, max_threads) // equal goroutines than threads
}
