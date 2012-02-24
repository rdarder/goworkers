// The workers package allows launching a function as many goroutines and keeping those in separate threads.
// Given a CPU intensive worker like function it would be good to have many workers concurrently processing without having each other competing for the same resources. Basically, having one worker goroutine per CPU and each goroutine in a separate thread would do it.
package workers

import (
	"log"
	"runtime"
	"sync"
	"syscall"
)

const (
	find_retries = 1000
)

type pidRegistry struct {
	pids map[int]bool
	lock sync.Mutex
}

func find_unused_thread(tries int, wg *sync.WaitGroup, reg *pidRegistry, worker func()) {
	for i := 0; i <= tries; i++ {
		reg.lock.Lock()
		runtime.LockOSThread()
		pid := syscall.Gettid()
		if _, taken := reg.pids[pid]; !taken {
			reg.pids[pid] = true
			reg.lock.Unlock()
			runtime.Gosched() //block to aid context switching
			worker()
			reg.lock.Lock()
			delete(reg.pids, pid)
			reg.lock.Unlock()
			wg.Done()
			return
		} else {
			reg.lock.Unlock()
			runtime.UnlockOSThread()
		}
	}
	log.Println("goroutine given up finding a new thread")
	wg.Done()
}

//Distribute worker into `num_threads` goroutines in separate threads. All new goroutines will belong to a the waitgroup
func Distribute(num_threads int, wg *sync.WaitGroup, worker func()) {
	reg := &pidRegistry{pids: make(map[int]bool, num_threads)}

	for i := 0; i < num_threads; i++ {
		wg.Add(1)
		go find_unused_thread(find_retries*num_threads, wg, reg, worker)
	}
}

//Fill will spawn as many worker goroutines as GOMAXPROCS and keep them in different threads
func Fill(worker func()) *sync.WaitGroup {
	wg := new(sync.WaitGroup)
	procs := runtime.GOMAXPROCS(0)
	log.Printf("spawning %d goroutines\n", procs)
	Distribute(procs, wg, worker)
	return wg
}
