package main

import (
	"log"
	"math/rand"
	"os"
	"time"
)

func main() {
	log.Println("payload: started, pid =", os.Getpid())

	// бесконечный цикл
	for {
		// псевдо-нагрузка
		_ = busyWork()
		time.Sleep(200 * time.Millisecond)
	}
}

// имит. cpu-нагрузку
func busyWork() int {
	n := rand.Intn(1000) + 1000
	s := 0
	for i := 0; i < n; i++ {
		s += i * 2
	}
	return s
}
