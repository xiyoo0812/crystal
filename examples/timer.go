package main

import (
	"time"
	"crystal"
	"context"
)

func main(){
	var ctx = context.Background()
	var tw = crystal.NewTimerWheel(ctx, 100)
	var log = crystal.InitLogger(crystal.DEBUG, time.Hour, "./log/", true)

	defer func(){
		tw.Stop()
		log.Stop()
	}()

	timeCh := make(chan int64)
	tid := tw.RegTimer(500, 5000, timeCh)
	crystal.Infoln("reg timer:", tid)
	for  {
		select {
		case id := <-timeCh:
			if id == tid {
				crystal.Infof("timeid:%d", tid)
			}
		}
	}
}
