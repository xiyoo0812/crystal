package main

import (
	"crystal"
	"time"
	"context"
)

func onServerMessage(msg *crystal.Message, conn *crystal.TcpConn){
	crystal.Infof("link %d recv a msg = %d!", conn.NetID(), msg.MsgId)
}

func main(){
	var ctx = context.Background()
	var tw = crystal.NewTimerWheel(ctx, 100)
	var log = crystal.InitLogger(crystal.DEBUG, time.Hour, "./log/", true)

	var tpool = crystal.NewWorkerPool(32)
	var ctrl = crystal.NewTCPCtrl(ctx, 1, 0)

	defer func(){
		tw.Stop()
		log.Stop()
		tpool.Close()
		ctrl.Stop()
	}()

	var connid int64
	crystal.SetMessageFunc(onServerMessage)
	crystal.SetConnectFunc(func (conn *crystal.TcpConn) {
		crystal.Infof("link %d connected!", conn.NetID())
		connid = conn.NetID()
	})
	crystal.SetCloseFunc(func (conn *crystal.TcpConn) {
		crystal.Infof("link %d close!", conn.NetID())
	})
	crystal.Errorf("try connect", "ddd")
	if ok, err := ctrl.Connect("127.0.0.1:20013"); !ok {
		crystal.Infoln("connect failed", err)
		return
	}

	timeCh := make(chan int64)
	tid := tw.RegTimer(500, 5000, timeCh)
	crystal.Infoln("reg timer:", tid)
	for  {
		select {
		case id := <-timeCh:
			if id == tid {
				crystal.Infof("timeid:%d", tid)

				var msg crystal.Message
				msg.General(10000, 0, nil)
				msg.Write(uint32(25))
				msg.Write(uint32(25))
				if conn, ok := ctrl.Conn(connid); ok{
					conn.Write(&msg)
				}
			}
		}
	}
}
