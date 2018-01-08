package main

import (
	"crystal"
	"time"
	"context"
)

func onClientMessage(msg *crystal.Message, conn *crystal.TcpConn){
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

	crystal.SetMessageFunc(onClientMessage)
	crystal.SetConnectFunc(func (conn *crystal.TcpConn) {
		crystal.Infof("link %d connected!", conn.NetID())
	})
	crystal.SetCloseFunc(func (conn *crystal.TcpConn) {
		crystal.Infof("link %d close!", conn.NetID())
	})
	crystal.Errorln("try listen")
	if ok, err := ctrl.Listen("127.0.0.1:20013"); !ok {
		crystal.Errorln("listen failed", err)
		return
	}
	ctrl.Accept()
}