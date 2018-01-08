package main

import (
	"crystal"
)

func main(){
	var log = tao.InitLogger(crystal.DEBUG, time.Hour, "./log/", true)

	defer func(){
		log.Stop()
	}()


	crystal.Debugf("test %d %s!", 12345, "Debugf")
	crystal.Infof("test %d %s!", 12345, "Infof")
	crystal.Warnf("test %d %s!", 12345, "Warnf")
	crystal.Errorf("test %d %s!", 12345, "Errorf")

	crystal.Debugln("test logger:", "Debugln", 12345)
	crystal.Infoln("test logger:", "Infoln", 12345)
	crystal.Warnln("test logger:", "Warnln", 12345)
	crystal.Errorln("test logger:", "Errorln", 12345)
}