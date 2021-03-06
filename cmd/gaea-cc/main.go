// Copyright 2019 The Gaea Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"github.com/XiaoMi/Gaea/cc/proxy"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/XiaoMi/Gaea/cc"
	"github.com/XiaoMi/Gaea/core"

	"github.com/XiaoMi/Gaea/models"
)

var ccConfigFile = flag.String("c", "./etc/gaea_cc.ini", "gaea cc配置")
var info = flag.Bool("info", false, "show info of gaea-cc")

func main() {
	flag.Parse()
	if *info {
		fmt.Printf("Build Version Information:%s\n", core.Info.LongForm())
		return
	}

	fmt.Printf("Build Version Information:%s\n", core.Info.LongForm())

	// 初始化配置
	ccConfig, err := models.ParseCCConfig(*ccConfigFile)
	if err != nil {
		fmt.Printf("parse cc source failed, %v\n", err)
	}

	// 构造服务实例
	s, err := cc.NewServer(ccConfig.Addr, ccConfig)
	if err != nil {
		proxy.ControllerLogger.Fatalf("create server failed, %v", err)
		return
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGPIPE)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			sig := <-c
			if sig == syscall.SIGINT || sig == syscall.SIGTERM || sig == syscall.SIGQUIT {
				proxy.ControllerLogger.Infof("got signal %d, quit", sig)
				s.Close()
				return
			}
			proxy.ControllerLogger.Infof("ignore signal %d", sig)
		}
	}()

	s.Run()
	wg.Wait()
}
