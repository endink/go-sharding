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
	"github.com/XiaoMi/Gaea/logging"
	"github.com/XiaoMi/Gaea/util"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/XiaoMi/Gaea/core"
	"github.com/XiaoMi/Gaea/models"
	"github.com/XiaoMi/Gaea/proxy/server"
)

func main() {
	var defaultConfigFilePath = "etc/gaea.ini"
	if util.IsWindows() {
		defaultConfigFilePath = "gaea.ini"
	}

	var configFile = flag.String("config", defaultConfigFilePath, "gaea config file")
	var info = flag.Bool("info", false, "show info of gaea")
	flag.Parse()

	if *info {
		fmt.Printf("Build Version Information:%s\n", core.Info.LongForm())
		return
	}

	fmt.Printf("Build Version Information:%s\n", core.Info.LongForm())
	var cfg *models.Proxy
	if !util.FileExists(*configFile) {
		cfg = models.DefaultProxy()
	} else {
		// init config of gaea proxy
		c, err := models.ParseProxyConfigFromFile(*configFile)
		if err != nil {
			fmt.Printf("parse config file error:%v\n", err.Error())
			return
		}
		cfg = c
	}

	// init manager
	mgr, err := server.LoadAndCreateManager(cfg)
	if err != nil {
		logging.DefaultLogger.Fatalf("init manager failed, error: %v", err)
		return
	}

	svr, err := server.NewServer(cfg, mgr)
	if err != nil {
		logging.DefaultLogger.Fatalf("NewServer error, quit. error: %s", err.Error())
		return
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		syscall.SIGPIPE,
		//syscall.SIGUSR1,
	)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			sig := <-sc
			if sig == syscall.SIGINT || sig == syscall.SIGTERM || sig == syscall.SIGQUIT {
				logging.DefaultLogger.Infof("Got signal %d, quit", sig)
				_ = svr.Close()
				break
			} else if sig == syscall.SIGPIPE {
				logging.DefaultLogger.Infof("Ignore broken pipe signal")
			}
			//} else if sig == syscall.SIGUSR1 {
			//	log.Infof("Got update config signal")
			//}
		}
	}()
	_ = svr.Run()
	wg.Wait()
}
