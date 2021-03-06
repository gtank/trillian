// Copyright 2016 Google Inc. All Rights Reserved.
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

// The trillian_log_signer binary runs the log signing code.
package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/glog"
	"github.com/google/trillian/cmd"
	"github.com/google/trillian/crypto/keys"
	"github.com/google/trillian/extension"
	"github.com/google/trillian/monitoring/prometheus"
	"github.com/google/trillian/quota"
	"github.com/google/trillian/server"
	"github.com/google/trillian/storage/yolo"
	"github.com/google/trillian/util"
	"github.com/google/trillian/util/etcd"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/tsuna/gohbase"
	"golang.org/x/net/context"
)

var (
	brokers                  = flag.String("kafka-brokers", os.Getenv("KAFKA_PEERS"), "The Kafka brokers to connect to, as a comma separated list")
	topics                   = flag.String("kafka-topics", os.Getenv("KAFKA_TOPICS"), "The Kafka brokers to connect to, as a comma separated list")
	hbaseQuorum              = flag.String("hbase-quorum", "", "The ZooKeeper quorum to use for contacting HBase")
	hbaseRoot                = flag.String("hbase-root", "", "Where, in ZooKeeper, HBase metadata is stored")
	hbaseTable               = flag.String("hbase-table", "trillian", "The name of the HBase table to use")
	httpEndpoint             = flag.String("http_endpoint", "localhost:8091", "Endpoint for HTTP (host:port, empty means disabled)")
	sequencerIntervalFlag    = flag.Duration("sequencer_interval", time.Second*10, "Time between each sequencing pass through all logs")
	batchSizeFlag            = flag.Int("batch_size", 50, "Max number of leaves to process per batch")
	numSeqFlag               = flag.Int("num_sequencers", 10, "Number of sequencer workers to run in parallel")
	sequencerGuardWindowFlag = flag.Duration("sequencer_guard_window", 0, "If set, the time elapsed before submitted leaves are eligible for sequencing")
	forceMaster              = flag.Bool("force_master", false, "If true, assume master for all logs")
	etcdServers              = flag.String("etcd_servers", "", "A comma-separated list of etcd servers")
	etcdHTTPService          = flag.String("etcd_http_service", "trillian-logsigner-http", "Service name to announce our HTTP endpoint under")
	lockDir                  = flag.String("lock_file_path", "/test/multimaster", "etcd lock file directory path")

	preElectionPause    = flag.Duration("pre_election_pause", 1*time.Second, "Maximum time to wait before starting elections")
	masterCheckInterval = flag.Duration("master_check_interval", 5*time.Second, "Interval between checking mastership still held")
	masterHoldInterval  = flag.Duration("master_hold_interval", 60*time.Second, "Minimum interval to hold mastership for")
	resignOdds          = flag.Int("resign_odds", 10, "Chance of resigning mastership after each check, the N in 1-in-N")

	configFile = flag.String("config", "", "Config file containing flags, file contents can be overridden by command line flags")
)

func main() {
	flag.Parse()

	if *configFile != "" {
		if err := cmd.ParseFlagFile(*configFile); err != nil {
			glog.Exitf("Failed to load flags from config file %q: %s", *configFile, err)
		}
	}

	glog.CopyStandardLogTo("WARNING")
	glog.Info("**** Log Signer Starting ****")

	if *brokers == "" {
		glog.Exit("Need to specify Kafka brokers")
	}
	brokerList := strings.Split(*brokers, ",")
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Return.Successes = true
	kafka, err := sarama.NewClient(brokerList, config)
	if err != nil {
		glog.Exit("Error starting Kafka client:", err)
	}

	if *hbaseQuorum == "" {
		glog.Exit("Need to specify hbase host")
	}

	var hbaseClient gohbase.Client
	if *hbaseRoot != "" {
		hbaseClient = gohbase.NewClient(*hbaseQuorum, gohbase.ZookeeperRoot(*hbaseRoot))
	} else {
		hbaseClient = gohbase.NewClient(*hbaseQuorum)
	}

	if *httpEndpoint == "PORT0" {
		addr := fmt.Sprintf("0.0.0.0:%s", os.Getenv("PORT0"))
		*httpEndpoint = addr
	}

	ctx, cancel := context.WithCancel(context.Background())
	go util.AwaitSignal(cancel)

	hostname, _ := os.Hostname()
	instanceID := fmt.Sprintf("%s.%d", hostname, os.Getpid())
	var electionFactory util.ElectionFactory
	if *forceMaster {
		glog.Warning("**** Acting as master for all logs ****")
		electionFactory = util.NoopElectionFactory{InstanceID: instanceID}
	} else {
		electionFactory = etcd.NewElectionFactory(instanceID, *etcdServers, *lockDir)
	}

	st := yolo.NewLogStorage(kafka, hbaseClient)
	registry := extension.Registry{
		AdminStorage:    yolo.NewAdminStorage(st),
		SignerFactory:   keys.PEMSignerFactory{},
		LogStorage:      st,
		ElectionFactory: electionFactory,
		QuotaManager:    quota.Noop(),
		MetricFactory:   prometheus.MetricFactory{},
	}

	// Start HTTP server (optional)
	if *httpEndpoint != "" {
		// Announce our endpoint to etcd if so configured.
		unannounceHTTP := server.AnnounceSelf(ctx, *etcdServers, *etcdHTTPService, *httpEndpoint)
		if unannounceHTTP != nil {
			defer unannounceHTTP()
		}

		glog.Infof("Creating HTTP server starting on %v", *httpEndpoint)
		http.Handle("/metrics", promhttp.Handler())
		if err := util.StartHTTPServer(*httpEndpoint); err != nil {
			glog.Exitf("Failed to start HTTP server on %v: %v", *httpEndpoint, err)
		}
	}

	// Start the sequencing loop, which will run until we terminate the process. This controls
	// both sequencing and signing.
	// TODO(Martin2112): Should respect read only mode and the flags in tree control etc
	sequencerManager := server.NewSequencerManager(registry, *sequencerGuardWindowFlag)
	info := server.LogOperationInfo{
		Registry:            registry,
		BatchSize:           *batchSizeFlag,
		NumWorkers:          *numSeqFlag,
		RunInterval:         *sequencerIntervalFlag,
		TimeSource:          util.SystemTimeSource{},
		PreElectionPause:    *preElectionPause,
		MasterCheckInterval: *masterCheckInterval,
		MasterHoldInterval:  *masterHoldInterval,
		ResignOdds:          *resignOdds,
	}
	sequencerTask := server.NewLogOperationManager(info, sequencerManager)
	sequencerTask.OperationLoop(ctx)

	// Give things a few seconds to tidy up
	glog.Infof("Stopping server, about to exit")
	glog.Flush()
	time.Sleep(time.Second * 5)
}
