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

// The trillian_log_server binary runs the Trillian log server, and also
// provides an admin server.
package main

import (
	"context"
	"flag"
	_ "net/http/pprof"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/golang/glog"
	"github.com/google/trillian"
	"github.com/google/trillian/cmd"
	"github.com/google/trillian/crypto/keys"
	"github.com/google/trillian/extension"
	"github.com/google/trillian/monitoring"
	"github.com/google/trillian/monitoring/prometheus"
	"github.com/google/trillian/quota"
	"github.com/google/trillian/server"
	"github.com/google/trillian/server/interceptor"
	"github.com/google/trillian/storage/yolo"
	"github.com/google/trillian/util"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/tsuna/gohbase"
	"google.golang.org/grpc"
)

var (
	brokers      = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The Kafka brokers to connect to, as a comma separated list")
	hbaseHost    = flag.String("hbase", "", "The HBase host to connect to")
	rpcEndpoint  = flag.String("rpc_endpoint", "localhost:8090", "Endpoint for RPC requests (host:port)")
	httpEndpoint = flag.String("http_endpoint", "localhost:8091", "Endpoint for HTTP metrics and REST requests on (host:port, empty means disabled)")

	etcdServers     = flag.String("etcd_servers", "", "A comma-separated list of etcd servers; no etcd registration if empty")
	etcdService     = flag.String("etcd_service", "trillian-logserver", "Service name to announce ourselves under")
	etcdHTTPService = flag.String("etcd_http_service", "trillian-logserver-http", "Service name to announce our HTTP endpoint under")

	configFile = flag.String("config", "", "Config file containing flags, file contents can be overridden by command line flags")
)

func main() {
	flag.Parse()

	if *configFile != "" {
		if err := cmd.ParseFlagFile(*configFile); err != nil {
			glog.Exitf("Failed to load flags from config file %q: %s", *configFile, err)
		}
	}

	ctx := context.Background()

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

	if *hbaseHost == "" {
		glog.Exit("Need to specify hbase host")
	}
	hbaseClient := gohbase.NewClient(*hbaseHost)

	// Announce our endpoints to etcd if so configured.
	unannounce := server.AnnounceSelf(ctx, *etcdServers, *etcdService, *rpcEndpoint)
	if unannounce != nil {
		defer unannounce()
	}
	if *httpEndpoint != "" {
		unannounceHTTP := server.AnnounceSelf(ctx, *etcdServers, *etcdHTTPService, *httpEndpoint)
		if unannounceHTTP != nil {
			defer unannounceHTTP()
		}
	}

	st := yolo.NewLogStorage(kafka, hbaseClient)
	registry := extension.Registry{
		AdminStorage:  yolo.NewAdminStorage(st),
		SignerFactory: keys.PEMSignerFactory{},
		LogStorage:    st,
		QuotaManager:  quota.Noop(), // TODO
		MetricFactory: prometheus.MetricFactory{},
	}

	ts := util.SystemTimeSource{}
	stats := monitoring.NewRPCStatsInterceptor(ts, "log", registry.MetricFactory)
	ti := &interceptor.TrillianInterceptor{
		Admin:        registry.AdminStorage,
		QuotaManager: registry.QuotaManager,
	}
	s := grpc.NewServer(
		grpc.UnaryInterceptor(interceptor.WrapErrors(interceptor.Combine(stats.Interceptor(), ti.UnaryInterceptor))))
	// No defer: server ownership is delegated to server.Main

	m := server.Main{
		RPCEndpoint:  *rpcEndpoint,
		HTTPEndpoint: *httpEndpoint,
		Registry:     registry,
		Server:       s,
		RegisterHandlerFn: func(context.Context, *runtime.ServeMux, string, []grpc.DialOption) error {
			return nil
		},
		RegisterServerFn: func(s *grpc.Server, registry extension.Registry) error {
			logServer := server.NewTrillianLogRPCServer(registry, ts)
			if err := logServer.IsHealthy(); err != nil {
				return err
			}
			trillian.RegisterTrillianLogServer(s, logServer)
			return err
		},
	}

	if err := m.Run(ctx); err != nil {
		glog.Exitf("Server exited with error: %v", err)
	}
}
