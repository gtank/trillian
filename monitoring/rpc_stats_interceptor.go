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

package monitoring

import (
	"fmt"
	"time"

	"github.com/google/trillian/util"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	reqCountName          = "rpc_requests_total"
	reqSuccessCountName   = "rpc_success_total"
	reqSuccessLatencyName = "rpc_success_latency_ms"
	reqErrorCountName     = "rpc_errors_total"
	reqErrorLatencyName   = "rpc_errors_latency_ms"
	methodName            = "method"
)

// RPCStatsInterceptor provides a gRPC interceptor that records statistics about the RPCs passing through it.
type RPCStatsInterceptor struct {
	prefix            string
	timeSource        util.TimeSource
	reqCount          *prometheus.CounterVec
	reqSuccessCount   *prometheus.CounterVec
	reqSuccessLatency *prometheus.HistogramVec
	reqErrorCount     *prometheus.CounterVec
	reqErrorLatency   *prometheus.HistogramVec
}

// NewRPCStatsInterceptor creates a new RPCStatsInterceptor for the given application/component, with
// a specified time source.
func NewRPCStatsInterceptor(timeSource util.TimeSource, prefix string) *RPCStatsInterceptor {
	interceptor := RPCStatsInterceptor{
		prefix:     prefix,
		timeSource: timeSource,
		reqCount: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefixedName(prefix, reqCountName),
				Help: "Number of requests",
			},
			[]string{methodName},
		),
		reqSuccessCount: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefixedName(prefix, reqSuccessCountName),
				Help: "Number of successful requests",
			},
			[]string{methodName},
		),
		reqSuccessLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: prefixedName(prefix, reqSuccessLatencyName),
				Help: "Latency of successful requests",
			},
			[]string{methodName},
		),
		reqErrorCount: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefixedName(prefix, reqErrorCountName),
				Help: "Number of errored requests",
			},
			[]string{methodName},
		),
		reqErrorLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: prefixedName(prefix, reqErrorLatencyName),
				Help: "Latency of errored requests",
			},
			[]string{methodName},
		),
	}
	prometheus.MustRegister(interceptor.reqCount)
	prometheus.MustRegister(interceptor.reqSuccessCount)
	prometheus.MustRegister(interceptor.reqSuccessLatency)
	prometheus.MustRegister(interceptor.reqErrorCount)
	prometheus.MustRegister(interceptor.reqErrorLatency)
	return &interceptor
}

func prefixedName(prefix, name string) string {
	return fmt.Sprintf("%s_%s", prefix, name)
}

func (r *RPCStatsInterceptor) recordFailureLatency(labels prometheus.Labels, startTime time.Time) {
	latency := r.timeSource.Now().Sub(startTime)
	r.reqErrorCount.With(labels).Inc()
	r.reqErrorLatency.With(labels).Observe(float64(latency.Nanoseconds() / int64(time.Millisecond)))
}

// Interceptor returns a UnaryServerInterceptor that can be registered with an RPC server and
// will record request counts / errors and latencies for that servers handlers
func (r *RPCStatsInterceptor) Interceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		method := info.FullMethod
		labels := prometheus.Labels{methodName: method}

		// Increase the request count for the method and start the clock
		r.reqCount.With(labels).Inc()
		startTime := r.timeSource.Now()

		defer func() {
			if rec := recover(); rec != nil {
				// If we reach here then the handler exited via panic, count it as a server failure
				r.recordFailureLatency(labels, startTime)
				panic(rec)
			}
		}()

		// Invoke the actual operation
		rsp, err := handler(ctx, req)

		// Record success / failure and latency
		if err != nil {
			r.recordFailureLatency(labels, startTime)
		} else {
			latency := r.timeSource.Now().Sub(startTime)
			r.reqSuccessCount.With(labels).Inc()
			r.reqSuccessLatency.With(labels).Observe(float64(latency.Nanoseconds() / int64(time.Millisecond)))
		}

		// Pass the result of the handler invocation back
		return rsp, err
	}
}
