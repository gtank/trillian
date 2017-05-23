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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/trillian/util"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Arbitrary time for use in tests
var fakeTime = time.Date(2016, 10, 3, 12, 38, 27, 36, time.UTC)

type recordingUnaryHandler struct {
	// ctx and req are recorded on invocation
	ctx context.Context
	req interface{}
	// rsp and err are returned on invocation
	rsp interface{}
	err error
}

func (r recordingUnaryHandler) handler() grpc.UnaryHandler {
	return func(ctx context.Context, req interface{}) (interface{}, error) {
		r.ctx = ctx
		r.req = req
		return r.rsp, r.err
	}
}

func getCounterValue(vec *prometheus.CounterVec, method string) (float64, error) {
	labels := prometheus.Labels{methodName: method}
	counter, err := vec.GetMetricWith(labels)
	if err != nil {
		return -1, fmt.Errorf("failed to GetMetricWith(%+v): %v", labels, err)
	}
	var metric dto.Metric
	if err := counter.Write(&metric); err != nil {
		return -1, fmt.Errorf("failed to Write() stat: %v", err)
	}
	counterVal := metric.GetCounter()
	if counterVal == nil {
		return -1, errors.New("failed to GetCounter()")
	}
	return counterVal.GetValue(), nil
}

func getHistogramInfo(vec *prometheus.HistogramVec, method string) (uint64, float64, error) {
	labels := prometheus.Labels{methodName: method}
	hist, err := vec.MetricVec.GetMetricWith(labels)
	if err != nil {
		return 0, -1, fmt.Errorf("failed to GetMetricWith(%+v): %v", labels, err)
	}
	var metric dto.Metric
	if err := hist.Write(&metric); err != nil {
		return 0, -1, fmt.Errorf("failed to Write() stat: %v", err)
	}
	histVal := metric.GetHistogram()
	if histVal == nil {
		return 0, -1, errors.New("failed to GetHistogram()")
	}
	return histVal.GetSampleCount(), histVal.GetSampleSum(), nil
}

func TestSingleRequests(t *testing.T) {
	var tests = []struct {
		name       string
		method     string
		handler    recordingUnaryHandler
		timeSource util.IncrementingFakeTimeSource
	}{
		// This is an OK request with 500ms latency
		{
			name:    "ok_request",
			method:  "getmethod",
			handler: recordingUnaryHandler{req: "OK", err: nil},
			timeSource: util.IncrementingFakeTimeSource{
				BaseTime:   fakeTime,
				Increments: []time.Duration{0, time.Millisecond * 500},
			},
		},
		// This is an errored request with 3000ms latency
		{
			name:    "error_request",
			method:  "setmethod",
			handler: recordingUnaryHandler{err: errors.New("bang")},
			timeSource: util.IncrementingFakeTimeSource{
				BaseTime:   fakeTime,
				Increments: []time.Duration{0, time.Millisecond * 3000},
			},
		},
	}

	for _, test := range tests {
		prefix := fmt.Sprintf("test_%s", test.name)
		stats := NewRPCStatsInterceptor(&test.timeSource, prefix)
		i := stats.Interceptor()

		// Invoke the test handler wrapped by the interceptor.
		got, err := i(context.Background(), "wibble", &grpc.UnaryServerInfo{FullMethod: test.method}, test.handler.handler())

		// Check the interceptor passed through the results.
		if got != test.handler.rsp || (err != nil) != (test.handler.err != nil) {
			t.Errorf("interceptor(%s)=%v,%v; want %v,%v", test.name, got, err, test.handler.rsp, test.handler.err)
		}

		// Now check the resulting state of the metrics.
		if got, err := getCounterValue(stats.reqCount, test.method); err != nil {
			t.Errorf("failed to get counter value: %v", err)
			continue
		} else if want := 1.0; got != want {
			t.Errorf("stats.reqCount=%v; want %v", got, want)
		}
		wantLatency := float64(test.timeSource.Increments[1].Nanoseconds() / int64(time.Millisecond))
		wantErrors := 0.0
		wantSuccess := 0.0
		if test.handler.err == nil {
			wantSuccess = 1.0
		} else {
			wantErrors = 1.0
		}
		if got, err := getCounterValue(stats.reqSuccessCount, test.method); err != nil {
			t.Errorf("failed to get counter value: %v", err)
			continue
		} else if got != wantSuccess {
			t.Errorf("stats.reqSuccessCount=%v; want %v", got, wantSuccess)
		}
		if got, err := getCounterValue(stats.reqErrorCount, test.method); err != nil {
			t.Errorf("failed to get counter value: %v", err)
			continue
		} else if got != wantErrors {
			t.Errorf("stats.reqErrorCount=%v; want %v", got, wantSuccess)
		}

		if gotCount, gotSum, err := getHistogramInfo(stats.reqSuccessLatency, test.method); err != nil {
			t.Errorf("failed to get histogram values: %v", err)
			continue
		} else if gotCount != uint64(wantSuccess) {
			t.Errorf("stats.reqSuccessLatency.Count=%v; want %v", gotCount, wantSuccess)
		} else if gotSum != wantLatency*wantSuccess {
			t.Errorf("stats.reqSuccessLatency.Sum=%v; want %v", gotSum, wantLatency*wantSuccess)
		}
		if gotCount, gotSum, err := getHistogramInfo(stats.reqErrorLatency, test.method); err != nil {
			t.Errorf("failed to get histogram values: %v", err)
			continue
		} else if gotCount != uint64(wantErrors) {
			t.Errorf("stats.reqErrorLatency.Count=%v; want %v", gotCount, wantErrors)
		} else if gotSum != wantLatency*wantErrors {
			t.Errorf("stats.reqErrorLatency.Sum=%v; want %v", gotSum, wantLatency*wantErrors)
		}
	}
}

func TestMultipleOKRequestsTotalLatency(t *testing.T) {
	// We're going to make 3 requests so set up the time source appropriately
	ts := util.IncrementingFakeTimeSource{
		BaseTime: fakeTime,
		Increments: []time.Duration{
			0,
			time.Millisecond * 500,
			0,
			time.Millisecond * 2000,
			0,
			time.Millisecond * 1337,
		},
	}
	handler := recordingUnaryHandler{rsp: "OK", err: nil}
	stats := NewRPCStatsInterceptor(&ts, "test_multi_ok")
	i := stats.Interceptor()

	for r := 0; r < 3; r++ {
		rsp, err := i(context.Background(), "wibble", &grpc.UnaryServerInfo{FullMethod: "testmethod"}, handler.handler())
		if rsp != "OK" || err != nil {
			t.Fatalf("interceptor()=%v,%v; want 'OK',nil", rsp, err)
		}
	}
	if count, sum, err := getHistogramInfo(stats.reqSuccessLatency, "testmethod"); err != nil {
		t.Errorf("failed to get histogram values: %v", err)
	} else if count != 3 {
		t.Errorf("stats.reqSuccessLatency.Count=%v; want %v", count, 3)
	} else if sum != 3837 {
		t.Errorf("stats.reqSuccessLatency.Sum=%v; want %v", sum, 3837)
	}
}

func TestMultipleErrorRequestsTotalLatency(t *testing.T) {
	// We're going to make 3 requests so set up the time source appropriately
	ts := util.IncrementingFakeTimeSource{
		BaseTime: fakeTime,
		Increments: []time.Duration{
			0,
			time.Millisecond * 427,
			0,
			time.Millisecond * 1066,
			0,
			time.Millisecond * 1123,
		},
	}
	handler := recordingUnaryHandler{rsp: "", err: errors.New("bang")}
	stats := NewRPCStatsInterceptor(&ts, "test_multi_err")
	i := stats.Interceptor()

	for r := 0; r < 3; r++ {
		_, err := i(context.Background(), "wibble", &grpc.UnaryServerInfo{FullMethod: "testmethod"}, handler.handler())
		if err == nil {
			t.Fatalf("interceptor()=_,%v; want _,'bang'", err)
		}
	}

	if count, sum, err := getHistogramInfo(stats.reqErrorLatency, "testmethod"); err != nil {
		t.Errorf("failed to get histogram values: %v", err)
	} else if count != 3 {
		t.Errorf("stats.reqSuccessLatency.Count=%v; want %v", count, 3)
	} else if sum != 2616 {
		t.Errorf("stats.reqSuccessLatency.Sum=%v; want %v", sum, 2616)
	}
}
