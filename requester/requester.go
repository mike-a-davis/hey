// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package requester provides commands to run load tests and display results.
package requester

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"sync"
	"time"

	"golang.org/x/net/http2"
)

// Max size of the buffer of result channel.
const maxResult = 100000000
const maxIdleConn = 500

type result struct {
	err           error
	statusCode    int
	offset        time.Duration
	duration      time.Duration
	connDuration  time.Duration // connection setup(DNS lookup + Dial up) duration
	dnsDuration   time.Duration // dns lookup duration
	reqDuration   time.Duration // request "write" duration
	resDuration   time.Duration // response "read" duration
	delayDuration time.Duration // delay between response and request
	contentLength int64
}

type Work struct {
	// Request is the request to be made.
	Request *http.Request

	RequestBody []byte

	// N is the total number of requests to make.
	N int

	// C is the concurrency level, the number of concurrent workers to run.
	C int

	// H2 is an option to make HTTP/2 requests
	H2 bool

	// Timeout in seconds.
	Timeout int

	// Qps is the rate limit in queries per second.
	QPS float64

	// DisableCompression is an option to disable compression in response
	DisableCompression bool

	// DisableKeepAlives is an option to prevents re-use of TCP connections between different HTTP requests
	DisableKeepAlives bool

	// DisableRedirects is an option to prevent the following of HTTP redirects
	DisableRedirects bool

	// Output represents the output type. If "csv" is provided, the
	// output will be dumped as a csv stream.
	Output string

	// ProxyAddr is the address of HTTP proxy server in the format on "host:port".
	// Optional.
	ProxyAddr *url.URL

	// Writer is where results will be written. If nil, results are written to stdout.
	Writer io.Writer

	initOnce sync.Once
	results  chan *result
	stopCh   chan struct{}
	start    time.Duration

	report *report
}

func (b *Work) writer() io.Writer {
	if b.Writer == nil {
		return os.Stdout
	}
	return b.Writer
}

// Init initializes internal data-structures
func (b *Work) Init() {
	b.initOnce.Do(func() {
		b.results = make(chan *result, min(b.C*1000, maxResult))
		b.stopCh = make(chan struct{}, b.C)
	})
}

// Run makes all the requests, prints the summary. It blocks until
// all work is done.
func (b *Work) Run() {
	b.Init()
	b.start = now()
	b.report = newReport(b.writer(), b.results, b.Output, b.N)
	// Run the reporter first, it polls the result channel until it is closed.
	go func() {
		runReporter(b.report)
	}()
	b.runWorkers()
	b.Finish()
}

func (b *Work) Stop() {
	// Send stop signal so that workers can stop gracefully.
	for i := 0; i < b.C; i++ {
		b.stopCh <- struct{}{}
	}
}

func (b *Work) Finish() {
	close(b.results)
	total := now() - b.start
	// Wait until the reporter is done.
	<-b.report.done
	b.report.finalize(total)
}

func (b *Work) makeRequest(c *http.Client) {
	s := now()
	var size int64
	var code int
	var dnsStart, connStart, resStart, reqStart, delayStart time.Duration
	var dnsDuration, connDuration, resDuration, reqDuration, delayDuration time.Duration
	req := cloneRequest(b.Request, b.RequestBody)
	trace := &httptrace.ClientTrace{
		DNSStart: func(info httptrace.DNSStartInfo) {
			dnsStart = now()
		},
		DNSDone: func(dnsInfo httptrace.DNSDoneInfo) {
			dnsDuration = now() - dnsStart
		},
		GetConn: func(h string) {
			connStart = now()
		},
		GotConn: func(connInfo httptrace.GotConnInfo) {
			if !connInfo.Reused {
				connDuration = now() - connStart
			}
			reqStart = now()
		},
		WroteRequest: func(w httptrace.WroteRequestInfo) {
			reqDuration = now() - reqStart
			delayStart = now()
		},
		GotFirstResponseByte: func() {
			delayDuration = now() - delayStart
			resStart = now()
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	resp, err := c.Do(req)
	if err == nil {
		size = resp.ContentLength
		code = resp.StatusCode
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}
	t := now()
	resDuration = t - resStart
	finish := t - s
	b.results <- &result{
		offset:        s,
		statusCode:    code,
		duration:      finish,
		err:           err,
		contentLength: size,
		connDuration:  connDuration,
		dnsDuration:   dnsDuration,
		reqDuration:   reqDuration,
		resDuration:   resDuration,
		delayDuration: delayDuration,
	}
}

func (b *Work) runWorker(client *http.Client, n int) {
	var throttle <-chan time.Time
	if b.QPS > 0 {
		throttle = time.Tick(time.Duration(1e6/(b.QPS)) * time.Microsecond)
	}

	if b.DisableRedirects {
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}
	}
	for i := 0; i < n; i++ {
		// Check if application is stopped. Do not send into a closed channel.
		select {
		case <-b.stopCh:
			return
		default:
			if b.QPS > 0 {
				<-throttle
			}
			b.makeRequest(client)
		}
	}
}

func (b *Work) runWorkers() {
	var wg sync.WaitGroup
	wg.Add(b.C)

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			ServerName:         b.Request.Host,
		},
		MaxIdleConnsPerHost: min(b.C, maxIdleConn),
		DisableCompression:  b.DisableCompression,
		DisableKeepAlives:   b.DisableKeepAlives,
		Proxy:               http.ProxyURL(b.ProxyAddr),
	}
	if b.H2 {
		http2.ConfigureTransport(tr)
	} else {
		tr.TLSNextProto = make(map[string]func(string, *tls.Conn) http.RoundTripper)
	}
	client := &http.Client{Transport: tr, Timeout: time.Duration(b.Timeout) * time.Second}

	// Ignore the case where b.N % b.C != 0.
	for i := 0; i < b.C; i++ {
		go func() {
			b.runWorker(client, b.N/b.C)
			wg.Done()
		}()
	}
	wg.Wait()
}

// cloneRequest returns a clone of the provided *http.Request.
// The clone is a shallow copy of the struct and its Header map.
func cloneRequest(r *http.Request, body []byte) *http.Request {

	nanos := time.Now().UnixNano()
	timestamp := int(nanos / 1000000)

	rand.Seed(nanos)
	low_average := rand.Intn(10-1+1) + 1
	// high_average := rand.Intn(30-10+10) + 10
	low_num := rand.Intn(100-1+1) + 1
	mid_num := rand.Intn(1000-100+1) + 100
	high_num := rand.Intn(10000-1000+1) + 1000
	large_num := rand.Intn(100000-10000+1) + 10000
	huge_num := rand.Intn(10000000-1000000+1) + 1000000
	vm_num := rand.Intn(10000000-1000+1) + 1000
	host_num := rand.Intn(1000000-10+1) + 10

	os_string := []string{"Windows", "Linux", "Mac"}
	vm_os := os_string[rand.Intn(len(os_string))]

	vm_cpu := fmt.Sprintf(`{"metric":"vsphere_vm_cpu","esxhostname":"DC0_H%d","guest":"other","host":"host%d.example.com","moid":"vm-%d","os":"%s","source":"DC0_H%d_VM0","vcenter":"localhost:8989","vmname":"DC0_H%d_VM0","run_summation":"%d","ready_summation":"%d","usage_average":"%d","used_summation":"%d","demand_average":"%d","timestamp":"%d"}`, host_num, host_num, vm_num, vm_os, vm_num, vm_num, high_num, mid_num, low_average, high_num, mid_num, timestamp)
	vm_net := fmt.Sprintf(`{"metric":"vsphere_vm_net","esxhostname":"DC0_H%d","guest":"other","host":"host%d.example.com","moid":"vm-%d","os":"%s","source":"DC0_H%d_VM0","vcenter":"localhost:8989","vmname":"DC0_H%d_VM0","bytesRx_average":"%d","bytesTx_average":"%d","timestamp":"%d"}`, host_num, host_num, vm_num, vm_os, vm_num, vm_num, high_num, high_num, timestamp)
	vm_virtualDisk := fmt.Sprintf(`{"metric":"vsphere_vm_virtualDisk","esxhostname":"DC0_H%d","guest":"other","host":"host%d.example.com","moid":"vm-%d","os":"%s","source":"DC0_H%d_VM0","vcenter":"localhost:8989","vmname":"DC0_H%d_VM0","write_average":"%d","read_average":"%d","timestamp":"%d"}`, host_num, host_num, vm_num, vm_os, vm_num, vm_num, mid_num, low_num, timestamp)
	host_net := fmt.Sprintf(`{"metric":"vsphere_host_net","esxhostname":"DC0_H%d","host":"host%d.example.com","interface":"vmnic0","moid":"host-%d","os":"%s","source":"DC0_H%d","vcenter":"localhost:8989","usage_average":"%d","bytesTx_average":"%d","bytesRx_average":"%d","timestamp":"%d"}`, host_num, host_num, host_num, vm_os, host_num, high_num, mid_num, low_num, timestamp)
	host_cpu := fmt.Sprintf(`{"metric":"vsphere_host_cpu","esxhostname":"DC0_H%d","host":"host%d.example.com","moid":"host-%d","os":"%s","source":"DC0_H%d","vcenter":"localhost:8989","utilization_average":"%d","usage_average":"%d","readiness_average":"%d","costop_summation":"%d","coreUtilization_average":"%d","wait_summation":"%d","idle_summation":"%d","latency_average":"%d","ready_summation":"%d","used_summation":"%d","timestamp":"%d"}`, host_num, host_num, host_num, vm_os, host_num, low_num, mid_num, low_num, low_num, mid_num, large_num, large_num, low_num, large_num, large_num, timestamp)
	host_disk := fmt.Sprintf(`{"metric":"vsphere_host_disk","esxhostname":"DC0_H%d","host":"host%d.example.com","moid":"host-%d","os":"%s","source":"DC0_H%d","vcenter":"localhost:8989","read_average":"%d","write_average":"%d","timestamp":"%d"}`, host_num, host_num, host_num, vm_os, host_num, mid_num, high_num, timestamp)
	host_mem := fmt.Sprintf(`{"metric":"vsphere_host_mem","esxhostname":"DC0_H%d","host":"host%d.example.com","moid":"host-%d","os":"%s","source":"DC0_H%d","vcenter":"localhost:8989","usage_average":"%d","timestamp":"%d"} `, host_num, host_num, host_num, vm_os, host_num, mid_num, timestamp)
	internal_vsphere := fmt.Sprintf(`{"metric":"internal_vsphere","host":"host%d.example.com","os":"%s","vcenter":"localhost:8989","connect_ns":"%d","discover_ns":"%d","discovered_objects":"%d","timestamp":"%d"}`, host_num, vm_os, large_num, huge_num, low_num, timestamp)
	internal_datastore := fmt.Sprintf(`{"metric":"internal_vsphere","host":"host%d.example.com","os":"%s","resourcetype":"datastore","vcenter":"localhost:8989","gather_duration_ns":"%d","gather_count":"%d","timestamp":"%d"}`, host_num, vm_os, large_num, low_num, timestamp)
	internal_vm := fmt.Sprintf(`{"metric":"internal_vsphere","host":"host%d.example.com","os":"%s","resourcetype":"vm","vcenter":"192.168.1.151","gather_duration_ns":"%d","gather_count":"%d","timestamp":"%d"}`, host_num, vm_os, large_num, low_num, timestamp)
	internal_host := fmt.Sprintf(`{"metric":"internal_vsphere","host":"host%d.example.com","os":"%s","resourcetype":"host","vcenter":"localhost:8989","gather_count":"%d","gather_duration_ns":"%d","timestamp":"%d"}`, host_num, vm_os, mid_num, huge_num, timestamp)
	internal_gather := fmt.Sprintf(`{"metric":"internal_gather","host":"host%d.example.com","input":"vsphere","os":"%s","gather_time_ns":"%d","metrics_gathered":"%d","timestamp":"%d"}`, host_num, vm_os, huge_num, mid_num, timestamp)

	var bstring []byte
	bstring = append(bstring, fmt.Sprintln(vm_cpu)...)
	bstring = append(bstring, fmt.Sprintln(vm_net)...)
	bstring = append(bstring, fmt.Sprintln(vm_virtualDisk)...)
	bstring = append(bstring, fmt.Sprintln(host_net)...)
	bstring = append(bstring, fmt.Sprintln(host_cpu)...)
	bstring = append(bstring, fmt.Sprintln(host_disk)...)
	bstring = append(bstring, fmt.Sprintln(host_mem)...)
	bstring = append(bstring, fmt.Sprintln(internal_vsphere)...)
	bstring = append(bstring, fmt.Sprintln(internal_datastore)...)
	bstring = append(bstring, fmt.Sprintln(internal_vm)...)
	bstring = append(bstring, fmt.Sprintln(internal_host)...)
	bstring = append(bstring, fmt.Sprintln(internal_gather)...)

	r2 := new(http.Request)

	*r2 = *r
	// deep copy of the Header
	r2.Header = make(http.Header, len(r.Header))
	for k, s := range r.Header {
		r2.Header[k] = append([]string(nil), s...)
	}

	r2.Body = ioutil.NopCloser(bytes.NewReader(bstring))
	r2.ContentLength = int64(len(bstring))
	return r2

}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
