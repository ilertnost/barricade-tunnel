package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

type Msg struct {
	Type    string `json:"type"`
	ID      string `json:"id"`
	Method  string `json:"method,omitempty"`
	URL     string `json:"url,omitempty"`
	Headers kv     `json:"headers,omitempty"`
	Body    []byte `json:"body,omitempty"`
	Status  int    `json:"status,omitempty"`
	Error   string `json:"error,omitempty"`
}

type kv map[string]string

type pendingResp struct {
	ch chan *Msg
}

func main() {
	mode := flag.String("mode", "server", "server or client")
	listen := flag.String("addr", ":8081", "listen address")
	remote := flag.String("remote", "", "server URL for client mode")
	forward := flag.String("forward", "http://localhost:8081", "forward URL for client mode")
	phonePath := flag.String("phone-path", "/phone", "phone WebSocket path")
	insecure := flag.Bool("insecure", false, "skip TLS verification")
	flag.Parse()

	if *mode == "server" {
		runServer(*listen, *phonePath)
	} else {
		runClient(*remote, *forward, *phonePath, *insecure)
	}
}

func runServer(addr, phonePath string) {
	var (
		phone   *websocket.Conn
		phoneMu sync.Mutex
		writeMu sync.Mutex
		pending = make(map[string]*pendingResp)
		pMu     sync.Mutex
		idSeq   int64
		proxies sync.Map
	)

	// Phone connection
	http.HandleFunc(phonePath, func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("phone ws upgrade: %v", err)
			return
		}
		phoneMu.Lock()
		if phone != nil {
			phone.Close()
		}
		phone = ws
		phoneMu.Unlock()

		log.Printf("phone connected from %s", r.RemoteAddr)

		// Keepalive pings every 20s
		pingStop := make(chan struct{})
		go func() {
			ticker := time.NewTicker(20 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					writeMu.Lock()
					err := ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(10*time.Second))
					writeMu.Unlock()
					if err != nil {
						return
					}
				case <-pingStop:
					return
				}
			}
		}()

		defer func() {
			close(pingStop)
			phoneMu.Lock()
			phone = nil
			phoneMu.Unlock()
			ws.Close()
			log.Printf("phone disconnected")
		}()

		for {
			_, msg, err := ws.ReadMessage()
			if err != nil {
				return
			}
			var resp Msg
			if err := json.Unmarshal(msg, &resp); err != nil {
				continue
			}
			switch resp.Type {
			case "response", "error":
				pMu.Lock()
				pr := pending[resp.ID]
				delete(pending, resp.ID)
				pMu.Unlock()
				if pr != nil {
					select {
					case pr.ch <- &resp:
					default:
					}
				}
			case "ping":
				writeMu.Lock()
				ws.WriteJSON(Msg{Type: "pong"})
				writeMu.Unlock()
			}
		}
	})

	// Phone proxy WebSocket — phone connects here for each proxy
	http.HandleFunc("/_proxy/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/_proxy/")
		if id == "" {
			http.Error(w, "missing proxy id", 400)
			return
		}
		phoneProxy, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("proxy ws upgrade %s: %v", id, err)
			return
		}
		if v, ok := proxies.Load(id); ok {
			v.(chan *websocket.Conn) <- phoneProxy
		} else {
			phoneProxy.Close()
		}
	})

	// WebSocket proxy — Flutter client connects here
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		clientWS, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("client ws upgrade: %v", err)
			return
		}
		defer clientWS.Close()

		phoneMu.Lock()
		p := phone
		phoneMu.Unlock()
		if p == nil {
			return
		}

		proxyID := fmt.Sprintf("proxy_%d", atomic.AddInt64(&idSeq, 1))
		proxyCh := make(chan *websocket.Conn, 1)
		proxies.Store(proxyID, proxyCh)
		defer proxies.Delete(proxyID)

		writeMu.Lock()
		err = p.WriteJSON(Msg{
			Type: "proxy_start",
			ID:   proxyID,
			URL:  r.URL.RequestURI(),
		})
		writeMu.Unlock()
		if err != nil {
			log.Printf("proxy %s: write to phone: %v", proxyID, err)
			return
		}

		var phoneProxy *websocket.Conn
		select {
		case phoneProxy = <-proxyCh:
		case <-time.After(30 * time.Second):
			log.Printf("proxy %s: phone didn't connect", proxyID)
			return
		}
		defer phoneProxy.Close()

		log.Printf("proxy %s: connected", proxyID)

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			for {
				_, data, err := clientWS.ReadMessage()
				if err != nil {
					break
				}
				if err := phoneProxy.WriteMessage(websocket.TextMessage, data); err != nil {
					break
				}
			}
		}()

		go func() {
			defer wg.Done()
			for {
				_, data, err := phoneProxy.ReadMessage()
				if err != nil {
					break
				}
				if err := clientWS.WriteMessage(websocket.TextMessage, data); err != nil {
					break
				}
			}
		}()

		wg.Wait()
	})

	// HTTP request handling
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == phonePath || r.URL.Path == "/ws" || strings.HasPrefix(r.URL.Path, "/_proxy/") {
			return
		}

		body, _ := io.ReadAll(r.Body)
		r.Body.Close()

		id := fmt.Sprintf("%d", atomic.AddInt64(&idSeq, 1))

		headers := make(kv)
		for k, v := range r.Header {
			if len(v) > 0 {
				headers[k] = v[0]
			}
		}

		ch := make(chan *Msg, 1)
		pMu.Lock()
		pending[id] = &pendingResp{ch: ch}
		pMu.Unlock()

		phoneMu.Lock()
		ws := phone
		phoneMu.Unlock()

		if ws == nil {
			http.Error(w, "phone not connected", http.StatusServiceUnavailable)
			pMu.Lock()
			delete(pending, id)
			pMu.Unlock()
			return
		}

		req := Msg{
			Type:    "request",
			ID:      id,
			Method:  r.Method,
			URL:     r.URL.RequestURI(),
			Headers: headers,
			Body:    body,
		}
		writeMu.Lock()
		werr := ws.WriteJSON(req)
		writeMu.Unlock()
		if werr != nil {
			http.Error(w, "tunnel error", http.StatusBadGateway)
			pMu.Lock()
			delete(pending, id)
			pMu.Unlock()
			return
		}

		timeout := time.NewTimer(120 * time.Second)
		defer timeout.Stop()

		select {
		case resp := <-ch:
			if resp.Type == "error" || resp.Error != "" {
				http.Error(w, resp.Error, http.StatusBadGateway)
				return
			}
			for k, v := range resp.Headers {
				w.Header().Set(k, v)
			}
			w.WriteHeader(resp.Status)
			w.Write(resp.Body)
		case <-timeout.C:
			http.Error(w, "tunnel timeout", http.StatusGatewayTimeout)
		}
	})

	log.Printf("server on %s (phone path: %s)", addr, phonePath)
	log.Fatal(http.ListenAndServe(addr, nil))
}

func runClient(remoteAddr, forwardAddr, phonePath string, insecure bool) {
	u, err := url.Parse(remoteAddr)
	if err != nil {
		log.Fatal(err)
	}
	baseURL := fmt.Sprintf("%s://%s", u.Scheme, u.Host)

	dialer := websocket.DefaultDialer
	if insecure {
		dialer.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	phoneURL := strings.TrimSuffix(remoteAddr, "/") + phonePath

	for {
		ws, _, err := dialer.Dial(phoneURL, nil)
		if err != nil {
			log.Printf("connect: %v, retry in 10s", err)
			time.Sleep(10 * time.Second)
			continue
		}
		log.Printf("connected to %s", phoneURL)

		var wMu sync.Mutex

		// Client keepalive: send ping every 15s
		done := make(chan struct{})
		go func() {
			ticker := time.NewTicker(15 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					wMu.Lock()
					err := ws.WriteMessage(websocket.TextMessage, []byte(`{"type":"ping"}`))
					wMu.Unlock()
					if err != nil {
						ws.Close()
						return
					}
				case <-done:
					return
				}
			}
		}()

		func() {
			defer close(done)
			defer ws.Close()
			for {
				_, msg, err := ws.ReadMessage()
				if err != nil {
					log.Printf("read: %v", err)
					return
				}
				var req Msg
				if err := json.Unmarshal(msg, &req); err != nil {
					continue
				}

				// Skip keepalive pongs/responses
				if req.Type == "pong" || req.Type == "ping" {
					continue
				}

				switch req.Type {
				case "request":
					go func() {
						resp := handleHTTPRequest(req, forwardAddr)
						wMu.Lock()
						err := ws.WriteJSON(resp)
						wMu.Unlock()
						if err != nil {
							log.Printf("write response: %v", err)
						}
					}()
			case "proxy_start":
				log.Printf("proxy_start received: id=%s url=%s", req.ID, req.URL)
				go handleProxyStart(ws, &wMu, req, baseURL, forwardAddr, insecure)
				}
			}
		}()

		time.Sleep(5 * time.Second)
	}
}

func handleHTTPRequest(req Msg, forwardAddr string) Msg {
	httpReq, err := http.NewRequest(req.Method, forwardAddr+req.URL, bytes.NewReader(req.Body))
	if err != nil {
		return Msg{Type: "error", ID: req.ID, Error: err.Error()}
	}
	for k, v := range req.Headers {
		httpReq.Header.Set(k, v)
	}

	client := &http.Client{Timeout: 120 * time.Second}
	httpResp, err := client.Do(httpReq)
	if err != nil {
		return Msg{Type: "error", ID: req.ID, Error: err.Error()}
	}
	defer httpResp.Body.Close()

	respBody, _ := io.ReadAll(httpResp.Body)

	headers := make(kv)
	for k, v := range httpResp.Header {
		if len(v) > 0 {
			headers[k] = v[0]
		}
	}

	return Msg{
		Type:    "response",
		ID:      req.ID,
		Status:  httpResp.StatusCode,
		Headers: headers,
		Body:    respBody,
	}
}

func handleProxyStart(ws *websocket.Conn, wMu *sync.Mutex, req Msg, baseURL, forwardAddr string, insecure bool) {
	proxyID := req.ID
	proxyURL := strings.TrimSuffix(baseURL, "/") + "/_proxy/" + proxyID

	dialer := websocket.DefaultDialer
	if insecure {
		dialer.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	proxyWS, _, err := dialer.Dial(proxyURL, nil)
	if err != nil {
		log.Printf("proxy %s: dial %v", proxyID, err)
		return
	}
	defer proxyWS.Close()

	targetURL := strings.TrimSuffix(forwardAddr, "/") + req.URL
	targetURL = strings.Replace(targetURL, "http://", "ws://", 1)
	targetURL = strings.Replace(targetURL, "https://", "wss://", 1)

	targetWS, _, err := websocket.DefaultDialer.Dial(targetURL, nil)
	if err != nil {
		log.Printf("proxy %s: dial target %v", proxyID, err)
		return
	}
	defer targetWS.Close()

	log.Printf("proxy %s: established %s -> %s", proxyID, proxyURL, targetURL)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for {
			_, data, err := proxyWS.ReadMessage()
			if err != nil {
				return
			}
			if err := targetWS.WriteMessage(websocket.TextMessage, data); err != nil {
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for {
			_, data, err := targetWS.ReadMessage()
			if err != nil {
				return
			}
			if err := proxyWS.WriteMessage(websocket.TextMessage, data); err != nil {
				return
			}
		}
	}()

	wg.Wait()
}
