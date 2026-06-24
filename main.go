package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync"
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
	ch  chan *Msg
	url string
}

func main() {
	mode := flag.String("mode", "server", "server or client")
	listen := flag.String("addr", ":8081", "listen address")
	remote := flag.String("remote", "", "server URL for client mode")
	forward := flag.String("forward", "http://localhost:8081", "forward URL for client mode")
	flag.Parse()

	if *mode == "server" {
		runServer(*listen)
	} else {
		runClient(*remote, *forward)
	}
}

func runServer(addr string) {
	var (
		phone   *websocket.Conn
		phoneMu sync.Mutex
		pending = make(map[string]*pendingResp)
		pMu     sync.Mutex
		idSeq   int64
	)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Upgrade") == "websocket" {
			// Phone connection
			ws, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				log.Printf("ws upgrade: %v", err)
				return
			}
			phoneMu.Lock()
			if phone != nil {
				phone.Close()
			}
			phone = ws
			phoneMu.Unlock()

			log.Printf("phone connected from %s", r.RemoteAddr)
			defer func() {
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
				if resp.Type == "response" || resp.Type == "error" {
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
				}
			}
		}

		// Client request
		body, _ := io.ReadAll(r.Body)
		r.Body.Close()

		idSeq++
		id := fmt.Sprintf("%d", idSeq)

		headers := make(kv)
		for k, v := range r.Header {
			if len(v) > 0 {
				headers[k] = v[0]
			}
		}

		ch := make(chan *Msg, 1)
		pMu.Lock()
		pending[id] = &pendingResp{ch: ch, url: r.URL.String()}
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
		if err := ws.WriteJSON(req); err != nil {
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

	log.Printf("server on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}

func runClient(remoteAddr, forwardAddr string) {
	u, err := url.Parse(remoteAddr)
	if err != nil {
		log.Fatal(err)
	}

	for {
		ws, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		if err != nil {
			log.Printf("connect: %v, retry in 10s", err)
			time.Sleep(10 * time.Second)
			continue
		}
		log.Printf("connected to %s", u.String())

		func() {
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
				if req.Type != "request" {
					continue
				}

				go func(req Msg) {
					httpReq, err := http.NewRequest(req.Method, forwardAddr+req.URL, bytes.NewReader(req.Body))
					if err != nil {
						ws.WriteJSON(Msg{Type: "error", ID: req.ID, Error: err.Error()})
						return
					}
					for k, v := range req.Headers {
						httpReq.Header.Set(k, v)
					}

					client := &http.Client{Timeout: 120 * time.Second}
					httpResp, err := client.Do(httpReq)
					if err != nil {
						ws.WriteJSON(Msg{Type: "error", ID: req.ID, Error: err.Error()})
						return
					}
					defer httpResp.Body.Close()

					respBody, _ := io.ReadAll(httpResp.Body)

					headers := make(kv)
					for k, v := range httpResp.Header {
						if len(v) > 0 {
							headers[k] = v[0]
						}
					}

					resp := Msg{
						Type:    "response",
						ID:      req.ID,
						Status:  httpResp.StatusCode,
						Headers: headers,
						Body:    respBody,
					}
					ws.WriteJSON(resp)
				}(req)
			}
		}()

		time.Sleep(5 * time.Second)
	}
}
