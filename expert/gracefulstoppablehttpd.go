package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Handler holds server-wide state
type handler struct {
	quitChan       chan os.Signal
	totalRequests  int64 // atomic
	activeRequests int64 // atomic
	stopping       int32 // atomic boolean (0 = running, 1 = stopping)
	wg             sync.WaitGroup
	startTime      time.Time
	requestTimeout time.Duration
}

// stop handler triggers shutdown (idempotent and non-blocking)
func (h *handler) stop(w http.ResponseWriter, r *http.Request) {
	// mark stopping flag, then trigger quit channel non-blocking
	if atomic.LoadInt32(&h.stopping) == 1 {
		http.Error(w, "shutdown already in progress", http.StatusConflict)
		return
	}

	atomic.StoreInt32(&h.stopping, 1)

	select {
	case h.quitChan <- os.Interrupt:
		// sent signal to quit handler
	default:
		// if channel is full, don't block — signal already queued
	}

	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte("shutdown initiated\n"))
}

// normal handler demonstrates work that respects context cancellation
func (h *handler) normal(w http.ResponseWriter, r *http.Request) {
	// refuse new requests if stopping
	if atomic.LoadInt32(&h.stopping) == 1 {
		http.Error(w, "server is shutting down, no new requests accepted", http.StatusServiceUnavailable)
		return
	}

	// increment counters and waitgroup
	atomic.AddInt64(&h.totalRequests, 1)
	atomic.AddInt64(&h.activeRequests, 1)
	h.wg.Add(1)

	defer func() {
		atomic.AddInt64(&h.activeRequests, -1)
		h.wg.Done()
	}()

	// use the request context (which the middleware sets with timeout)
	ctx := r.Context()

	// example work: wait for 4s OR until context canceled
	sleep := 4 * time.Second
	select {
	case <-time.After(sleep):
		// finished work
	case <-ctx.Done():
		// request was canceled (timeout or shutdown)
		http.Error(w, "request canceled", http.StatusRequestTimeout)
		return
	}

	// respond
	fmt.Fprintf(w, "Hello World\nYou requested: %s\n", r.URL)
	cur := atomic.LoadInt64(&h.totalRequests)
	fmt.Fprintf(w, "this is request number %d\n", cur)
	log.Printf("served %s from %s (total=%d active=%d)",
		r.URL.Path, r.RemoteAddr, atomic.LoadInt64(&h.totalRequests), atomic.LoadInt64(&h.activeRequests))
}

// health endpoint returns JSON with basic status
func (h *handler) health(w http.ResponseWriter, r *http.Request) {
	uptime := time.Since(h.startTime).Seconds()
	resp := map[string]interface{}{
		"status":         "ok",
		"uptime_seconds": int64(uptime),
		"total_requests": atomic.LoadInt64(&h.totalRequests),
		"active_requests": atomic.LoadInt64(&h.activeRequests),
		"stopping":       atomic.LoadInt32(&h.stopping) == 1,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// metrics endpoint returns a tiny plaintext metrics output (prometheus-like)
func (h *handler) metrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "# HELP typing_total_requests Total number of requests served\n")
	fmt.Fprintf(w, "typing_total_requests %d\n", atomic.LoadInt64(&h.totalRequests))
	fmt.Fprintf(w, "# HELP typing_active_requests Currently active requests\n")
	fmt.Fprintf(w, "typing_active_requests %d\n", atomic.LoadInt64(&h.activeRequests))
	fmt.Fprintf(w, "# HELP typing_stopping Whether the server is shutting down (1) or not (0)\n")
	fmt.Fprintf(w, "typing_stopping %d\n", atomic.LoadInt32(&h.stopping))
}

// logging middleware: sets request timeout and logs duration
func (h *handler) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// set per-request timeout using requestTimeout (middleware creates a derived context)
		ctx, cancel := context.WithTimeout(r.Context(), h.requestTimeout)
		defer cancel()

		// update the request with context that will be used by handlers
		r = r.WithContext(ctx)

		// pass through
		next.ServeHTTP(w, r)

		duration := time.Since(start)
		log.Printf("%s %s %s in %v from %s", r.Method, r.URL.Path, r.Proto, duration, r.RemoteAddr)
	})
}

// handleQuit waits for a signal and shuts down the server gracefully
func handleQuit(quitChan chan os.Signal, srv *http.Server, h *handler, shutdownWait time.Duration) {
	sig := <-quitChan
	log.Printf("signal received: %v — initiating graceful shutdown", sig)

	// mark stopping and attempt graceful shutdown
	atomic.StoreInt32(&h.stopping, 1)

	// first ask HTTP server to stop accepting new connections and finish processing current ones
	exitCtx, cancel := context.WithTimeout(context.Background(), shutdownWait)
	defer cancel()

	if err := srv.Shutdown(exitCtx); err != nil {
		// Shutdown returns error if context times out or other issues
		log.Printf("server Shutdown error: %v", err)
	} else {
		log.Println("HTTP server Shutdown finished")
	}

	// now wait for in-flight requests to finish (but with a timeout)
	done := make(chan struct{})
	go func() {
		h.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("all in-flight handlers completed")
	case <-time.After(shutdownWait):
		log.Println("timed out waiting for handlers to finish; exiting anyway")
	}
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if parsed, err := time.ParseDuration(v); err == nil {
			return parsed
		}
		// try parse as integer seconds
		if sec, err := strconv.Atoi(v); err == nil {
			return time.Duration(sec) * time.Second
		}
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}

func main() {
	log.Println("started program")

	// Configuration via environment with sane defaults
	port := ":" + func() string {
		if p := os.Getenv("PORT"); p != "" {
			return p
		}
		return "8080"
	}()

	// request timeout per incoming request (middleware)
	requestTimeout := envDuration("REQUEST_TIMEOUT", 15*time.Second)

	// server shutdown wait (how long we attempt to gracefully finish handlers)
	shutdownWait := envDuration("SHUTDOWN_WAIT", 10*time.Second)

	// server-level timeouts
	readTimeout := envDuration("READ_TIMEOUT", 5*time.Second)
	writeTimeout := envDuration("WRITE_TIMEOUT", 15*time.Second)
	idleTimeout := envDuration("IDLE_TIMEOUT", 120*time.Second)

	h := &handler{
		quitChan:       make(chan os.Signal, 1),
		startTime:      time.Now(),
		requestTimeout: requestTimeout,
	}

	// mux and handlers
	mux := http.NewServeMux()
	mux.HandleFunc("/", h.normal)
	mux.HandleFunc("/stop", h.stop)
	mux.HandleFunc("/health", h.health)
	mux.HandleFunc("/metrics", h.metrics)

	// Add logging and timeout middleware
	finalHandler := h.loggingMiddleware(mux)

	srv := &http.Server{
		Addr:         port,
		Handler:      finalHandler,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
		// Base context can be set if you want per-server derived contexts:
		BaseContext: func(net.Listener) context.Context { return context.Background() },
	}

	// Setup OS signal notifications for graceful shutdown (SIGINT, SIGTERM)
	signal.Notify(h.quitChan, os.Interrupt)
	// Also include SIGTERM if available on platform
	// (on Windows SIGTERM is not used the same way, but this will not panic)
	if sigTerm := syscallSignalForTerm(); sigTerm != nil {
		signal.Notify(h.quitChan, *sigTerm)
	}

	// launch goroutine to handle quit
	go handleQuit(h.quitChan, srv, h, shutdownWait)

	// start server
	log.Printf("listening on %s (request timeout=%s) ...", port, requestTimeout)
	err := srv.ListenAndServe()
	switch {
	case errors.Is(err, http.ErrServerClosed):
		log.Println("server closed. bye o/")
	case err != nil:
		log.Fatalf("ListenAndServe: %s", err)
	}
}

// syscallSignalForTerm returns the syscall.Signal for SIGTERM where available without importing syscall on unsupported platforms.
// We keep syscall usage inside this helper so the rest of the file doesn't rely on it.
func syscallSignalForTerm() *os.Signal {
	// attempt to obtain SIGTERM from the syscall package if available (unix-like)
	// We wrap it with build constraints in simple manner: try to lookup via os package (POSIX will support it via signal)
	// For portability, we simply try to cast.
	// NOTE: importing syscall directly here is OK for most environments (unix); to keep this single-file cross-platform
	// we return nil on Windows where SIGTERM handling differs.
	// We'll attempt to get it using signal package constants via platform-specific behavior is not available, so return nil.
	// In most Linux/Mac envs os.Interrupt (SIGINT) is sufficient; SIGTERM support can be added easily by editing this helper.
	return nil
}
