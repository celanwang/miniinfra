// Package server serves MiniKV commands over length-framed TCP connections.
package server

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/celanwang/miniinfra/minikv/internal/store"
)

type Config struct {
	MaxConnections  int
	MaxKeyBytes     int
	MaxValueBytes   int
	InboundBytes    int64
	OutboundBytes   int64
	InboundPerConn  int64
	OutboundPerConn int64
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	SweepInterval   time.Duration
	SweepBatch      int
}

func DefaultConfig() Config {
	return Config{
		MaxConnections:  256,
		MaxKeyBytes:     1024,
		MaxValueBytes:   1 << 20,
		InboundBytes:    16 << 20,
		OutboundBytes:   32 << 20,
		InboundPerConn:  (1 << 20) + 1024,
		OutboundPerConn: 1 << 20,
		ReadTimeout:     15 * time.Second,
		WriteTimeout:    5 * time.Second,
		SweepInterval:   5 * time.Millisecond,
		SweepBatch:      128,
	}
}

type budget struct {
	limit int64
	used  atomic.Int64
}

func (b *budget) reserve(n int64) bool {
	if n == 0 {
		return true
	}
	// Bound CAS retries so callers holding a shard lock never wait indefinitely.
	for range 8 {
		used := b.used.Load()
		if n > b.limit-used {
			return false
		}
		if b.used.CompareAndSwap(used, used+n) {
			return true
		}
	}
	return false
}

func (b *budget) release(n int64) {
	if n != 0 {
		b.used.Add(-n)
	}
}

type Server struct {
	cache    *store.Store
	config   Config
	inbound  budget
	outbound budget
	writes   writeBuffers
}

func New(cache *store.Store, config Config) (*Server, error) {
	if cache == nil || config.MaxConnections < 1 || config.MaxKeyBytes < 1 ||
		config.MaxValueBytes < 1 || config.InboundBytes < 1 || config.OutboundBytes < 1 ||
		config.InboundPerConn < 1 || config.OutboundPerConn < 1 ||
		config.ReadTimeout <= 0 || config.WriteTimeout <= 0 ||
		config.SweepInterval <= 0 || config.SweepBatch < 1 {
		return nil, errors.New("invalid server configuration")
	}
	return &Server{
		cache:    cache,
		config:   config,
		inbound:  budget{limit: config.InboundBytes},
		outbound: budget{limit: config.OutboundBytes},
		writes:   writeBuffers{limit: min(config.InboundBytes/2, maxIdleWriteBytes)},
	}, nil
}

// Serve owns listener and its accepted connections until ctx ends or Accept fails.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	ctx, cancel := context.WithCancel(ctx)
	stopAccept := context.AfterFunc(ctx, func() { _ = listener.Close() })
	var workers sync.WaitGroup
	defer func() {
		cancel()
		stopAccept()
		_ = listener.Close()
		workers.Wait()
	}()

	slots := make(chan struct{}, s.config.MaxConnections)
	workers.Go(func() { s.sweep(ctx) })
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		select {
		case slots <- struct{}{}:
			workers.Go(func() {
				defer func() { <-slots }()
				stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
				defer stop()
				s.serveConn(conn)
			})
		default:
			_ = conn.Close()
		}
	}
}

func (s *Server) sweep(ctx context.Context) {
	tick := time.NewTicker(s.config.SweepInterval)
	defer tick.Stop()
	shard := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			s.cache.Sweep(shard, s.config.SweepBatch)
			shard = (shard + 1) % s.cache.ShardCount()
		}
	}
}

func (s *Server) serveConn(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReaderSize(conn, headerBytes)
	keyBuffer := make([]byte, s.config.MaxKeyBytes)
	reply := make([]byte, 0, 32)
	for {
		if conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout)) != nil {
			return
		}
		line, err := reader.ReadSlice('\n')
		if err != nil {
			if errors.Is(err, bufio.ErrBufferFull) {
				_ = s.writeStatus(conn, reply, "BAD_REQUEST\n")
			}
			return
		}
		req, err := parseHeader(line[:len(line)-1])
		if err != nil {
			_ = s.writeStatus(conn, reply, "BAD_REQUEST\n")
			return
		}
		if req.keyBytes > len(keyBuffer) || req.valueBytes > s.config.MaxValueBytes {
			_ = s.writeStatus(conn, reply, "TOO_LARGE\n")
			return
		}
		if !s.handle(conn, reader, keyBuffer[:req.keyBytes], reply, req) {
			return
		}
	}
}

func (s *Server) handle(conn net.Conn, reader *bufio.Reader, key, reply []byte, req request) bool {
	bufferSize := s.writes.rounded(req.valueBytes, s.config.MaxValueBytes)
	inBytes, reserved := s.reserveInbound(len(key), bufferSize)
	if !reserved && bufferSize != req.valueBytes {
		bufferSize = req.valueBytes
		inBytes, reserved = s.reserveInbound(len(key), bufferSize)
	}
	if !reserved {
		_ = s.writeStatus(conn, reply, "BUSY\n")
		return false
	}

	value, result, err := s.receive(reader, key, req, bufferSize)
	s.inbound.release(inBytes)
	if err != nil {
		return false
	}
	if result == store.Hit {
		outBytes := int64(cap(value))
		err = s.writeValue(conn, reply, value)
		value = nil
		s.outbound.release(outBytes)
		return err == nil
	}

	var response string
	switch result {
	case store.Miss:
		response = "MISS\n"
	case store.Stored:
		response = "STORED\n"
	case store.Exists:
		response = "EXISTS\n"
	case store.NoSpace:
		response = "NO_SPACE\n"
	case store.Busy:
		response = "BUSY\n"
	case store.Invalid:
		response = "BAD_REQUEST\n"
	}
	return s.writeStatus(conn, reply, response) == nil
}

func (s *Server) reserveInbound(keySize, valueSize int) (int64, bool) {
	keyBytes, valueBytes := int64(keySize), int64(valueSize)
	if valueBytes > s.config.InboundPerConn-keyBytes {
		return 0, false
	}
	total := keyBytes + valueBytes
	return total, s.inbound.reserve(total)
}

func (s *Server) receive(reader *bufio.Reader, key []byte, req request, bufferSize int) ([]byte, store.Result, error) {
	if _, err := io.ReadFull(reader, key); err != nil {
		return nil, store.Invalid, err
	}
	if req.write {
		buffer := s.writes.take(bufferSize)
		value := buffer[:req.valueBytes]
		if _, err := io.ReadFull(reader, value); err != nil {
			s.writes.put(buffer)
			return nil, store.Invalid, err
		}
		if len(value) != cap(value) {
			result := s.cache.SetCopy(key, value, req.ttl, req.onlyMiss)
			s.writes.put(buffer)
			return nil, result, nil
		}
		result := s.cache.Set(key, value, req.ttl, req.onlyMiss)
		if result != store.Stored {
			s.writes.put(buffer)
		}
		return nil, result, nil
	}
	value, result := s.cache.Get(key, func(n int64) bool {
		return n <= s.config.OutboundPerConn && s.outbound.reserve(n)
	})
	return value, result, nil
}

func (s *Server) writeStatus(conn net.Conn, reply []byte, status string) error {
	if err := conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout)); err != nil {
		return err
	}
	_, err := conn.Write(append(reply[:0], status...))
	return err
}

func (s *Server) writeValue(conn net.Conn, header, value []byte) error {
	if err := conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout)); err != nil {
		return err
	}
	header = append(header[:0], "VALUE "...)
	header = strconv.AppendInt(header, int64(len(value)), 10)
	header = append(header, '\n')
	if _, err := conn.Write(header); err != nil {
		return err
	}
	if len(value) > 0 {
		_, err := conn.Write(value)
		return err
	}
	return nil
}
