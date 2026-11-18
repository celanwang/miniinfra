package server

import (
	"bytes"
	"errors"
	"math"
	"strconv"
	"time"
)

const headerBytes = 128

type request struct {
	keyBytes   int
	valueBytes int
	ttl        time.Duration
	write      bool
	onlyMiss   bool
}

var errHeader = errors.New("invalid request header")

func parseHeader(line []byte) (request, error) {
	var fields [5][]byte
	n := 0
	for field := range bytes.SplitSeq(line, []byte(" ")) {
		if len(field) == 0 || n == len(fields) {
			return request{}, errHeader
		}
		fields[n] = field
		n++
	}
	if n < 2 {
		return request{}, errHeader
	}
	key, err := strconv.ParseUint(string(fields[1]), 10, strconv.IntSize-1)
	if err != nil || key == 0 {
		return request{}, errHeader
	}
	req := request{keyBytes: int(key)}
	switch string(fields[0]) {
	case "GET":
		if n == 2 {
			return req, nil
		}
	case "SET":
		if n < 4 || (n == 5 && string(fields[4]) != "NX") {
			return request{}, errHeader
		}
		value, err := strconv.ParseUint(string(fields[2]), 10, strconv.IntSize-1)
		if err != nil {
			return request{}, errHeader
		}
		if string(fields[3]) != "NONE" {
			ms, err := strconv.ParseUint(string(fields[3]), 10, 64)
			if err != nil || ms == 0 || ms > uint64(math.MaxInt64/int64(time.Millisecond)) {
				return request{}, errHeader
			}
			req.ttl = time.Duration(ms) * time.Millisecond
		}
		req.write = true
		req.valueBytes = int(value)
		req.onlyMiss = n == 5
		return req, nil
	}
	return request{}, errHeader
}
