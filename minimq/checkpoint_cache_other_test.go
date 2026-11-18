//go:build !linux || (!amd64 && !arm64)

package minimq

import "errors"

func checkCheckpointCache(_ string, cold bool) (int, int, error) {
	if cold {
		return 0, 0, errors.New("verified cold-cache trials require Linux on amd64 or arm64")
	}
	return 0, 0, nil
}
