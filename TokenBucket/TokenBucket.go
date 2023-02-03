package TokenBucket

import (
	config "github.com/c-danil0o/NASP/Config"
	"time"
)

type TokenBucket struct {
	lastRequest time.Time
	numberOfReq int
}

func CreateTokenBucket() TokenBucket {
	return TokenBucket{lastRequest: time.Now(), numberOfReq: config.REQUEST_PERMIN}
}

func IsReady(bucket TokenBucket) bool {
	var ready = false
	currentTime := time.Now()
	endTime := bucket.lastRequest.Add(time.Minute)

	if bucket.lastRequest.Before(currentTime) && endTime.After(currentTime) {
		if bucket.numberOfReq == 0 {
			ready = false
		} else {
			bucket.numberOfReq = bucket.numberOfReq - 1
			ready = true
		}
	} else {
		bucket.numberOfReq = config.REQUEST_PERMIN
		bucket.lastRequest = time.Now()
		ready = true
	}

	return ready
}
