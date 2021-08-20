package crdt

import (
	"time"
)

type Element struct {
	Value     interface{}
	timestamp time.Time
}
