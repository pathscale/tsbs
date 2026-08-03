package query

import (
	"fmt"
	"sync"
)

// WorkTable encodes an execution plan rather than SQL. It is serialized as
// JSON Lines so the Rust runner can consume exactly the query stream generated
// by TSBS.
type WorkTable struct {
	HumanLabel       string   `json:"human_label"`
	HumanDescription string   `json:"human_description"`
	Operation        string   `json:"operation"`
	Hosts            []string `json:"hosts,omitempty"`
	HostCount        int      `json:"host_count,omitempty"`
	StartTimestamp   int64    `json:"start_timestamp,omitempty"`
	EndTimestamp     int64    `json:"end_timestamp,omitempty"`
	MetricCount      int      `json:"metric_count,omitempty"`
	BucketNanos      int64    `json:"bucket_nanos,omitempty"`
	Limit            int      `json:"limit,omitempty"`
	Threshold        float64  `json:"threshold,omitempty"`
	id               uint64
}

var WorkTablePool = sync.Pool{
	New: func() interface{} { return &WorkTable{} },
}

func NewWorkTable() *WorkTable {
	return WorkTablePool.Get().(*WorkTable)
}

func (q *WorkTable) GetID() uint64 {
	return q.id
}

func (q *WorkTable) SetID(n uint64) {
	q.id = n
}

func (q *WorkTable) String() string {
	return fmt.Sprintf("HumanLabel: %q, Operation: %q, Hosts: %v, Start: %d, End: %d", q.HumanLabel, q.Operation, q.Hosts, q.StartTimestamp, q.EndTimestamp)
}

func (q *WorkTable) HumanLabelName() []byte {
	return []byte(q.HumanLabel)
}

func (q *WorkTable) HumanDescriptionName() []byte {
	return []byte(q.HumanDescription)
}

func (q *WorkTable) Release() {
	q.HumanLabel = ""
	q.HumanDescription = ""
	q.Operation = ""
	q.Hosts = q.Hosts[:0]
	q.HostCount = 0
	q.StartTimestamp = 0
	q.EndTimestamp = 0
	q.MetricCount = 0
	q.BucketNanos = 0
	q.Limit = 0
	q.Threshold = 0
	q.id = 0
	WorkTablePool.Put(q)
}
