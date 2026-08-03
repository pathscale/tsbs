package worktable

import (
	"time"

	"github.com/questdb/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/questdb/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/questdb/tsbs/pkg/query"
)

type BaseGenerator struct{}

func (g *BaseGenerator) GenerateEmptyQuery() query.Query {
	return query.NewWorkTable()
}

func (g *BaseGenerator) NewDevops(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := devops.NewCore(start, end, scale)
	if err != nil {
		return nil, err
	}
	return &Devops{BaseGenerator: g, Core: core}, nil
}

func fill(
	qi query.Query,
	label string,
	operation string,
	hosts []string,
	hostCount int,
	start int64,
	end int64,
	metricCount int,
	bucketNanos int64,
	limit int,
	threshold float64,
) {
	q := qi.(*query.WorkTable)
	q.HumanLabel = label
	q.HumanDescription = label
	q.Operation = operation
	q.Hosts = append(q.Hosts[:0], hosts...)
	q.HostCount = hostCount
	q.StartTimestamp = start
	q.EndTimestamp = end
	q.MetricCount = metricCount
	q.BucketNanos = bucketNanos
	q.Limit = limit
	q.Threshold = threshold
}
