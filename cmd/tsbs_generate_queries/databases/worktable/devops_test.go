package worktable

import (
	"testing"
	"time"

	"github.com/questdb/tsbs/pkg/query"
)

func TestSingleGroupByPlan(t *testing.T) {
	base := &BaseGenerator{}
	generator, err := base.NewDevops(
		time.Unix(0, 0),
		time.Unix(0, int64(24*time.Hour)),
		10,
	)
	if err != nil {
		t.Fatal(err)
	}
	q := query.NewWorkTable()
	generator.(*Devops).GroupByTime(q, 1, 5, time.Hour)
	if q.Operation != "single-groupby" || q.MetricCount != 5 || len(q.Hosts) != 1 {
		t.Fatalf("unexpected plan: %+v", q)
	}
	q.Release()
}
