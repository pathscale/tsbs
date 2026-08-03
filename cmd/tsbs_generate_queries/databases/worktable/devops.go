package worktable

import (
	"fmt"
	"time"

	"github.com/questdb/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/questdb/tsbs/pkg/query"
)

type Devops struct {
	*BaseGenerator
	*devops.Core
}

func (d *Devops) GroupByTime(qi query.Query, nHosts, numMetrics int, duration time.Duration) {
	interval := d.Interval.MustRandWindow(duration)
	hosts, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)
	label := fmt.Sprintf(
		"WorkTable %d cpu metric(s), random %4d hosts, random %s by 1m",
		numMetrics,
		nHosts,
		duration,
	)
	fill(qi, label, "single-groupby", hosts, 0, interval.StartUnixNano(), interval.EndUnixNano(), numMetrics, int64(time.Minute), 0, 0)
}

func (d *Devops) MaxAllCPU(qi query.Query, nHosts int, duration time.Duration) {
	interval := d.Interval.MustRandWindow(duration)
	hosts, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)
	label := devops.GetMaxAllLabel("WorkTable", nHosts)
	fill(qi, label, "max-all", hosts, 0, interval.StartUnixNano(), interval.EndUnixNano(), devops.GetCPUMetricsLen(), int64(time.Hour), 0, 0)
}

func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)
	label := devops.GetDoubleGroupByLabel("WorkTable", numMetrics)
	fill(qi, label, "double-groupby", nil, d.Scale, interval.StartUnixNano(), interval.EndUnixNano(), numMetrics, int64(time.Hour), 0, 0)
}

func (d *Devops) GroupByOrderByLimit(qi query.Query) {
	interval := d.Interval.MustRandWindow(time.Hour)
	const limit = 5
	fill(qi, "WorkTable max cpu over last 5 min-intervals (random end)", "groupby-orderby-limit", nil, d.Scale, 0, interval.EndUnixNano(), 1, int64(time.Minute), limit, 0)
}

func (d *Devops) LastPointPerHost(qi query.Query) {
	fill(qi, "WorkTable last row per host", "lastpoint", nil, d.Scale, 0, 0, devops.GetCPUMetricsLen(), 0, 0, 0)
}

func (d *Devops) HighCPUForHosts(qi query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.HighCPUDuration)
	var hosts []string
	if nHosts > 0 {
		var err error
		hosts, err = d.GetRandomHosts(nHosts)
		panicIfErr(err)
	}
	label, err := devops.GetHighCPULabel("WorkTable", nHosts)
	panicIfErr(err)
	fill(qi, label, "high-cpu", hosts, d.Scale, interval.StartUnixNano(), interval.EndUnixNano(), devops.GetCPUMetricsLen(), 0, 0, 90.0)
}

func panicIfErr(err error) {
	if err != nil {
		panic(err.Error())
	}
}
