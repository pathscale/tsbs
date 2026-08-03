package worktable

import (
	"fmt"

	"github.com/blagojts/viper"
	"github.com/questdb/tsbs/pkg/data/serialize"
	"github.com/questdb/tsbs/pkg/data/source"
	"github.com/questdb/tsbs/pkg/targets"
	"github.com/questdb/tsbs/pkg/targets/constants"
	"github.com/questdb/tsbs/pkg/targets/questdb"
	"github.com/spf13/pflag"
)

// Target emits Influx Line Protocol because the WorkTable runner consumes the
// same deterministic, line-oriented CPU input as QuestDB. The runner is an
// in-process Rust binary, so the generic network loader is intentionally not
// implemented.
type Target struct{}

func NewTarget() targets.ImplementedTarget {
	return &Target{}
}

func (t *Target) TargetSpecificFlags(string, *pflag.FlagSet) {}

func (t *Target) TargetName() string {
	return constants.FormatWorkTable
}

func (t *Target) Serializer() serialize.PointSerializer {
	return &questdb.Serializer{}
}

func (t *Target) Benchmark(string, *source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	return nil, fmt.Errorf("WorkTable uses the tsbs_run_worktable in-process loader")
}
