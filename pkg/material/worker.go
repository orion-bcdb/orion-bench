package material

import (
	"fmt"

	"orion-bench/pkg/types"

	"github.com/hyperledger-labs/orion-server/pkg/logger"
)

type WorkerMaterial struct {
	lg             *logger.SugarLogger
	Rank           uint64
	Address        string
	PrometheusPort types.Port
}

func (w *WorkerMaterial) PrometheusServeAddress() string {
	return fmt.Sprintf("0.0.0.0:%d", w.PrometheusPort)
}

func (w *WorkerMaterial) PrometheusTargetAddress() string {
	return fmt.Sprintf("%s:%d", w.Address, w.PrometheusPort)
}
