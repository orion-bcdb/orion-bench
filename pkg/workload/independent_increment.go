package workload

import "orion-bench/pkg/material"

type IndependentIncrement struct {
	commonWorkload
}

func (w *IndependentIncrement) Init() {
	w.config.DB()
	tx, err := w.config.UserSession(material.Admin).UsersTx()
	w.Check(err)
	w.Check(tx.Abort())
}

func (w *IndependentIncrement) Run(worker int) {

}
