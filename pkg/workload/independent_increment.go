package workload

type IndependentIncrement struct {
	commonWorkload
}

func (w *IndependentIncrement) Init() {
	w.CreateTable("counters")
	w.AddUsers("counters")
}

func (w *IndependentIncrement) Run(worker int) {
}
