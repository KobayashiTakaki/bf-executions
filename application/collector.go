package application

import (
	"context"
	"log/slog"
	"time"

	"github.com/KobayashiTakaki/bf-executions/bitflyer"
	"github.com/KobayashiTakaki/bf-executions/storage"
)

type Collector struct {
	storage  storage.Storage
	bfclient bitflyer.BitflyerHTTPClient
}

func NewCollector(
	storage storage.Storage,
	bfclient bitflyer.BitflyerHTTPClient,
) *Collector {
	return &Collector{
		storage:  storage,
		bfclient: bfclient,
	}
}

func (c *Collector) Run(ctx context.Context, fromDate time.Time) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c.storage.Standby(ctx)

	var (
		before int
	)

	for {
		oldestExec := c.storage.GetOldestExecution()
		if oldestExec != nil {
			if oldestExec.ExecDate.Before(fromDate) {
				break
			}
			before = oldestExec.ID
		}
		executions, err := c.bfclient.GetExecutions(ctx, 500, before, 0)
		if err != nil {
			slog.Info("error in GetExecutions", "error", err)
			return err
		}
		stExecutions := make([]*storage.Execution, 0, len(executions))
		for _, e := range executions {
			stExecutions = append(stExecutions, &storage.Execution{
				ID:       e.ID,
				Side:     e.Side,
				Price:    e.Price,
				Size:     e.Size,
				ExecDate: e.ExecDate,
			})
		}
		err = c.storage.Append(stExecutions)
		if err != nil {
			slog.Info("error in Append", "error", err)
			return err
		}

		time.Sleep(time.Millisecond * 1000)
	}
	return nil
}
