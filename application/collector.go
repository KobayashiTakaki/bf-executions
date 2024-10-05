package application

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/KobayashiTakaki/bf-executions/bitflyer"
	"github.com/KobayashiTakaki/bf-executions/storage"
)

type Collector struct {
	storage  storage.Storage
	bfclient bitflyer.BitflyerHTTPClient
	order    OrderType
}

func NewCollector(
	storage storage.Storage,
	bfclient bitflyer.BitflyerHTTPClient,
	order OrderType,
) *Collector {
	return &Collector{
		storage:  storage,
		bfclient: bfclient,
		order:    order,
	}
}

type OrderType string

const (
	OrderTypeAsc  OrderType = "asc"
	OrderTypeDesc OrderType = "desc"
)

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
		fmt.Printf(
			"received data at %s\n",
			executions[len(executions)-1].ExecDate.Format("2006-01-02 15:04:05.999999999"),
		)
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

	if c.order == OrderTypeDesc {
		err := c.storage.Reverse()
		if err != nil {
			slog.Info("error in Reverse", "error", err)
			return err
		}
	}
	return nil
}
