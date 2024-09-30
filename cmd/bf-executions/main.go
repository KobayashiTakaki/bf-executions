package main

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/KobayashiTakaki/bf-executions/application"
	"github.com/KobayashiTakaki/bf-executions/bitflyer"
	"github.com/KobayashiTakaki/bf-executions/storage"
)

func main() {
	ctx := context.Background()
	fromDate := time.Now().Add(-24 * time.Hour)
	httpclient := &http.Client{}
	bfclient := bitflyer.NewBitflyerHTTPClient(httpclient, bitflyer.PRODUCT_CODE_FX_BTC_JPY)
	st, err := storage.NewStorage("result", "executions.csv")
	if err != nil {
		slog.Info("error in NewStorage", "error", err)
		return
	}
	collector := application.NewCollector(st, bfclient, application.OrderTypeDesc)
	err = collector.Run(ctx, fromDate)
	if err != nil {
		slog.Info("error in Run", "error", err)
		return
	}
}
