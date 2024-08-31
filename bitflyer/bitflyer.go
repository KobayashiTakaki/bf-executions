package bitflyer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"time"
)

const (
	BASE_URL                = "https://api.bitflyer.com"
	PRODUCT_CODE_FX_BTC_JPY = "FX_BTC_JPY"
	RESP_TIME_LAYOUT        = "2006-01-02T15:04:05.999999999"
)

type BitflyerHTTPClient interface {
	GetExecutions(ctx context.Context, count int, before int, after int) ([]*Execution, error)
}

type bitflyerHTTPClient struct {
	httpclient  *http.Client
	productCode string
}

func NewBitflyerHTTPClient(
	httpclient *http.Client,
	productCode string,
) *bitflyerHTTPClient {
	return &bitflyerHTTPClient{
		httpclient:  httpclient,
		productCode: PRODUCT_CODE_FX_BTC_JPY,
	}
}

func (c *bitflyerHTTPClient) GetExecutions(ctx context.Context, count int, before int, after int) ([]*Execution, error) {
	const apiPath = "/v1/getexecutions"
	u, err := url.Parse(BASE_URL)
	if err != nil {
		return nil, err
	}
	u.Path = path.Join(u.Path, apiPath)
	q := u.Query()
	q.Set("product_code", c.productCode)
	if count != 0 {
		q.Set("count", strconv.Itoa(count))
	}
	if before != 0 {
		q.Set("before", strconv.Itoa(before))
	}
	if after != 0 {
		q.Set("after", strconv.Itoa(after))
	}
	u.RawQuery = q.Encode()
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpclient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		buf := new(bytes.Buffer)
		buf.ReadFrom(resp.Body)
		slog.Error("status code is not 200", "status code", resp.StatusCode, "body", buf.String())
		return nil, fmt.Errorf("status code is not 200. status code: %d", resp.StatusCode)
	}
	type respExecution struct {
		ID                         int     `json:"id"`
		Side                       string  `json:"side"`
		Price                      float64 `json:"price"`
		Size                       float64 `json:"size"`
		ExecDate                   string  `json:"exec_date"`
		BuyChildOrderAcceptanceID  string  `json:"buy_child_order_acceptance_id"`
		SellChildOrderAcceptanceID string  `json:"sell_child_order_acceptance_id"`
	}
	var executions []*respExecution
	err = json.NewDecoder(resp.Body).Decode(&executions)
	if err != nil {
		return nil, err
	}
	ret := make([]*Execution, 0, len(executions))
	for _, e := range executions {
		t, err := time.Parse(RESP_TIME_LAYOUT, e.ExecDate)
		if err != nil {
			return nil, err
		}
		ret = append(ret, &Execution{
			ID:       e.ID,
			Side:     e.Side,
			Price:    e.Price,
			Size:     e.Size,
			ExecDate: t,
		})
	}

	return ret, nil
}

type Execution struct {
	ID       int
	Side     string
	Price    float64
	Size     float64
	ExecDate time.Time
	// BuyChildOrderAcceptanceID  string
	// SellChildOrderAcceptanceID string
}
