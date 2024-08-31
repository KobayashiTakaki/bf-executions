package storage

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type MergeWriter struct{}

func NewMergeWriter() *MergeWriter {
	return &MergeWriter{}
}

func (w *MergeWriter) Start(ctx context.Context, ch chan *Execution, outputFilePath string) error {
	f, err := os.OpenFile(filepath.Join(outputFilePath), os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	defer f.Close()

	var merged *Execution
	for {
		select {
		case <-ctx.Done():
			return writeExecution(f, merged)
		case exec := <-ch:
			if exec == nil {
				return nil
			}
			if merged == nil {
				merged = exec
			} else if merged.Side == exec.Side &&
				(merged.ExecDate.Equal(exec.ExecDate) || merged.Price == exec.Price) {
				merged = &Execution{
					ID:       merged.ID,
					Side:     merged.Side,
					Price:    merged.Price,
					Size:     merged.Size + exec.Size,
					ExecDate: merged.ExecDate,
				}
			} else {
				err := writeExecution(f, merged)
				if err != nil {
					return err
				}
				merged = exec
			}
		}
	}
}

func writeExecution(f *os.File, e *Execution) error {
	record := []string{
		strconv.Itoa(e.ID),
		e.Side,
		strconv.FormatFloat(e.Price, 'f', -1, 64),
		strconv.FormatFloat(e.Size, 'f', -1, 64),
		e.ExecDate.Format(TIME_LAYOUT),
	}
	row := strings.Join(record, ",") + "\n"
	_, err := f.WriteString(row)
	if err != nil {
		return err
	}
	return nil
}
