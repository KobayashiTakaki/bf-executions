package storage

import (
	"context"
	"encoding/csv"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const (
	TIME_LAYOUT = "2006-01-02T15:04:05.999999999"
)

type Storage interface {
	Standby(ctx context.Context)
	Append(executions []*Execution) error
	GetOldestExecution() *Execution
}

var _ Storage = (*storage)(nil)

type Execution struct {
	ID       int
	Side     string
	Price    float64
	Size     float64
	ExecDate time.Time
	// BuyChildOrderAcceptanceID  string
	// SellChildOrderAcceptanceID string
}

type storage struct {
	OutDir          string
	FileName        string
	oldestExecution *Execution
	standby         bool
	execCh          chan *Execution
	writer          *MergeWriter
}

func NewStorage(outDir, fileName string) (*storage, error) {
	storage := &storage{
		OutDir:   outDir,
		FileName: fileName,
	}
	err := storage.prepare()
	if err != nil {
		return nil, err
	}
	return storage, nil
}

func (s *storage) prepare() error {
	// ファイルの存在確認
	filePath := filepath.Join(s.OutDir, s.FileName)
	_, err := os.Stat(filePath)
	if err != nil {
		// 想定していないエラー
		if !os.IsNotExist(err) {
			return err
		}
		// ファイルが存在しない場合は新規作成
		f, err := os.Create(filePath)
		if err != nil {
			return err
		}

		// ヘッダー書き込み
		err = writeHeader(f)
		if err != nil {
			return err
		}

		if err := f.Close(); err != nil {
			return err
		}
	}

	// ファイルの中身を読み込んで、最古のレコードを取得
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	r := csv.NewReader(f)
	rows, err := r.ReadAll()
	if err != nil {
		return err
	}

	for i, row := range rows {
		if len(rows) < 2 {
			break
		}

		if i == 0 {
			continue
		}

		if i == len(rows)-1 {
			id, err := strconv.Atoi(row[0])
			if err != nil {
				return err
			}
			price, err := strconv.ParseFloat(row[2], 64)
			if err != nil {
				return err
			}
			size, err := strconv.ParseFloat(row[3], 64)
			if err != nil {
				return err
			}
			execDate, err := time.Parse(TIME_LAYOUT, row[4])
			if err != nil {
				return err
			}

			s.oldestExecution = &Execution{
				ID:       id,
				Side:     row[1],
				Price:    price,
				Size:     size,
				ExecDate: execDate,
			}
		}
	}

	writer := NewMergeWriter()
	ch := make(chan *Execution, 100)
	s.writer = writer
	s.execCh = ch

	return nil
}

func (s *storage) Standby(ctx context.Context) {
	outputFilePath := filepath.Join(s.OutDir, s.FileName)
	go func() {
		s.standby = true
		err := s.writer.Start(ctx, s.execCh, outputFilePath)
		if err != nil {
			slog.Info("error in StandBy", "error", err)
		}
		s.standby = false
	}()
}

func (s *storage) Append(executions []*Execution) error {
	if !s.standby {
		return errors.New("storage is not standby")
	}

	forAppend := []*Execution{}

	if s.oldestExecution == nil {
		forAppend = executions
	} else {
		for _, e := range executions {
			if e.ID < s.oldestExecution.ID {
				forAppend = append(forAppend, e)
			}
		}
	}

	if len(forAppend) == 0 {
		return nil
	}

	s.oldestExecution = forAppend[len(forAppend)-1]

	for _, e := range forAppend {
		s.execCh <- e
	}

	return nil
}

func (s *storage) GetOldestExecution() *Execution {
	return s.oldestExecution
}

func writeHeader(f *os.File) error {
	_, err := f.WriteString("id,side,price,size,exec_date\n")
	return err
}
