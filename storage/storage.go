package storage

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
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
	Reverse() error
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

func (s *storage) Reverse() error {
	filePath := filepath.Join(s.OutDir, s.FileName)
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	tmpFileName := s.FileName + ".tmp"
	tmpFile, err := os.Create(filepath.Join(s.OutDir, tmpFileName))
	r := csv.NewReader(f)
	header, err := r.Read()
	if err != nil {
		return err
	}
	w := csv.NewWriter(tmpFile)
	err = w.Write(header)
	if err != nil {
		return err
	}
	w.Flush()
	err = tmpFile.Close()
	if err != nil {
		return err
	}

	const bufferSize = 1000

	rows := make([][]string, 0, bufferSize)
	for {
		row, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if len(rows) < bufferSize {
			rows = append(rows, row)
			continue
		}
		err = reverseAndPrepend(filepath.Join(s.OutDir, tmpFileName), rows)
		rows = rows[:0]

		if err != nil {
			return err
		}
	}

	if len(rows) > 0 {
		err = reverseAndPrepend(filepath.Join(s.OutDir, tmpFileName), rows)
		if err != nil {
			return err
		}
	}

	err = f.Close()
	if err != nil {
		return err
	}

	err = os.Rename(filepath.Join(s.OutDir, s.FileName), filepath.Join(s.OutDir, s.FileName+".bak"))
	if err != nil {
		return err
	}
	err = os.Rename(filepath.Join(s.OutDir, tmpFileName), filepath.Join(s.OutDir, s.FileName))
	if err != nil {
		return err
	}
	err = os.Remove(filepath.Join(s.OutDir, fmt.Sprintf("%s.bak", s.FileName)))
	if err != nil {
		return err
	}

	return nil
}

func reverseAndPrepend(filePath string, rows [][]string) error {
	bakFilePath := filePath + ".bak"
	err := os.Rename(filePath, bakFilePath)
	if err != nil {
		return err
	}

	newFile, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	bakFile, err := os.OpenFile(bakFilePath, os.O_RDONLY, 0666)
	if err != nil {
		return err
	}

	r := csv.NewReader(bakFile)
	w := csv.NewWriter(newFile)

	header, err := r.Read()
	if err != nil {
		return err
	}
	w.Write(header)
	for i := len(rows) - 1; i >= 0; i-- {
		w.Write(rows[i])
	}
	w.Flush()
	for {
		row, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		w.Write(row)
	}
	w.Flush()

	err = bakFile.Close()
	if err != nil {
		return err
	}
	err = newFile.Close()
	if err != nil {
		return err
	}
	err = os.Remove(bakFilePath)
	if err != nil {
		return err
	}
	return nil
}

func writeHeader(f *os.File) error {
	_, err := f.WriteString("id,side,price,size,exec_date\n")
	return err
}
