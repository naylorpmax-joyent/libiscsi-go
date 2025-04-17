package iscsi

import (
	"fmt"
	"io"
	"log/slog"
)

type writer struct {
	dev       *device
	lba       int64
	offset    int64
	blocksize int64
}

func Writer(dev *device) (*writer, error) {
	c, err := dev.ReadCapacity16()
	if err != nil {
		return nil, fmt.Errorf("failed to get capacity of device: %w", err)
	}
	return &writer{
		dev:       dev,
		lba:       int64(c.MaxLBA) + 1,
		offset:    0,
		blocksize: int64(c.BlockSize),
	}, nil
}

func (w *writer) Close() error {
	return w.dev.Disconnect()
}

func (w *writer) WriteAt(p []byte, off int64) (n int, err error) {
	if off >= w.blocksize*w.lba {
		logger().Debug("offset past at EOF", slog.Int("offset", int(off)))
		return 0, io.EOF
	}
	logger().Debug("WriteAt", slog.Int("bytes", len(p)), slog.Int("offset", int(off)))
	// find our starting lba
	startBlock := off / w.blocksize
	endOffset := len(p) + int(off)
	blocks := (endOffset-int(off))/int(w.blocksize) + 1
	blocks = min(blocks, int(w.lba)-int(startBlock))

	// handle EOF
	if (endOffset/int(w.blocksize)) > int(w.lba) || blocks == int(w.lba-startBlock) {
		err = io.EOF
		logger().Debug("reached EOF", slog.Int("lba", int(w.lba)), slog.Int("endOffset", endOffset))
		blocks = int(w.lba - startBlock)
	} else if endOffset%int(w.blocksize) != 0 {
		// if endoffset is not block aligned then we need to read one more block
		blocks++
	}

	blocks = min(blocks, int(w.lba)-int(startBlock))
	for block := range blocks {
		start := startBlock * int64(block)
		end := min(start+w.blocksize, int64(len(p)))

		writeErr := w.dev.Write16(Write16{
			LBA:       int(start),
			BlockSize: int(end - start),
			Data:      p[start:end],
		})
		if writeErr != nil {
			return 0, fmt.Errorf("iscsi device write error: %w", writeErr)
		}
	}

	logger().Debug("finished write", slog.Int("length", len(p)))
	return len(p), err
}
