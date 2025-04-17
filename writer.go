package iscsi

import (
	"errors"
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

var ErrMaxLBA = errors.New("sorry mate you've gone too far")

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
	var (
		size      = int64(len(p))
		endOffset = size + off

		startBlock = off / w.blocksize
		blocks     = int(size / w.blocksize)
	)
	if len(p)%int(w.blocksize) != 0 {
		blocks++ // need an extra block if there's some leftover data + padding
	}
	blocks = min(blocks, int(w.lba-startBlock))

	logger().Debug(fmt.Sprintf("blocks: %d, start-end: %d-%d, remainder=%d",
		blocks, off, endOffset, len(p)%int(w.blocksize)))

	var written int
	for block := range blocks {
		start := off + int64(block)*w.blocksize
		end := start + min(w.blocksize, size)

		logger().Debug(fmt.Sprintf("startBlock=%d block=%d blocks=%d",
			startBlock, block, blocks))

		// pad data if it's smaller than a block
		data := p[start:end]
		logger().Debug(fmt.Sprintf("start=%d end=%d len=%d - before adjustments",
			start, end, len(data)))

		short := len(data) % int(w.blocksize)
		if short != 0 {
			data = make([]byte, w.blocksize)
			copy(data, p[start:end])

			end += int64(short)
		}

		logger().Debug(fmt.Sprintf("start=%d end=%d len=%d - after adjustments",
			start, end, len(data)))

		writeErr := w.dev.Write16(Write16{
			LBA:       int(start),
			BlockSize: int(w.blocksize),
			Data:      data,
		})
		if writeErr != nil {
			return written, fmt.Errorf("iscsi device write error: %w", writeErr)
		}

		written += len(data)
	}

	logger().Debug("finished write", slog.Int("length", len(p)))
	return written, err
}
