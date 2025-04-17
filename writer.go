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
	size := int64(len(p))
	endOffset := off + size
	if endOffset >= w.blocksize*w.lba {
		logger().Debug("offset past at EOF", slog.Int("offset", int(off)))
		return 0, io.EOF
	}

	logger().Debug("WriteAt", slog.Int("bytes", len(p)), slog.Int("offset", int(off)))

	startBlock := off / w.blocksize
	blocks := int(size / w.blocksize)
	if len(p)%int(w.blocksize) != 0 {
		blocks++ // need an extra block if there's some leftover data + padding
	}
	blocks = min(blocks, int(w.lba-startBlock))

	var written int
	for block := range blocks {
		lba := startBlock + int64(block)*w.blocksize

		// data offsets
		start := int64(block) * w.blocksize
		end := start + min(w.blocksize, size)

		// pad data if it's smaller than a block
		data := p[start:end]

		short := len(data) % int(w.blocksize)
		if short != 0 {
			data = make([]byte, w.blocksize)
			copy(data, p[start:end])

			end += int64(short)
		}

		writeErr := w.dev.Write16(Write16{
			LBA:       int(lba),
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
