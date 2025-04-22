package iscsi

import (
	"context"
	"fmt"
	"io"
	"log/slog"
)

type writer struct {
	ctx       context.Context
	cancel    context.CancelFunc
	dev       *device
	lba       int64
	blocksize int64
}

func Writer(ctx context.Context, dev *device) (*writer, error) {
	if dev.closed.Load() {
		return nil, ErrDeviceClosed
	}

	c, err := dev.ReadCapacity16()
	if err != nil {
		return nil, fmt.Errorf("failed to get capacity of device: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)

	return &writer{
		ctx:       ctx,
		cancel:    cancel,
		dev:       dev,
		lba:       int64(c.MaxLBA) + 1,
		blocksize: int64(c.BlockSize),
	}, nil
}

func (w *writer) Close() error {
	if w.dev.closed.Load() {
		return nil
	}

	w.cancel()
	return w.dev.Disconnect()
}

func (w *writer) WriteAt(p []byte, off int64) (n int, err error) {
	size := int64(len(p))
	endOffset := off + size
	if endOffset >= w.blocksize*w.lba {
		logger().Debug("offset past at EOF", slog.Int("offset", int(off)))
		return 0, io.EOF
	}
	if len(p)%int(w.blocksize) != 0 {
		return 0, fmt.Errorf(
			"number of bytes %d not divisible by block size %d", len(p), w.blocksize,
		)
	}

	logger().Debug("WriteAt", slog.Int("bytes", len(p)), slog.Int("offset", int(off)))

	startBlock := off / w.blocksize
	blocks := int(size / w.blocksize)

	var written int
	for block := range blocks {
		lba := startBlock + int64(block)*w.blocksize

		// data offsets
		start := int64(block) * w.blocksize
		end := start + min(w.blocksize, size)

		writeErr := w.dev.Write16(w.ctx, Write16{
			LBA:       int(lba),
			BlockSize: int(w.blocksize),
			Data:      p[start:end],
		})
		if writeErr != nil {
			return written, fmt.Errorf("iscsi device write error: %w", writeErr)
		}

		written += len(p[start:end])
	}

	logger().Debug("finished write", slog.Int("length", len(p)))
	return written, err
}
