package main

import (
	"bytes"
	"context"
	"fmt"
	"time"

	iscsi "github.com/joyent/libiscsi-go"
)

func main() {
	// connect device
	dev := iscsi.New(iscsi.ConnectionDetails{
		InitiatorIQN: "iqn.2025-04.com.joyent:copy-workflow",
		TargetURL:    "iscsi://172.17.2.100:3260/iqn.2010-01.com.solidfire:vmrf.vol-8512c980af664eaa9ff74a2dd0ce03f5.2453022/0",
	})
	if err := dev.Connect(); err != nil {
		panic(fmt.Errorf("error connecting: %w", err))
	}
	defer dev.Disconnect()

	// context with 2sec timeout
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	writer, err := iscsi.Writer(ctx, dev)
	if err != nil {
		panic(fmt.Errorf("error opening writer: %w", err))
	}
	defer writer.Close()

	// we'll timeout before we finish writing
	go func() {
		for {
			t := time.Now()
			data := make([]byte, 512)
			copy(data, bytes.Repeat([]byte{1}, len(data)))

			n, err := writer.WriteAt(data, 0)
			if err != nil {
				fmt.Printf("error writing to device: %v\n", err)
				return
			}

			fmt.Printf("wrote %d @ %s\n", n, t)
		}
	}()

	// close the device; this will happen after the write times out
	time.Sleep(3 * time.Second)
	writer.Close()

	ctx = context.Background()
	if err := dev.Connect(); err != nil {
		panic(fmt.Errorf("error connecting: %w", err))
	}

	reader, err := iscsi.Reader(ctx, dev)
	if err != nil {
		panic(fmt.Errorf("error opening reader: %w", err))
	}
	defer reader.Close()

	data := make([]byte, 10)
	n, err := reader.Read(data)
	if err != nil {
		panic(fmt.Errorf("error reading: %w", err))
	}

	fmt.Printf("data (%d): %s\n", n, string(data))
}
