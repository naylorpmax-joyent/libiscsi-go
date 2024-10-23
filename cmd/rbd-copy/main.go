package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ceph/go-ceph/rados"
	"github.com/ceph/go-ceph/rbd"
	iscsi "github.com/willgorman/libiscsi-go"
)

type reader struct {
	dev       dev
	blockSize int
	blocks    int
}

type dev interface {
	Read16(data iscsi.Read16) ([]byte, error)
	Disconnect() error
}

func New(initiatorIQN, targetURL string) (*reader, error) {
	d := iscsi.New(iscsi.ConnectionDetails{
		InitiatorIQN: initiatorIQN,
		TargetURL:    targetURL,
	})
	if err := d.Connect(); err != nil {
		return nil, err
	}
	cap, err := d.ReadCapacity10()
	if err != nil {
		return nil, err
	}
	return &reader{
		dev:       d,
		blockSize: cap.BlockSize,
		blocks:    cap.LBA,
	}, nil
}

func (r *reader) Close() error {
	return r.dev.Disconnect()
}

func (r *reader) Read(p []byte) (n int, err error) {
	panic("not implemented") // TODO: Implement
}

func (r *reader) ReadAt(p []byte, off int64) (n int, err error) {
	start := off / int64(r.blockSize)
	if start > int64(r.blocks) {
		return 0, errors.New("out of bounds")
	}
	blocks := (len(p) / r.blockSize) + 1
	// log.Println("blocks ", blocks)
	attempts := 0
read16:
	readbytes, err := r.dev.Read16(iscsi.Read16{
		LBA:       int(start),
		Blocks:    blocks,
		BlockSize: r.blockSize,
	})
	attempts++
	if err != nil {
		if err.Error() == "Poll failed" && attempts < 10 {
			log.Println("try again")
			goto read16
		}
		return 0, err // TODO: return the bytes read anyway?
	}
	// FIXME: (willgorman) handle non block aligned reads and reads of partial blocks
	// startIndex := off % int64(r.blockSize)
	// endIndex := r.blockSize - int(startIndex)
	// p = readbytes[startIndex:endIndex]

	return copy(p, readbytes), nil
}

func main() {
	if len(os.Args) != 7 {
		panic("missing required args")
	}
	initiatorIQN := os.Args[1]
	targetURL := os.Args[2]
	poolName := os.Args[3]
	imageName := os.Args[4]
	monitors := os.Args[5]
	key := os.Args[6]

	conn, err := cephConnFromOptions(monitors, key)
	if err != nil {
		log.Fatal(err)
	}

	ioctx, err := conn.OpenIOContext(poolName)
	if err != nil {
		log.Fatal(err)
	}
	defer ioctx.Destroy()

	img, err := rbd.OpenImage(ioctx, imageName, rbd.NoSnapshot)
	if err != nil {
		log.Fatal(err)
	}
	defer img.Close()

	reader, err := New(initiatorIQN, targetURL)
	if err != nil {
		log.Fatal(err)
	}
	start := time.Now()
	for i := 0; i < (reader.blockSize * reader.blocks); i++ {
		if i%(1024) == 0 {
			log.Println("offset ", i)
		}

		thebytes := make([]byte, reader.blockSize*2048)
		_, err := reader.ReadAt(thebytes, int64(i))
		if err != nil {
			log.Fatal(err)
		}
		// FIXME: (willgorman) I think we have to check for ranges of empty blocks
		// otherwise we write zeros to the target.  that may be fine but it seems
		// like it makes Ceph count that space as used
		if _, err = img.WriteAt(thebytes, int64(i)); err != nil {
			log.Fatal("writing", err)
		}
		i = i + (reader.blockSize * 2048)
	}
	log.Printf("took %s", time.Since(start))

	// img2, err := rbd.OpenImage(ioctx, imageName, rbd.NoSnapshot)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer img2.Close()

	// for i := 0; i < 10; i++ {
	// 	thebytes := make([]byte, reader.blockSize)
	// 	_, err := img2.ReadAt(thebytes, int64(i*reader.blockSize))
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// }

	// fmt.Println("got the image boss")
}

func cephConnFromConfig(confPath, keyringPath string) (*rados.Conn, error) {
	conn, err := rados.NewConnWithUser("admin")
	if err != nil {
		return nil, fmt.Errorf("unable to create ceph connection: %w", err)
	}

	if err := conn.ReadConfigFile(confPath); err != nil {
		return nil, fmt.Errorf("error reading ceph config file: %w", err)
	}
	if err := conn.ReadConfigFile(keyringPath); err != nil {
		return nil, fmt.Errorf("error reading ceph keyring file: %w", err)
	}

	if err := conn.Connect(); err != nil {
		return nil, fmt.Errorf("error connecting to ceph: %w", err)
	}
	return conn, nil
}

func cephConnFromOptions(monitors, key string) (*rados.Conn, error) {
	conn, err := rados.NewConnWithUser("admin")
	if err != nil {
		return nil, fmt.Errorf("unable to create ceph connection: %w", err)
	}

	if err := conn.SetConfigOption("mon_host", monitors); err != nil {
		return nil, fmt.Errorf("error setting mon_host: %w", err)
	}
	if err := conn.SetConfigOption("key", key); err != nil {
		return nil, fmt.Errorf("error setting key: %w", err)
	}

	if err := conn.Connect(); err != nil {
		return nil, fmt.Errorf("error connecting to ceph: %w", err)
	}
	return conn, nil
}
