package iscsi_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"os"
	"testing"

	iscsi "github.com/willgorman/libiscsi-go"
	"gotest.tools/assert"
)

func TestRead(t *testing.T) {
	fileName := writeTargetTempfile(t, 4*KiB)
	file, err := os.Open(fileName)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		log.Fatal(err)
	}
	fileChecksum := fmt.Sprintf("%x", hash.Sum(nil))
	t.Log("FILE CHECKSUM", fileChecksum)

	device := iscsi.New(iscsi.ConnectionDetails{
		InitiatorIQN: "iqn.2024-10.libiscsi:go",
		TargetURL:    runTestTarget(t, fileName),
	})

	err = device.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = device.Disconnect()
	}()

	sreader, err := iscsi.Reader(device)
	if err != nil {
		t.Fatal(err)
	}
	log.Printf("%#v", sreader)
	hash = sha256.New()
	if _, err := io.Copy(hash, sreader); err != nil {
		log.Fatal(err)
	}
	iscsiChecksum := fmt.Sprintf("%x", hash.Sum(nil))
	t.Log("ISCSI CHECKSUM ", iscsiChecksum)
	assert.Equal(t, fileChecksum, iscsiChecksum)
}

// TODO: (willgorman) in order to test non block aligned reads
// we can have a file io.Reader and iscsi io.Reader and
// read randomly sized []byte from them and assert that we always get
// the same values from each

func TestReadRandom(t *testing.T) {
	fileName := writeTargetTempfile(t, 5*MiB)
	file, err := os.Open(fileName)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	device := iscsi.New(iscsi.ConnectionDetails{
		InitiatorIQN: "iqn.2024-10.libiscsi:go",
		TargetURL:    runTestTarget(t, fileName),
	})

	err = device.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = device.Disconnect()
	}()

	sreader, err := iscsi.Reader(device)
	if err != nil {
		t.Fatal(err)
	}

	var fileErr, scsiErr error
	var fileN, scsiN int
	for fileErr != io.EOF && scsiErr != io.EOF {
		t.Log("HERE")
		n := rand.IntN(32 * KiB)
		fileBytes := make([]byte, n)
		scsiBytes := make([]byte, n)
		fileN, fileErr = file.Read(fileBytes)
		if fileErr != nil && fileErr != io.EOF {
			t.Fatal(err)
		}
		scsiN, scsiErr = sreader.Read(scsiBytes)
		if scsiErr != nil && scsiErr != io.EOF {
			t.Fatal(err)
		}
		assert.Equal(t, fileN, scsiN)
		assert.Assert(t, bytes.Compare(fileBytes, scsiBytes) == 0)
	}
}
