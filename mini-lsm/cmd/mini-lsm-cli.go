package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"mini-lsm-go/mini-lsm/pkg"
	"os"
	"strconv"
	"strings"
)

func uint64ToBytes(num uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, num)
	return b
}

func bytesToUint64(b []byte) uint64 {
	if len(b) < 8 {
		return 0 // 或者报错
	}
	return binary.BigEndian.Uint64(b)
}

func main() {
	lsmEngine := pkg.Open()

	epoch := 0
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print(">>> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())

		if strings.HasPrefix(line, "fill ") {
			parts := strings.Split(line, " ")
			if len(parts) != 3 {
				fmt.Println("invalid command")
				continue
			}
			begin, err1 := strconv.ParseUint(parts[1], 10, 64)
			end, err2 := strconv.ParseUint(parts[2], 10, 64)
			if err1 != nil || err2 != nil {
				fmt.Println("invalid range")
				continue
			}
			for i := begin; i <= end; i++ {
				key := uint64ToBytes(i)
				val := []byte(fmt.Sprintf("value%d@%dvaluevaluevalue", i, epoch))
				lsmEngine.Put(key, val)
			}
			fmt.Printf("%d values filled with epoch %d\n", end-begin+1, epoch)
		} else if strings.HasPrefix(line, "get ") {
			parts := strings.SplitN(line, " ", 2)
			if len(parts) != 2 {
				fmt.Println("invalid command")
				continue
			}
			keyInt, err := strconv.ParseUint(parts[1], 10, 64)
			if err != nil {
				fmt.Println("invalid key")
				continue
			}
			key := uint64ToBytes(keyInt)
			val := lsmEngine.Get(key)
			if val == nil {
				fmt.Printf("%d not exist\n", keyInt)
			} else {
				fmt.Printf("%d=%s\n", keyInt, string(val))
			}
		} else if strings.HasPrefix(line, "scan ") {
			parts := strings.SplitN(line, " ", 3)
			if len(parts) != 3 {
				fmt.Println("invalid command")
				continue
			}
			startInt, err1 := strconv.ParseUint(parts[1], 10, 64)
			endInt, err2 := strconv.ParseUint(parts[2], 10, 64)
			if err1 != nil || err2 != nil {
				fmt.Println("invalid range")
				continue
			}
			iter := lsmEngine.Scan(
				uint64ToBytes(startInt),
				uint64ToBytes(endInt),
			)
			cnt := 0
			for iter.Valid() {
				cnt++
				k := bytesToUint64(iter.Key())
				fmt.Printf("%d=%s\n", k, string(iter.Value()))
				iter.Next()
			}
			fmt.Println("scan cnt: ", cnt)
		} else if strings.HasPrefix(line, "put ") {
			parts := strings.SplitN(line, " ", 3)
			if len(parts) != 3 {
				fmt.Println("invalid command")
				continue
			}
			keyInt, err := strconv.ParseUint(parts[1], 10, 64)
			if err != nil {
				fmt.Println("invalid key")
				continue
			}
			key := uint64ToBytes(keyInt)
			val := []byte(parts[2])
			lsmEngine.Put(key, val)
			fmt.Printf("Put key=%d value=%s\n", keyInt, parts[2])
		} else if line == "quit" {
			lsmEngine.Close()
			break
		} else if line == "flush" {
			lsmEngine.Force_flush()
		} else if line == "compact" {
			lsmEngine.ForceFullCompaction()
		} else if line == "dump" {
			lsmEngine.Dump()
		} else {
			fmt.Println("invalid command:", line)
		}
		epoch++
	}
}
