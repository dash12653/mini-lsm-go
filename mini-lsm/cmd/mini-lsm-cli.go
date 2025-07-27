package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"mini-lsm-go/mini-lsm/pkg"
	"os"
	"strconv"
	"strings"
	"time"
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
			keys, values := make([][]byte, 0), make([][]byte, 0)
			start := time.Now()
			for i := begin; i <= end; i++ {
				key := uint64ToBytes(i)
				val := []byte(fmt.Sprintf("value%d@%dvaluevaluevalue", i, epoch))
				keys = append(keys, key)
				values = append(values, val)
			}
			lsmEngine.WriteBatch(keys, values)
			duration := time.Since(start)
			fmt.Printf("%d values filled with epoch %d in %v\n", end-begin+1, epoch, duration)
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
			start := time.Now()
			val := lsmEngine.Get(key)
			duration := time.Since(start)
			if val == nil {
				fmt.Printf("%d not exist\n", keyInt)
			} else {
				fmt.Printf("%d=%s (get took %v)\n", keyInt, string(val), duration)
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
			start := time.Now()
			iter := lsmEngine.Scan(
				uint64ToBytes(startInt),
				uint64ToBytes(endInt),
			)
			cnt := 0
			for iter.Valid() {
				if len(iter.Value()) == 0 {
					iter.Next()
					continue
				}
				cnt++
				k := bytesToUint64(iter.Key().Key)
				fmt.Printf("%d=%s\n", k, string(iter.Value()))
				iter.Next()
			}
			duration := time.Since(start)
			fmt.Printf("scan cnt: %d, time: %v\n", cnt, duration)
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
			start := time.Now()
			lsmEngine.Put(key, val)
			duration := time.Since(start)
			fmt.Printf("Put key=%d value=%s in %v\n", keyInt, parts[2], duration)
		} else if line == "quit" {
			lsmEngine.Close()
			break
		} else if line == "flush" {
			lsmEngine.Force_flush()
		} else if line == "compact" {
			lsmEngine.ForceFullCompaction()
		} else if line == "dump" {
			lsmEngine.Dump()
		} else if strings.HasPrefix(line, "delete ") {
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
			lsmEngine.Put(key, []byte{})
		} else if line == "json" {
			pkg.DoJson(lsmEngine)
		} else if line == "close" {
			lsmEngine.Close()
		} else {
			fmt.Println("invalid command:", line)
		}
		epoch++
	}
}
