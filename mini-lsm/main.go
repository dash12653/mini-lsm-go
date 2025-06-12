package main

import (
	"fmt"
	"mini-lsm-go/mini-lsm/src"
)

func main() {
	// 创建 MemTable 实例
	memTable := src.NewMemTable(0)

	// 插入元素
	memTable.Put(3, []byte("312"))
	// 查找元素
	if elem, ok := memTable.Get(3); ok {
		fmt.Println("Find element:", string(elem))
	} else {
		fmt.Println("Element not found")
	}

	memTable.Put(3, []byte("123"))

	// 查找元素
	if elem, ok := memTable.Get(3); ok {
		fmt.Println("Find element:", string(elem))
	} else {
		fmt.Println("Element not found")
	}
}
