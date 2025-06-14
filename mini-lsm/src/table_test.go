package src

import (
	"fmt"
	"testing"
)

func TestBlockMetaCodec(t *testing.T) {
	metas := []BlockMeta{
		{Offset: 0, First_key: []byte("a"), Last_key: []byte("c")},
		{Offset: 100, First_key: []byte("d"), Last_key: []byte("f")},
	}

	encoded := EncodeBlockMeta(metas)
	decoded, err := DecodeBlockMeta(encoded)
	if err != nil {
		panic(err)
	}

	for i, meta := range decoded {
		fmt.Printf("Meta #%d: Offset=%d, FirstKey=%s, LastKey=%s\n",
			i, meta.Offset, string(meta.First_key), string(meta.Last_key))
	}
}
