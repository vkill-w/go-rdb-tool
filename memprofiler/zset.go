package memprofiler

import (
	"github.com/vkill-w/go-rdb-tool/model"
	"math/rand"
)

func skipListOverhead(size int) int {
	return 2*sizeOfPointer() + hashtableOverhead(size) + (2*sizeOfPointer() + 16)
}

func skipListEntryOverhead() int {
	return hashTableEntryOverhead() + 2*sizeOfPointer() + 8 + (sizeOfPointer()+8)*zsetRandomLevel()
}

func zsetRandomLevel() int {
	const maxLevel = 32
	const p = 0.25
	i := 1
	r := rand.Intn(0xFFFF)
	for r < 0xFFFF/4 {
		i++
		r = rand.Intn(0xFFFF)
		if i >= maxLevel {
			return maxLevel
		}
	}
	return i
}

func sizeOfZSetObject(o *model.ZSetObject) int {
	if o.GetEncoding() == model.ZipListEncoding {
		extra := o.Extra.(*model.ZiplistDetail)
		return extra.RawStringSize
	}
	size := skipListOverhead(len(o.Entries))
	for _, entry := range o.Entries {
		size += sizeOfString(entry.Member) + 8 + skipListEntryOverhead() // size of score is 8 (double)
	}
	return size
}
