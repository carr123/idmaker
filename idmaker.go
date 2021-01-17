package idmaker

import (
	"fmt"
	"sync"
	"time"
)

type IDMaker struct {
	mu        sync.Mutex
	nodeshift int64
	time      int64
	step      int64
}

var (
	tmBegin = time.Unix(1587859944, 0) //2020-04-26 08:12:24
)

//递增ID生成器. 生成规则: 时间戳(毫秒)+nodeid+计数器。不同时间不同nodeid生成的ID不同.
//时间最大值: 2055-02-28 04:06:11, 超过这个时间不能再用。

//参数: nodeid范围[0, 2047], nodeid用于区分不同的ID生成器,确保不同的nodeid生成的ID也不相同
func NewIDMaker(nodeid int64) *IDMaker {
	if nodeid < 0 || nodeid >= 2048 {
		panic("nodeid should between 0 and 2047")
	}

	obj := &IDMaker{}
	obj.nodeshift = int64(nodeid) << 12

	return obj
}

func (t *IDMaker) NextID() int64 {
	//41bit +  11bit +  12bit

	var nID int64

	t.mu.Lock()
AGAIN:
	now := time.Since(tmBegin).Nanoseconds() / 1000000
	nDiff := now - t.time
	if nDiff == 0 {
		t.step = (t.step + 1) & 0x0FFF
		if t.step == 0 {
			for now <= t.time {
				now = time.Since(tmBegin).Nanoseconds() / 1000000
			}
		}
	} else if nDiff > 0 {
		t.step = 0
	} else {
		if nDiff < -10000 { //时间落后不能超过10秒
			panic(fmt.Sprintf("last time is %d,now is %d, now should not behind.", tmBegin.Add(time.Millisecond*time.Duration(t.time)), tmBegin.Add(time.Millisecond*time.Duration(now))))
		} else {
			time.Sleep(time.Millisecond * time.Duration(-nDiff))
			goto AGAIN
		}
	}
	t.time = now
	nID = now<<23 + t.nodeshift + t.step
	t.mu.Unlock()

	return nID
}

//查询某个时间点产生的最小ID可能值
//通常用在数据库根据时间范围查询主键id的场景
//参数: unix时间戳，秒。
func (t *IDMaker) FirstIDByUnixTime(nUnixTime int64) int64 {
	nDiff := nUnixTime - 1587859944
	if nDiff < 0 {
		return 0
	}

	return int64(nDiff*1000) << 23
}

func GetNodeID(nID int64) int64 {
	node := nID >> 12
	return int64(node & 0x07FF)
}

func GetTime(nID int64) time.Time {
	nMiniseconds := nID >> 23
	return tmBegin.Add(time.Millisecond * time.Duration(nMiniseconds))
}

func GetTimeString(nID int64) string {
	return GetTime(nID).Format("2006-01-02 15:04:05")
}

func GetStep(nID int64) int64 {
	return int64(nID & 0x0FFF)
}
