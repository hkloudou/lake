package index

// var myunixAndSeq = NewUnixAndSeq()

// type unixAndSeq struct {
// 	mu      sync.Mutex
// 	current int64
// 	seq     uint64
// }

// func NewUnixAndSeq() *unixAndSeq {
// 	return &unixAndSeq{
// 		current: time.Now().Unix(),
// 	}
// }

// // GetUnixAndSeq 获取当前 Unix 时间戳和递增的 Seq 值
// func (us *unixAndSeq) GetUnixAndSeq() (int64, uint64) {
// 	us.mu.Lock()
// 	defer us.mu.Unlock()
// 	if us.seq >= 0xFFFFFFFE {
// 		time.Sleep(time.Second)
// 	}
// 	now := time.Now().Unix()
// 	if now != us.current {
// 		us.current = now
// 		us.seq = 0
// 	} else {
// 		us.seq++
// 	}
// 	return us.current, us.seq
// }
