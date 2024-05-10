package benchmark

import (
	crand "crypto/rand"

	"encoding/base64"
	"math/big"
	"math/rand"

	"6.5840/labrpc"
)

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func makeSeed() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

// Randomize server handles
func random_handles(kvh []*labrpc.ClientEnd) []*labrpc.ClientEnd {
	sa := make([]*labrpc.ClientEnd, len(kvh))
	copy(sa, kvh)
	for i := range sa {
		j := rand.Intn(i + 1)
		sa[i], sa[j] = sa[j], sa[i]
	}
	return sa
}

type Clerk interface {
	Get(key string) string
	PutAppend(key string, value string, op string)
	Put(key string, value string)
	Append(key string, value string)
}

type Config interface {
	checkTimeout()
	cleanup()
	LogSize() int
	connectUnlocked(i int, to []int)
	connect(i int, to []int)
	disconnectUnlocked(i int, from []int)
	disconnect(i int, from []int)
	All() []int
	ConnectAll()
	partition(p1 []int, p2 []int)
	makeClient(to []int) Clerk
	deleteClient(ck Clerk)
	ConnectClientUnlocked(ck Clerk, to []int)
	ConnectClient(ck Clerk, to []int)
	DisconnectClient(ck Clerk, from []int)
	ShutdownServer(i int)
	StartServer(i int)
	Leader() (bool, int)
	make_partition() ([]int, []int)
	begin(description string)
	op()
	end()
}
