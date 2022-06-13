package experiments

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// Compare config access time by 3 method:
//	* simple global variable access
//  * hard locking
//  * compare and swap

type config struct {
	name string
}

func BenchmarkSingleThreadRead(b *testing.B) {
	var blackhole string

	b.Run("simple_read", func(b *testing.B) {
		c := config{name: "single read"}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			blackhole = c.name
		}
	})

	b.Run("read_with_mutex", func(b *testing.B) {
		c := config{name: "single read"}
		var mu sync.Mutex

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			mu.Lock()
			blackhole = c.name
			mu.Unlock()
		}
	})

	b.Run("read_with_cas", func(b *testing.B) {
		c := atomic.Value{}
		c.Store(config{name: "single read"})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			blackhole = c.Load().(config).name
		}
	})

	_ = fmt.Sprintf("%v", blackhole)
}
