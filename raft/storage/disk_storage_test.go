package storage

import (
	"github.com/stretchr/testify/require"
	"os"
	"path"
	"testing"

	pb "github.com/faustuzas/distributed-kv/raft/raftpb"
)

func TestDiskStateStorage(t *testing.T) {
	StateStorageTestSuite{
		t: t,
		factory: func(state *pb.PersistentState) MutableStateStorage {
			s := &DiskStorage{
				dataDir: os.TempDir(),
			}

			if state != nil {
				err := s.SetState(*state)
				require.NoError(t, err)
			}

			return s
		},
		cleanUp: func() {
			_ = os.Remove(path.Join(os.TempDir(), _stateFileName))
		},
	}.Run()
}
