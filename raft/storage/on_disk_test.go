package storage

import (
	"github.com/prometheus/client_golang/prometheus"
	"io/ioutil"
	"os"
	"path"
	"testing"

	pb "github.com/faustuzas/distributed-kv/raft/raftpb"
	"github.com/faustuzas/distributed-kv/util"
	"github.com/stretchr/testify/require"
)

func TestDiskStateStorage(t *testing.T) {
	suite := StateStorageTestSuite{
		t: t,
		factory: func(state *pb.PersistentState) MutableStateStorage {
			s := &OnDisk{
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
	}

	suite.Run()
}

func TestDiskLogStorage(t *testing.T) {
	var (
		dataDir string
		logDir  string
	)

	suite := LogStorageTestSuite{
		t: t,
		factory: func(entries []pb.Entry) MutableLogStorage {
			dataDir = createTmpDir()
			logDir = path.Join(dataDir, "log")

			s, err := NewOnDiskStorage(OnDiskParams{
				DataDir:      dataDir,
				Encoder:      NewJsonEncoder(),
				Metrics:      NewMetrics(prometheus.NewRegistry()),
				MaxBlockSize: 30,
			})
			if err != nil {
				panic(err)
			}

			err = s.Append(entries...)
			if err != nil {
				panic(err)
			}

			return s
		},
		cleanUp: func() {
			_ = os.RemoveAll(dataDir)
		},
		allEntries: func(storage MutableLogStorage) []pb.Entry {
			var entries []pb.Entry
			for _, file := range listDir(logDir) {
				entries = append(entries, readLogBlock(logDir, file)...)
			}
			return entries
		},
	}

	suite.Run()
}

func TestOnDiskStorage(t *testing.T) {
	t.Run("AppendEntries", func(t *testing.T) {
		t.Run("initial_entry", func(t *testing.T) {
			var (
				tmpDir = createTmpDir()
				logDir = path.Join(tmpDir, "log")
			)
			defer func() {
				_ = os.RemoveAll(tmpDir)
			}()

			storage, err := NewOnDiskStorage(OnDiskParams{
				DataDir:      tmpDir,
				Encoder:      NewJsonEncoder(),
				Metrics:      NewMetrics(prometheus.NewRegistry()),
				MaxBlockSize: 100,
			})
			require.NoError(t, err)

			err = storage.Append(pb.Entry{Index: 1, Term: 1, Data: []byte("hello there")})
			require.NoError(t, err)

			require.Equal(t, []string{"block-0.txt"}, listDir(logDir))
			require.Equal(t, []pb.Entry{{Index: 1, Term: 1, Data: []byte("hello there")}}, readLogBlock(logDir, "block-0.txt"))
		})

		t.Run("append_multiple_times_to_several_blocks", func(t *testing.T) {
			var (
				tmpDir = createTmpDir()
				logDir = path.Join(tmpDir, "log")
			)
			defer func() {
				_ = os.RemoveAll(tmpDir)
			}()

			storage, err := NewOnDiskStorage(OnDiskParams{
				DataDir:      tmpDir,
				Encoder:      NewJsonEncoder(),
				Metrics:      NewMetrics(prometheus.NewRegistry()),
				MaxBlockSize: 60,
			})
			require.NoError(t, err)

			err = storage.Append(pb.Entry{Index: 1, Term: 1, Data: []byte("first entry")})
			require.NoError(t, err)

			err = storage.Append(pb.Entry{Index: 2, Term: 1, Data: []byte("second entry")})
			require.NoError(t, err)

			err = storage.Append(pb.Entry{Index: 3, Term: 1, Data: []byte("third entry")})
			require.NoError(t, err)

			require.Equal(t, []string{"block-0.txt", "block-1.txt"}, listDir(logDir))
			require.Equal(t, []pb.Entry{{Index: 1, Term: 1, Data: []byte("first entry")}, {Index: 2, Term: 1, Data: []byte("second entry")}}, readLogBlock(logDir, "block-0.txt"))
			require.Equal(t, []pb.Entry{{Index: 3, Term: 1, Data: []byte("third entry")}}, readLogBlock(logDir, "block-1.txt"))
		})
	})
}

func TestLogBlock(t *testing.T) {
	t.Run("append_both_disk_and_memory", func(t *testing.T) {
		tmpDir := createTmpDir()
		defer func() {
			_ = os.RemoveAll(tmpDir)
		}()

		lb := logBlock{
			Id:          0,
			PreviousIdx: 0,
			Encoder:     NewJsonEncoder(),
			Metrics:     NewMetrics(prometheus.NewRegistry()),
			FileName:    path.Join(tmpDir, blockFileName(0)),
		}

		_ = lb.AppendEntries(pb.Entry{Index: 1})
		_ = lb.AppendEntries(pb.Entry{Index: 2})
		_ = lb.AppendEntries(pb.Entry{Index: 3})
		_ = lb.AppendEntries(pb.Entry{Index: 4})

		// assert disk
		require.Equal(t, []string{"block-0.txt"}, listDir(tmpDir))
		require.Equal(t, []pb.Entry{{Index: 1}, {Index: 2}, {Index: 3}, {Index: 4}}, readLogBlock(tmpDir, "block-0.txt"))

		// assert memory
		require.Equal(t, []pb.Entry{{Index: 1}, {Index: 2}, {Index: 3}, {Index: 4}}, lb.cachedEntries)
	})
}

func createTmpDir() string {
	dir, err := os.MkdirTemp("", "raft_log*")
	if err != nil {
		panic(err)
	}
	return dir
}

func listDir(dir string) []string {
	ls, err := util.ListDir(dir)
	if err != nil {
		panic(err)
	}
	return ls
}

func readFile(dir, file string) string {
	bytes, err := ioutil.ReadFile(path.Join(dir, file))
	if err != nil {
		panic(err)
	}
	return string(bytes)
}

func readLogBlock(dir, file string) []pb.Entry {
	f, err := os.Open(path.Join(dir, file))
	if err != nil {
		panic(err)
	}

	var (
		decoder = NewJsonDecoder(f)
		result  []pb.Entry
		entry   pb.Entry
	)
	for {
		if has, err := decoder.Scan(); err != nil {
			panic(err)
		} else if !has {
			return result
		}

		if _, err := decoder.DecodeNext(&entry); err != nil {
			panic(err)
		}

		result = append(result, entry)
	}
}
