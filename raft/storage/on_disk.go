package storage

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"sync"

	pb "github.com/faustuzas/distributed-kv/raft/raftpb"
	"github.com/faustuzas/distributed-kv/util"
	"github.com/gogo/protobuf/jsonpb"
)

const (
	_stateFileName = "state.json"
)

var jsonpbMarshaler = jsonpb.Marshaler{EnumsAsInts: true}

// hints to compiler what interfaces have to be implemented
var (
	_ MutableLogStorage   = (*OnDisk)(nil)
	_ MutableStateStorage = (*OnDisk)(nil)
)

type OnDiskParams struct {
	DataDir      string
	Encoder      Encoder
	Metrics      *Metrics
	MaxBlockSize int
}

type OnDisk struct {
	logLock   sync.RWMutex
	stateLock sync.RWMutex

	dataDir string
	log     log
}

// TODO: close stuff!

func NewOnDiskStorage(params OnDiskParams) (*OnDisk, error) {
	logDir := path.Join(params.DataDir, "log")
	if err := util.CreateDirIfNotExists(logDir); err != nil {
		return nil, fmt.Errorf("creating data dir: %w", err)
	}

	l := log{
		DataDir:      logDir,
		MaxBlockSize: params.MaxBlockSize,
		Encoder:      params.Encoder,
		Metrics:      params.Metrics,
	}

	if err := l.LoadBlocksMetadata(); err != nil {
		return nil, fmt.Errorf("loading blocks: %w", err)
	}

	return &OnDisk{
		dataDir: params.DataDir,
		log:     l,
	}, nil
}

// TODO: extract encoder
// TODO: add caching
func (s *OnDisk) State() (pb.PersistentState, bool, error) {
	s.stateLock.RLock()
	defer s.stateLock.RUnlock()

	f, err := os.Open(s.stateFilePath())
	if err != nil {
		if os.IsNotExist(err) {
			return pb.PersistentState{}, false, nil
		}

		return pb.PersistentState{}, false, fmt.Errorf("opening state file: %w", err)
	}

	var state pb.PersistentState
	err = jsonpb.Unmarshal(f, &state)
	if err != nil {
		return pb.PersistentState{}, false, fmt.Errorf("reading state file: %w", err)
	}

	return state, true, nil
}

func (s *OnDisk) SetState(state pb.PersistentState) error {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	stateStr, err := jsonpbMarshaler.MarshalToString(&state)
	if err != nil {
		return fmt.Errorf("marshaling state: %w", err)
	}

	return util.AtomicFileSwap(s.stateFilePath(), []byte(stateStr))
}

func (s *OnDisk) stateFilePath() string {
	return path.Join(s.dataDir, _stateFileName)
}

func (s *OnDisk) Entries(startIdx, endIdx uint64) ([]pb.Entry, error) {
	s.logLock.RLock()
	defer s.logLock.RUnlock()

	return s.log.Entries(startIdx, endIdx-1)
}

func (s *OnDisk) Term(idx uint64) (uint64, error) {
	if idx == 0 {
		return 0, nil
	}

	s.logLock.RLock()
	defer s.logLock.RUnlock()

	return s.log.Term(idx)
}

func (s *OnDisk) LastIndex() (uint64, error) {
	s.logLock.RLock()
	defer s.logLock.RUnlock()

	return s.log.lastIndex(), nil
}

func (s *OnDisk) Append(entries ...pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	s.logLock.Lock()
	defer s.logLock.Unlock()

	// check what is the last index and term of the entries stored in disk
	//  * if appended entries nicely follows it, just append to the file and check whether we need to seal the file
	// if we have overlapping entries
	//  * find the offset where they are different,
	//  * truncate everything from there forward
	//  * append new entries to the end

	lastIndex := s.log.lastIndex()
	if lastIndex != 0 && lastIndex+1 != entries[0].Index {
		if err := s.log.TruncateTo(entries[0].Index - 1); err != nil {
			return fmt.Errorf("truncating log to index %d (last index: %d): %w", entries[0].Index-1, lastIndex, err)
		}
	}

	return s.log.appendEntries(entries...)
}

type log struct {
	DataDir      string
	MaxBlockSize int
	Encoder      Encoder
	Metrics      *Metrics

	blocks []*logBlock
}

func (l *log) lastBlock() *logBlock {
	if len(l.blocks) == 0 {
		return nil
	}

	return l.blocks[len(l.blocks)-1]
}

func (l *log) lastIndex() uint64 {
	if block := l.lastBlock(); block != nil {
		if block.lastIdx != 0 {
			return block.lastIdx
		}

		// there are no entries in the block yet
		return block.PreviousIdx
	}
	return 0
}

func (l *log) appendEntries(entries ...pb.Entry) error {
	// TODO: should track file size after every entry append
	block := l.lastBlock()
	if err := block.AppendEntries(entries...); err != nil {
		return err
	}

	if block.size > l.MaxBlockSize {
		newBlock := l.newBlock(block.Id+1, block.lastIdx)
		l.blocks = append(l.blocks, newBlock)
	}

	return nil
}

func (l *log) newBlock(id int, previousIndex uint64) *logBlock {
	return &logBlock{
		Id:          id,
		Encoder:     l.Encoder,
		Metrics:     l.Metrics,
		FileName:    path.Join(l.DataDir, blockFileName(id)),
		PreviousIdx: previousIndex,
	}
}

func (l *log) LoadBlocksMetadata() error {
	blockNames, err := util.ListDir(l.DataDir)
	if err != nil {
		return fmt.Errorf("listing data dir: %w", err)
	}

	var previousBlock *logBlock
	for _, fileName := range blockNames {
		id, err := l.parseBlockIdFromFileName(fileName)
		if err != nil {
			return fmt.Errorf("parsing Id: %w", err)
		}

		previousIdx := uint64(0)
		if previousBlock != nil {
			previousIdx = previousBlock.lastIdx
		}

		block := l.newBlock(id, previousIdx)
		if err = block.load(); err != nil {
			return fmt.Errorf("loading block %d: %w", block.Id, err)
		}

		block.unload()

		l.blocks = append(l.blocks, block)
		previousBlock = block
	}

	sort.Slice(l.blocks, func(i, j int) bool {
		return l.blocks[i].Id < l.blocks[j].Id
	})

	// initial block
	if len(l.blocks) == 0 {
		l.blocks = append(l.blocks, l.newBlock(0, 0))
	}

	return nil
}

func blockFileName(blockId int) string {
	return fmt.Sprintf("block-%d.txt", blockId)
}

func (l *log) parseBlockIdFromFileName(fileName string) (int, error) {
	var id int

	n, err := fmt.Sscanf(fileName, "block-%d.txt", &id)
	if err != nil {
		return 0, fmt.Errorf("sscanf: %w", err)
	}

	if n != 1 {
		return 0, fmt.Errorf("unable to extract Id from %s", fileName)
	}

	return id, nil
}

func (l *log) Entries(startIdx uint64, endIdx uint64) ([]pb.Entry, error) {
	startBlock := l.findBlockForIdx(startIdx)
	if startBlock == nil {
		return nil, fmt.Errorf("not able to find block for idx %d", startIdx)
	}

	endBlock := l.findBlockForIdx(endIdx)
	if startBlock == nil {
		return nil, fmt.Errorf("not able to find block for idx %d", endIdx)
	}

	var result []pb.Entry
	for _, block := range l.blocks[startBlock.Id : endBlock.Id+1] {
		start := util.MaxUint64(block.firstIdx, startIdx)
		end := util.MinUint64(block.lastIdx, endIdx)

		entries, err := block.Entries(start, end)
		if err != nil {
			return nil, fmt.Errorf("slicing from block %d [%d:%d]: %w", block.Id, start, end, err)
		}

		// TODO: hmm...
		block.unload()

		result = append(result, entries...)
	}
	return result, nil
}

func (l *log) findBlockForIdx(idx uint64) *logBlock {
	for _, block := range l.blocks {
		if block.firstIdx <= idx && idx <= block.lastIdx {
			return block
		}
	}

	return nil
}

func (l *log) Term(idx uint64) (uint64, error) {
	block := l.findBlockForIdx(idx)
	if block == nil {
		return 0, fmt.Errorf("block for idx %d not found", idx)
	}

	return block.Term(idx)
}

func (l *log) TruncateTo(lastGoodIdx uint64) error {
	// all log is bad and should be removed
	if lastGoodIdx == 0 {
		for _, b := range l.blocks {
			if err := b.Remove(); err != nil {
				return fmt.Errorf("removing block %d: %w", b.Id, err)
			}
		}
		return nil
	}

	// find the block from where we have invalid entries
	block := l.findBlockForIdx(lastGoodIdx)
	if block == nil {
		return fmt.Errorf("cannot truncate to %d because it is not present in the log", lastGoodIdx)
	}

	// remove all further blocks since they are invalid
	if block.Id < l.lastBlock().Id {
		for _, b := range l.blocks[block.Id+1:] {
			if err := b.Remove(); err != nil {
				return fmt.Errorf("removing block %d: %w", b.Id, err)
			}
		}
		l.blocks = l.blocks[:block.Id+1]
	}

	if err := block.TruncateTo(lastGoodIdx); err != nil {
		return fmt.Errorf("truncating block to %d: %w", lastGoodIdx, err)
	}

	return nil
}

type logBlock struct {
	Id int
	// PreviousIdx is the last index in the previous block
	PreviousIdx uint64
	FileName    string
	Encoder     Encoder
	Metrics     *Metrics

	file              *os.File
	firstIdx, lastIdx uint64
	size              int
	cachedEntries     []pb.Entry
}

func (lb *logBlock) AppendEntries(entries ...pb.Entry) error {
	if lb.file == nil {
		if err := lb.openFile(); err != nil {
			return fmt.Errorf("creating block file: %w", err)
		}
	}

	if lb.cachedEntries == nil {
		if err := lb.loadEntries(); err != nil {
			return fmt.Errorf("loading entries: %w", err)
		}
	}

	for _, entry := range entries {
		entrySize, err := lb.Encoder.Encode(lb.file, &entry)
		if err != nil {
			return fmt.Errorf("encoding entry: %w", err)
		}

		lb.size += entrySize
		lb.trackIndexes(entry)
		lb.Metrics.RecordBytesWritten(entrySize)

		lb.cachedEntries = append(lb.cachedEntries, entry)
	}

	if err := lb.file.Sync(); err != nil {
		return fmt.Errorf("syncing block: %w", err)
	}

	return nil
}

func (lb *logBlock) trackIndexes(entry pb.Entry) {
	if lb.firstIdx == 0 {
		lb.firstIdx = entry.Index
	}

	lb.lastIdx = entry.Index
}

func (lb *logBlock) openFile() error {
	var err error
	lb.file, err = os.OpenFile(lb.FileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, os.ModePerm)
	return err
}

func (lb *logBlock) load() error {
	if err := lb.openFile(); err != nil {
		return fmt.Errorf("opening block file: %w", err)
	}

	if err := lb.loadEntries(); err != nil {
		return fmt.Errorf("loading entries: %w", err)
	}

	lb.firstIdx = lb.cachedEntries[0].Index
	lb.lastIdx = lb.cachedEntries[len(lb.cachedEntries)-1].Index

	return nil
}

func (lb *logBlock) loadEntries() error {
	return lb.Metrics.ObserveBlockLoading(func() error {
		_, err := lb.file.Seek(0, io.SeekStart)
		if err != nil {
			return fmt.Errorf("seeking to beggining: %w", err)
		}

		var (
			dec   = lb.Encoder.Decoder(lb.file)
			entry pb.Entry
		)
		for {
			has, err := dec.Scan()
			if err != nil {
				return fmt.Errorf("scanning: %w", err)
			}

			if !has {
				return nil
			}

			bytesRead, err := dec.DecodeNext(&entry)
			for err != nil {
				return fmt.Errorf("decoding: %w", err)
			}

			lb.Metrics.RecordBytesRead(bytesRead)

			lb.cachedEntries = append(lb.cachedEntries, entry)
			entry.Reset()
		}
	})
}

func (lb *logBlock) unload() {
	lb.cachedEntries = nil
}

func (lb *logBlock) Entries(startIdx uint64, endIdx uint64) ([]pb.Entry, error) {
	if lb.cachedEntries == nil {
		if err := lb.loadEntries(); err != nil {
			return nil, fmt.Errorf("loading entries: %w", err)
		}
	}

	start := startIdx - lb.firstIdx
	end := endIdx - lb.firstIdx
	s := lb.cachedEntries[start : end+1]
	return s, nil
}

func (lb *logBlock) Term(idx uint64) (uint64, error) {
	if lb.cachedEntries == nil {
		if err := lb.loadEntries(); err != nil {
			return 0, fmt.Errorf("loading entries: %w", err)
		}
	}

	adjustedIdx := idx - lb.firstIdx
	return lb.cachedEntries[adjustedIdx].Term, nil
}

func (lb *logBlock) Remove() error {
	if err := os.Remove(lb.FileName); !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (lb *logBlock) TruncateTo(lastGoodIdx uint64) error {
	if lb.cachedEntries == nil {
		if err := lb.loadEntries(); err != nil {
			return fmt.Errorf("loading entries: %w", err)
		}
	}

	idx := lastGoodIdx - lb.cachedEntries[0].Index
	lb.cachedEntries = lb.cachedEntries[:idx+1]
	lb.lastIdx = lastGoodIdx

	var buf bytes.Buffer
	buf.Grow(lb.size)

	lb.size = 0
	for _, entry := range lb.cachedEntries {
		entrySize, err := lb.Encoder.Encode(&buf, &entry)
		if err != nil {
			return fmt.Errorf("encoding entry: %w", err)
		}

		lb.size += entrySize
	}

	_ = lb.file.Close()
	lb.file = nil
	if err := util.AtomicFileSwap(lb.FileName, buf.Bytes()); err != nil {
		return fmt.Errorf("swapping block content: %w", err)
	}

	return nil
}

type Encoder interface {
	// Encode serializes and writes an entry to the w.
	// Returns bytes written or an error
	Encode(w io.Writer, entry *pb.Entry) (int, error)

	Decoder(r io.Reader) Decoder
}

type Decoder interface {
	// Scan returns whether the source has any more data to return
	// and loads the next entry into memory if present
	Scan() (bool, error)

	// DecodeNext reads an entry worth of data from the source and serialises the content back to an entry.
	// Returns bytes read from the source
	DecodeNext(entry *pb.Entry) (int, error)
}

type jsonEncoder struct {
	enc jsonpb.Marshaler
}

func NewJsonEncoder() Encoder {
	return &jsonEncoder{
		enc: jsonpb.Marshaler{
			EnumsAsInts: true,
		},
	}
}

func (e *jsonEncoder) Encode(w io.Writer, entry *pb.Entry) (int, error) {
	str, err := e.enc.MarshalToString(entry)
	if err != nil {
		return 0, fmt.Errorf("json encoding: %w", err)
	}

	var bytesWritten int
	if written, err := w.Write([]byte(str)); err != nil {
		return 0, err
	} else {
		bytesWritten += written
	}

	if written, err := w.Write([]byte("\n")); err != nil {
		return 0, err
	} else {
		bytesWritten += written
	}

	return bytesWritten, err
}

func (e *jsonEncoder) Decoder(r io.Reader) Decoder {
	return NewJsonDecoder(r)
}

type jsonDecoder struct {
	scanner *bufio.Scanner

	next []byte
}

func NewJsonDecoder(source io.Reader) Decoder {
	return &jsonDecoder{
		scanner: bufio.NewScanner(source),
	}
}

func (d *jsonDecoder) Scan() (bool, error) {
	if d.next != nil {
		return true, nil
	}

	if d.scanner.Scan() {
		d.next = d.scanner.Bytes()
		return true, nil
	} else {
		return false, d.scanner.Err()
	}
}

func (d *jsonDecoder) DecodeNext(entry *pb.Entry) (int, error) {
	if d.next == nil {
		panic("decode next should be called after Scan()")
	}

	if err := jsonpb.Unmarshal(bytes.NewBuffer(d.next), entry); err != nil {
		return 0, err
	}

	bytesRead := len(d.next)
	d.next = nil
	return bytesRead, nil
}
