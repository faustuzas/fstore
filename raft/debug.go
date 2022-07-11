package raft

import (
	"encoding/json"
	"fmt"
	pb "github.com/faustuzas/fstore/raft/raftpb"
	"os"
	"runtime/debug"
)

type DebugEntry struct {
	Type string
	Data interface{}
}

var debugEntries []DebugEntry

func debugRecover(r *raft, err any) {
	fmt.Printf("RAFT IS PANICING: %v\n", err)
	fmt.Printf("Dump can be found in %v\n", dump(r, debugEntries, string(debug.Stack())))
	os.Exit(1)
}

func dump(r *raft, debugEntries []DebugEntry, stackTrace string) string {
	f, _ := os.CreateTemp("", "*raft_dump.txt")

	_, _ = fmt.Fprintf(f, "DUMP OF RAFT INSTANCE %d\n", r.id)
	_, _ = fmt.Fprintf(f, "%#+v\n%#+v\n", *r, *r.raftLog)
	_, _ = fmt.Fprintf(f, "\n\nSTACK TRACE\n:%v\n", stackTrace)

	_, _ = fmt.Fprintf(f, "\n\nDEBUG ENTRIES\n")
	for _, entry := range debugEntries {
		_ = json.NewEncoder(f).Encode(entry)
	}

	return f.Name()
}

func recordProgress(r *raft, progress Progress) {
	if len(progress.EntriesToApply) > 0 {
		debugEntries = append(debugEntries, DebugEntry{"ENTRIES TO APPLY", struct {
			Entries         []pb.Entry
			Applied, Commit uint64
		}{
			Entries: progress.EntriesToApply,
			Applied: r.raftLog.appliedIndex,
			Commit:  r.raftLog.commitIndex,
		}})
	}

	if len(progress.EntriesToPersist) > 0 {
		debugEntries = append(debugEntries, DebugEntry{"ENTRIES TO PERSIST", struct {
			Entries         []pb.Entry
			Applied, Commit uint64
		}{
			Entries: progress.EntriesToPersist,
			Applied: r.raftLog.appliedIndex,
			Commit:  r.raftLog.commitIndex,
		}})
	}

	for _, msg := range progress.Messages {
		debugEntries = append(debugEntries, DebugEntry{"MSG OUT", msg})
	}
}

func recordReceivedMsg(msg pb.Message) {
	debugEntries = append(debugEntries, DebugEntry{"MSG IN", msg})
}
