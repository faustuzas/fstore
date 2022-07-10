## Todo

### Raft
* Optimise by syncing and sending message to peers in parallel
* Cache all blocks that have uncommitted entries
* Do something similar as etcd to track inflight requests
* panic: commit index decreasing when doing a workload