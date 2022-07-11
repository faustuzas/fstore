## Todo

### Raft
* Optimise by syncing and sending message to peers in parallel
* Cache all blocks that have uncommitted entries
* Do something similar as etcd to track inflight requests
* panic: commit index decreasing when doing a workload, panics even if you leave the cluster hang for a little while, it looks like heartbeats are not send