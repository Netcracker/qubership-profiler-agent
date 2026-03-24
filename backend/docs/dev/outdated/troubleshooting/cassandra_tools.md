
See also

- [nodetool-commands-like-compact](https://dba.stackexchange.com/questions/316036/can-someone-explain-in-simple-terms-cassandra-nodetool-commands-like-compact-and)
- [forcing-major-compaction](https://dba.stackexchange.com/questions/314531/why-is-forcing-major-compaction-on-a-table-not-ideal)

#### GC

> `nodetool garbagecollect`

Clears deleted data from SSTables. ("Shadowed", deleted=shadowed by a tombstone)
<br> This command triggers compaction on an SSTable to remove "droppable" deleted (cell/row/partition) data.

- By default, it will clean deleted partitions and rows. To also clean deleted cell values, use the option `-g CELL`.
- It does not generally drop the tombstones (even after `gc_grace_seconds`).  

There are some scenarios where it does not evict deleted data.
The concept of "_droppable_" is quite complex particularly since parts of a partition (i.e. columns, rows)
can be fragmented across multiple SSTables so working out whether deleted data can be dropped is not an easy task.

Setting `gc_grace_seconds` should not bee too low:
<br> if you run into a situation where one of the nodes encounter a hardware failure, you need to be
able to replace that node within GC grace. Otherwise, you cannot put that node back into the
cluster because the tombstones would have expired. If you try to put a failed node back, the other
nodes no longer have the tombstones (they've been GCd) so the deleted data on the failed node will
get resurrected back to your cluster.

> `nodetool compact`
<br> A manual way to instruct Cassandra to consolidate/rewrite SSTables
> while also evicting expired (tombstoned) data.
> Tombstones are considered expired when they are past the GC grace configured on a table.

Note that running this command is NOT recommended. See also for more details:

- [forcing-major-compaction](https://dba.stackexchange.com/questions/314531/why-is-forcing-major-compaction-on-a-table-not-ideal)

> `nodetool cleanup`
<br> Removes partitions (records) whose keys are no longer owned by a node
> because the token range(s) it owns has changed after another node has been added to the cluster.

For example, let's say node A owns token range 0-100. Node B was added to the cluster with ownership of token range 80-200.
As a result of this, node A no longer owns the data in the token range 80-100 (now only owns data in the token range 0-79).
<br> You would run cleanup on this node to get rid of the data in the 80-100 range to reclaim the disk space.

> `nodetool repair`
> <br> Repairs are the mechanism by which Cassandra fixes (repairs) the inconsistencies.
> The functionality is analogous to the way rsync works for filesystems

Cassandra has a distributed architecture where nothing is shared, not even the storage layer.
Each node in the cluster is a separate instance on its own. Due to this distributed nature,
copies of the data on the nodes (replicas) can get out of sync.

#### Conclusion

In the first instance, you can try to run `nodetool garbagecollect` on the problematic table to
try to reclaim the space.

If that does not work, then you can try `nodetool compact` BUT be aware of the consequences.
