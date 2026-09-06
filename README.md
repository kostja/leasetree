# leasetree

A pure state machine for distributed quotas. No IO, no clock. You feed it events; it returns
messages to send. It is the third of three small crates that together enforce a cluster-wide
limit without a round trip on the write path:

- [`bcounter`](https://github.com/kostja/bcounter) does the accounting: a node's grant, and a
  usage map that merges by node id.
- [`plumtree-fsm`](https://github.com/kostja/plumtree) is the overlay: it tells a node which
  peer is upstream, towards the leader.
- `leasetree` is the protocol: leases handed down a tree rooted at the leader, usage reported
  up, everything TTL'd and fenced by the leader's term.

## How it works

The leader holds the whole limit. It lends chunks to its children; each child lends out of what
it holds to its own children. A node draws on its lease locally, with no message on the write
path. Usage flows up the same tree as a per-node map, merged at every level, so the leader's
total is exact and a branch that moves in the tree is never counted twice.

Two kinds of limit, and the kind decides what a node does without a good lease:

- A **stock** is a total, such as bytes or objects stored: drawn on, reported, given back on a
  delete. Without a good lease a node refuses; the limit is precious. A stock is configured
  together with this node's own usage from durable storage.
- A **rate** is a flow, such as requests per tick: a node holds a share of the refill and runs
  a token bucket from it. Nothing is reported. Without a good lease a node admits everything
  and asks for a lease; availability comes first.

A lease is good while it was confirmed in the current term and has not lapsed. Most keys never
have a limit set; they cost nothing, and a node with nothing in play puts nothing on the wire.

### The protocol

One `Message` per peer carries every limit: the sender's term and leader, its tick, its subtree
(going up), and the items. Going up: `Request { key, want }`, `Renew { key, granted, usage,
wanted }`, the keep-alive and report, and `Release { key, amount }`. Going down: `Grant { key,
amount, renewal, hold }`, where a renewal's `hold` is all the parent books for the child, and
`Shrink { key, to }`. A stale leader learns of its successor from the term on the first message
it receives; a child learns of a new leader from any ancestor's reply.

### The rules

Each was found by a simulation that went wrong without it.

- A lease is dated from the tick its request was *sent*, so a parent that lapses it (dated from
  arrival, later) never re-lends room the child still considers its own.
- A child reports whenever its bookings, its subtree, its wants or its term changed, and every
  `ttl / 2` as a keepalive. A parent books what the child reports.
- A parent that cannot fill a request books the child anyway, asks its own parent for the
  shortfall at once, keeps that much earmarked, and pushes room down the moment it arrives.
- A node without a good lease asks for one, whatever the kind decides about the write.
- A node that moves to a new parent owes the old one everything it held before dropping
  anything, and pays only once the new parent has confirmed the adoption; until then both book
  it and nobody re-lends it.
- A parent that booked more than it holds cuts a stock only after a grace period, never below
  what the child's subtree has used, and the child passes the cut down. A rate is never cut.
- A node keeps its parent while that parent keeps delivering the leader's traffic, and never
  takes its own child as parent.
- A node drops a lease the moment it lapses: the parent has re-lent that room.
- A new leader grants nothing and cuts nobody until its reports cover every live member, or one
  `ttl` has passed.

## What the caller supplies

The machine does not know who the leader is, which peer is upstream, or who is alive. It is
told, and it does not care from where:

| input | from | when |
|---|---|---|
| `set_limit(key, Limit)`, `remove_limit(&key)` | the quota configuration | at boot and on change; a stock's usage rides along |
| `set_cluster_view(members, leader, term)` | Raft's system tables | on every change; `members` may be omitted |
| `set_upstream(peer)` | the overlay | whenever the leader's traffic is delivered, with the peer that delivered it |
| `down(peers)`, `up(peers)` | the failure detector | on its verdicts |
| `on_message(from, Message)` | the network | on receipt |
| `tick(n)` | the timer | every tick |

Every input returns `true` when it took the outbound queue from empty to non-empty: the edge on
which to wake a sender, which drains `ready()`.

## Driving it

```rust,ignore
use leasetree::{Lease, Config, Limit, Action};

// Boot: the limits from configuration, a stock's own usage from durable storage inside it,
// the view from Raft.
let mut lease = Lease::new(my_raft_id, Config { ttl: 40 });
for row in quota_table {
    let limit = match row.kind {
        Kind::Rate => Limit::Rate { limit: row.value, chunk: row.chunk },
        Kind::Stock => {
            let (acquired, released) = usage_table.own(&row.key);
            Limit::Stock { limit: row.value, chunk: row.chunk, acquired, released }
        }
    };
    lease.set_limit(row.key(), limit);
}
lease.set_cluster_view(Some(&instances), leader, term);

// On every system-table change.
wake_if(lease.set_cluster_view(Some(&instances), leader, term));

// The leader sends something over the overlay now and then; every delivery of it tells a
// node which peer is upstream.
wake_if(lease.set_upstream(deliverer));

// The failure detector.
wake_if(lease.down(&dead)); wake_if(lease.up(&back));

// The network and the timer.
wake_if(lease.on_message(peer, decode(bytes)));
wake_if(lease.tick(1));

// The sender fibre, woken on the edge.
for Action::Send(peer, msg) in lease.ready() { pool.send(peer, encode(&msg)).await; }

// The write path, inside the transaction that stores the object: all or none.
lease.acquire(&[(tenant_bytes, n), (bucket_bytes, n), (user_objects, 1)])?;
// The delete path.
lease.release(&bucket_bytes, n);
```

`acquire` is local and never sends. A refusal marks the limit wanted, and the next `tick` asks
the parent, so the write path has no network side effects. `usage(&key)` is this node's view of
a stock under it, exact at the leader up to report lag; `stats(&key)` has the figures for
metrics.

A complete driver, with a surrogate network, Raft's view arriving late, a failure detector, a
load and the measurements, is the `lease-sim` crate in the
[`bcounter`](https://github.com/kostja/bcounter) repository.

## Configuration

Every duration is in ticks; the caller decides what a tick is. There is one knob:

| | value |
|---|---|
| `ttl` | how long a lease is good without renewal; default 40 |
| renew | every `ttl / 2` |
| repeat an unanswered request | after `ttl / 8` |
| cut a child, once over-committed for | `ttl / 8` |
| leave a parent | after it missed two deliveries of the leader's traffic |
| a new leader's fallback window | `ttl` |

With a tick of 100 ms, `ttl: 40` is a four-second lease, renewed every two seconds. A node
whose parent dies re-parents at the next two deliveries and is re-leased within a few ticks. A
new leader lends again as soon as its reports cover every live member, or after four seconds
if some member never speaks: a node with nothing in play sends nothing, and is not counted.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
