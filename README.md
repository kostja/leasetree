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
it holds to its own children. A node draws on its lease locally. Usage flows up the same tree as
a per-node map, merged at every level, so the leader's total is exact and a branch that moves in
the tree is never counted twice.

A **stock** is a total, such as bytes or objects stored: drawn on, reported, and given back on a
delete. Without a good lease a node refuses; the limit is precious. A **rate** is a flow, such as
requests per tick: a node holds a share of the refill and runs a token bucket from it. Without a
good lease a node admits everything and asks for a lease; availability comes first. Keys that
have no limit, which is most of them, cost nothing and put nothing on the wire.

The rules the protocol needs are listed in the crate docs. Each was found by a simulation that
went wrong without it: leases dated from the request, reports on change, demand forwarded up at
once and earmarked, room pushed down when it arrives, releases deferred until the new parent
confirms, cuts only after a grace period, parent hysteresis, lapsed leases dropped at once, and
a new leader that lends nothing until every old lease is booked again.

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

// The write path, inside the transaction that stores the object.
lease.acquire(&[(tenant_bytes, n), (bucket_bytes, n), (user_objects, 1)])?;
// The delete path.
lease.release(&bucket_bytes, n);
```

`acquire` is local and never sends. A refusal marks the quota wanted, and the next `tick` asks
the parent, so the write path has no network side effects.

Every duration is in ticks; the caller decides what a tick is. `ttl` is the one knob: a lease
is renewed at `ttl / 2`, a request is repeated and a cut waits `ttl / 8`, and a parent that
missed two deliveries of the leader's traffic is left for the peer that delivered them.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
