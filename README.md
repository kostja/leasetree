# leasetree

A pure state machine for distributed rate limits. No IO, no clock. You feed it events; it
returns calls to make. It sits beside [`plumtree-fsm`](https://github.com/kostja/plumtree),
whose spanning tree it follows: plumtree is the overlay, leasetree is the protocol that hands
shares of a rate down that tree.

## How it works

The cluster's leader holds the whole rate. It lends shares to its children, who lend out of
what they hold to their own children. Every node runs a token bucket from its share and admits
against it locally, with no message on the request path. A share has a TTL and lapses when not
renewed; a node without one admits everything and asks for one.

That last sentence is the design: availability first. A node has no share because it is new,
or its parent died, or the leader changed; during the round trip or two it takes to get one it
admits, and the limit is overshot by that much. A rate limit shapes load. It is not a boundary,
and a limit that must never be exceeded, a stock of bytes or objects, is a different problem
and not this crate.

Keys that have no limit, which is most of them, cost nothing and put nothing on the wire.

### The protocol: one call

The whole protocol is one RPC from a child to its parent, `LeaseRequest` answered by
`LeaseResponse`. The parent never calls the child.

```rust
pub struct LeaseRequest<Id, K> {
    pub term: u64, pub leader: Option<Id>,  // the child's view; a stale parent learns from it
    pub sent: u64,                          // the child's tick: the share is dated from it
    pub seen: u64,                          // the `sent` of the last answer it applied
    pub items: Vec<RequestItem<K>>,
}
pub struct RequestItem<K> {
    pub key: K,
    pub granted: u64,                       // what I hold from you; less than before is a release
    pub wanted: u64,                        // what more I would take
}
pub struct LeaseResponse<Id, K> {
    pub term: u64, pub leader: Option<Id>,  // the parent's view; a stale child learns from it
    pub in_reply_to: u64,                   // the request's `sent`
    pub sent: u64,                          // the parent's tick, echoed back as `seen`
    pub items: Vec<ResponseItem<K>>,
}
pub struct ResponseItem<K> {
    pub key: K,
    pub total: u64,                         // your share now: a total, not a delta
}
```

A child calls every `ttl / 2` as a keepalive, and at once when what it holds or wants changed
or its term did. While it wants something it also retries: after a round trip at first, since
the parent may be fetching it, then twice as long after each empty answer, up to `ttl / 8`. A
call that goes unanswered is sent again on the same backoff, keepalive or not: otherwise one
lost keepalive would let the share lapse, since the next keepalive's answer lands one tick
after the share's validity ends.

`wanted` is what the child would take. The parent answers with the child's share as a total,
its entitlement in the split below; what it could not give it remembers for this child and
asks its own parent for, and what it fetches for a waiting child it never hands back as spare.
A total is idempotent: a re-sent request gets the same answer and lends nothing more, and
a total below what the child holds is a cut, which the child adopts. The child adopts only the
answer to its latest request. A release is a smaller `granted`; a node that moved tells its
old parent `granted: 0`. A parent books what the child reports, and `seen` lets it tell, on
its own clock, whether a report was sent before its last answer arrived, in which case its own
booking stands; no clock is ever compared across nodes.

### The rules

**A share is dated from the tick its request was sent.** The child keeps `valid_until`, past
which it has no share; the parent keeps `expires` on its booking, past which it may lend the
share to someone else. The child dates its share from the tick it *sent* the request, the
parent dates its booking from the tick the request *arrived*, and arrival is never earlier
than sending, so the parent's expiry is always at or after the child's. With a TTL of 40 and
one tick of latency:

| tick | what happens |
|---|---|
| 100 | child C calls parent P |
| 101 | P receives the call, books C until 141, answers |
| 102 | C receives the answer; its share is good until 140 |

The room is never a share at C and lendable at P at the same time. Dating the share from the
answer's arrival instead reads as safe and is not: P could lend it to D at 141 while C still
held it. Each node compares only its own clock with itself, so skew does not matter; what
matters is that a node's clock never steps back. This is the lease discipline of Chubby and
GFS.

**A child calls on change, and every `ttl / 2` regardless; a parent books what the child
reports.** The keepalive is time-driven: with no news, a child still calls every `ttl / 2`, so
one lost call does not lapse its share. Everything else is change-driven, at the next tick:
what the node holds changed, what it wants changed, or its term did. And the parent's booking
is what the child says it holds, not what the parent sent: a grant that never arrived, a share
handed back, a share adopted from another parent, all reconcile on the next call.

**A parent splits its share among its claimants by weighted max-min fairness.** The claimants
are its children and itself. A child's demand is what it holds and wants, as reported; its
weight is its subtree size (`set_weights`); the parent's own use, measured as the tokens it
drew over the last tick, weighs one. Water-filling: a level per unit of weight rises until
the share is spent, a claimant whose demand is under its level gets its demand, and the rest is
shared by weight among those still asking. A root of 75 with its own use 2, a 25-node child
wanting 50 and a leaf wanting 100 gives 2, 50 and 23; were the 25 nodes to want 200, the
level would be 75 / 27 per node: 2, 68 and 3. So a busy node in a small domain cannot take
a large domain's share, an idle domain's share flows to whoever is busy, and a root that
inherits more than its limit after a leader change cuts its children to their entitlements
within one keepalive round. A child is cut only once it is more than a chunk over its
entitlement, so a split that shifts by a unit does not churn; and a parent never lends beyond
what it holds, so the room an over-entitled child frees on its keepalive goes to the next
asker. Needs -- what a child already lent beyond its share, a moved subtree -- are demand
like any other, served first within the child's entitlement.

**A node without a share admits everything and asks for one.** Without the asking half a
node would stay outside the limit for good, since nothing is ever refused to it. Node C has a
rate of 2 per tick and no share:

| tick | without the rule | with the rule |
|---|---|---|
| 1 | admitted, unleased; nothing asked | admitted, unleased; C calls with `wanted: 2` |
| 3 | admitted, unleased | leased at 2; admitted against the bucket |
| 100 | still admitted, still unleased | throttled at 2 per tick like everyone |

**A node's parent is the peer the overlay delivers the leader's traffic through; it moves to
a new deliverer once that peer has delivered twice in a row, and never to its own child.**
The lease tree has no shape of its own. It follows the overlay's tree rooted at the leader and
follows its changes: when plumtree swaps a link for a cheaper one, the deliverer changes for
good, and two deliveries later so does the parent, with one release and one adoption. What it
does not follow is a single detour, a lost copy fetched from a lazy peer. Measured in the
simulator at N=200: with no loss, every node's parent is the peer the overlay delivers through;
at 5% loss, about nine in ten, the rest inside the two-delivery lag of a swap in progress. So
the lease tree is as local to a data centre as the overlay is. The own-child refusal is for the
turn-around after a leader change, so that two nodes never book each other's shares in a loop.
A node whose parent was the old leader takes the first upstream it is offered, since Raft has
just told it that leader is gone.

A leader change costs nothing else: the new leader holds the rate at once, and every node that
holds a share calls its parent when Raft shows the new term, so the new tree books what it
holds. A deposed leader steps down on the first call it receives with the newer term.

## What the caller supplies

The machine does not know who the leader is, which peer is upstream, or who is alive. It is
told, and it does not care from where:

| input | from | when |
|---|---|---|
| `set_limit(key, Limit)`, `remove_limit(&key)` | the quota configuration | at boot and on change |
| `set_cluster_view(members, leader, term)` | Raft's system tables | on every change; `members` may be omitted |
| `set_upstream(peer)` | the overlay | whenever the leader's traffic is delivered, with the peer that delivered it |
| `set_parent(peer)` | [`tree::place`](#the-tree-from-the-view) | whenever the view changes; switches at once, no hysteresis |
| `down(peers)`, `up(peers)` | the failure detector | on its verdicts |
| `on_request(from, LeaseRequest) -> LeaseResponse` | the RPC handler | on a child's call; returns the answer to send |
| `on_response(from, LeaseResponse)` | the RPC client | on the parent's answer |
| `tick(n)` | the timer | every tick, from a monotonic clock |

An input returns `true` when it took the outbound queue from empty to non-empty: the edge on
which to wake a caller, which drains `ready()` and makes each `Call`. `on_request` returns the
answer instead; the handler sends it back.

## Driving it

```rust,ignore
use leasetree::{Lease, Config, Limit, Action};

// Boot: the limits from configuration, the view from Raft.
let mut lease = Lease::new(my_raft_id, Config { ttl: 40 });
for row in quota_table {
    lease.set_limit(row.key(), Limit { limit: row.per_tick, chunk: row.chunk });
}
lease.set_cluster_view(Some(&instances), leader, term);

// On every system-table change.
wake_if(lease.set_cluster_view(Some(&instances), leader, term));

// The leader sends something over the overlay now and then; every delivery of it tells a
// node which peer is upstream.
wake_if(lease.set_upstream(deliverer));

// The failure detector.
wake_if(lease.down(&dead)); wake_if(lease.up(&back));

// The RPC handler: a child called us.
fn lease_rpc(req: LeaseRequest) -> LeaseResponse { lease.on_request(caller, req) }

// The timer.
wake_if(lease.tick(1));

// The caller fibre, woken on the edge: make each call, feed the answer back.
for Action::Call(peer, req) in lease.ready() {
    let resp = pool.call(peer, "lease", &req).await?;
    wake_if(lease.on_response(peer, resp));
}

// The request path: all or none across the limits a request is subject to.
match lease.acquire(&[(user_rps, 1), (bucket_bps, bytes)]) {
    Ok(()) => serve(),
    Err(denied) => slow_down(denied.key),   // or pause the stream, for bytes per tick
}
```

`acquire` is local and never calls. A refusal marks the limit wanted, and the next `tick`
asks, so the request path has no network side effects. `stats(&key)` has the figures for
metrics: the share, what is lent, what is wanted, the bucket.

A complete driver, with a surrogate network, Raft's view arriving late, a failure detector, a
load, and the measurements, is the `lease-sim` crate in the
[`bcounter`](https://github.com/kostja/bcounter) repository.

## Configuration

Every duration is in ticks; the caller decides what a tick is. There is one knob:

| | value |
|---|---|
| `ttl` | how long a share is good without renewal; default 40 |
| keepalive call | every `ttl / 2` |
| ask again for what is wanted | after a round trip, doubling after each empty answer, up to `ttl / 8` |
| leave a parent | after it missed two deliveries of the leader's traffic |

Drive `tick` from a **monotonic clock**, never wall time. A node compares only its own clock
readings, so skew between nodes is harmless; but a clock that steps back keeps a lapsed share
spendable for as long as it stepped. A pause or a forward jump is fine: a large `tick(n)`
lapses everything at once.

With a tick of 100 ms, `ttl: 40` is a four-second share, renewed every two seconds. A node
whose parent dies re-parents at the next two deliveries and is re-leased within a few ticks,
admitting everything meanwhile.

## The tree from the view

Where membership, liveness and the leader are replicated facts every node learns the same
way -- a Raft cluster's system tables, say -- no overlay is needed to agree on the tree.
`tree::place(me, leader, members, radix)` computes one node's place from the view, and every
node computes the same tree from the same view, at its own wall-clock time. Feed the result
to `set_parent` whenever the view changes; the lease machine tolerates the moments in
between, since a parent books whoever asks.

The rule is a trie over the ids themselves, so a node's position depends on its id alone and
a change moves as few nodes as possible. Inside a failure domain, a node's parent is its id
with the lowest nonzero base-`radix` digit cleared, climbing past ids that are not present;
an id with no present ancestor hangs under the domain's lowest id, the domain's root. The
domain roots hang under the leader. In the leader's own domain the path from the leader up to
the root flips direction, and nothing else moves. So a leader change moves the gateways and
one path of about `log` nodes; a node leaving moves only its children; a node joining lands
at its own place. With radix 4 and two hundred nodes the tree is at most six deep and the
root has at most sixteen children.

Only the view moves the tree. A child whose parent is dead but not yet reported so keeps
calling with backoff, its share lapses after one TTL, and it admits without one, asking,
until the view changes. That interval is the failure detector's delay plus one TTL.

## References

- **Leases, and dating them from the request:** Mike Burrows. *The Chubby lock service for
  loosely-coupled distributed systems.* OSDI 2006. Also Sanjay Ghemawat, Howard Gobioff,
  Shun-Tak Leung. *The Google File System.* SOSP 2003, section 3.1, on chunk leases.
- **The overlay:** João Leitão, José Pereira, Luís Rodrigues. *Epidemic Broadcast Trees.*
  IEEE SRDS 2007; the [`plumtree-fsm`](https://github.com/kostja/plumtree) crate.
- **Stock limits**, the problem this crate no longer tries to solve: the escrow model in the
  [`bcounter`](https://github.com/kostja/bcounter) README, and the eight rules a lease tree
  needs to enforce a stock without Raft in the loop, in this repository's history before
  this version.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
