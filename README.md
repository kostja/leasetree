# leasetree

A pure state machine for distributed quotas. No IO, no clock. You feed it events; it returns
calls to make. It is the third of three small crates that together enforce a cluster-wide
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

A lease is good while it was confirmed in the current term and has not lapsed. On a leader
change a lease is fenced, not dropped: a stock waits for its parent's next answer, which
carries the new term, and a rate keeps admitting. The old tree's bookings stay valid up to the new
leader, which adopts them. What the fence does not cover is a leader cut off with part of the
tree: that side never hears the new term and keeps writing what it already held, until Raft
makes the old leader step down and its children's leases lapse one `ttl` later. That is the
bound: the step-down time plus one `ttl`, and never more than the cut-off side held.

Most keys never have a limit set; they cost nothing, and a node with nothing in play puts
nothing on the wire.

### The protocol: one call

The whole protocol is one RPC from a child to its parent, `LeaseRequest` answered by
`LeaseResponse`. The parent never calls the child. Everything the tree needs rides in that
call, for every limit in play at once:

```rust
pub struct LeaseRequest<Id, K> {
    pub term: u64, pub leader: Option<Id>,  // the child's view; a stale parent learns from it
    pub sent: u64,                          // the child's tick: the lease is dated from it
    pub members: Vec<Id>,                   // the child's subtree, itself included
    pub items: Vec<RequestItem<Id, K>>,
}
pub struct RequestItem<Id, K> {
    pub key: K,
    pub granted: u64,                       // what I hold from you; less than before is a release
    pub wanted: u64,                        // what more I would take
    pub usage: Vec<(Id, u64, u64)>,         // stocks only, when there is news: the map under me
}
pub struct LeaseResponse<Id, K> {
    pub term: u64, pub leader: Option<Id>,  // the parent's view; a stale child learns from it
    pub in_reply_to: u64,                   // the request's `sent`
    pub items: Vec<ResponseItem<K>>,
}
pub struct ResponseItem<K> {
    pub key: K,
    pub grant: u64,                         // this much more is yours
    pub hold: u64,                          // all I book for you; less than you hold is a cut
}
```

A child calls every `ttl / 2` as a keepalive, and at once when what it holds, wants or covers
changed or its term did. While it wants something it also retries: after a round trip at
first, since the parent may be fetching it, then twice as long after each empty answer, up
to `ttl / 8`, so an exhausted quota is not polled hard.

`wanted` is what the child would take. The parent gives what it has, remembers the rest for
this child and asks its own parent for it, and what it fetches for a waiting child it never
hands back as spare; the child finds it on its retry. A release is a smaller `granted`; a
node that moved tells its old parent `granted: 0`. A cut is a `hold` below what the child
holds; the child gives back what it has not spent, and what its own children hold they learn
of in their next answers. Reporting is a stock's business: `usage` is empty for a rate, and
empty means no news.

### The rules

Each was found by a simulation that went wrong without it. The first is spelled out below;
the others are stated in short and explained in the crate docs.

**A lease is dated from the tick its request was sent.** Both ends of a lease keep an expiry.
The child keeps `valid_until`, past which it stops spending; the parent keeps `expires` on its
booking, past which it forgets the booking and may lend that room to someone else. The rule
fixes which clock reading each side uses: the child dates its lease from the tick it *sent*
the request, the parent dates its booking from the tick the request *arrived*. Arrival is
never earlier than sending, so the parent's expiry is always at or after the child's. With a
TTL of 40 and one tick of latency:

| tick | what happens |
|---|---|
| 100 | child C calls parent P |
| 101 | P receives the call, books C until 141, answers |
| 102 | C receives the answer; its lease is good until 140 |

If C never calls again, C stops spending at 140 and P frees the room at 141. The room is
never spendable by C and lendable by P at the same time. Dating the lease from the answer's
arrival instead, good until 142, is the version that reads as safe and is not: P frees the
room at 141, lends it to D, and for two ticks both C and D may write. The overshoot is one
write per node per tick of latency, plus whatever the clocks disagree by; the simulator found
it as a handful of bytes, and it grows with both. The rule costs nothing on the wire: the
answer echoes the request's `sent` tick in `in_reply_to`. Each node compares only its own clock with
itself, so skew between nodes does not matter; what matters is that a node's clock never steps
back, see the configuration section. This is the lease discipline of Chubby and GFS: a holder
dates its lease from the request it sent, not from the reply.

**A child calls on change, and every `ttl / 2` regardless; a parent books what the child
reports.** Two reasons to call, kept apart. The keepalive is time-driven: with no news, a
child still calls every `ttl / 2`, so one lost call does not lapse its lease. Everything
else is change-driven, at the next tick: what the node holds changed, what it wants changed,
what it covers changed, or its term did. And the parent's booking is what the child says it
holds, not what the parent sent: a grant that never arrived, room the child gave back on its
own, a lease adopted from another parent, all reconcile on the next call, and the parent
keeps no memory of what it sent. Parent P has booked child C at 100:

| tick | at C | P books, after C's next call |
|---|---|---|
| 10 | a write is refused | C calls at 11 with `wanted: 100`; P gives what it can, books 200 |
| 14 | C moves under P2 | C tells P2 `granted: 200`; P2 books 200; P still books 200 |
| 16 | P2's first answer arrives | C tells P `granted: 0`; P books 0 |
| 30 | nothing, for twenty ticks | C calls P2 anyway |

Without it: reporting only on the keepalive made every change wait up to `ttl / 2`, so a new
leader's coverage took depth × `ttl / 2` to fill and a moved lease was booked twice for that
long; reporting only on change lost the parent's way to lapse a silent child; and booking what
the parent sent, not what the child reports, left a stale booking behind every lost grant and
every move, a quarter of the shares in one simulator run. The cost is one call per node per
`ttl / 2` when idle, more only while something changes, and none at all with nothing in play.

**A node without a good lease asks for one, whatever the kind decides about the write.**
"Good" is confirmed in the current term and not lapsed. What happens to the write meanwhile
is the kind's decision: a stock refuses, a rate admits. What happens to the lease is not: in
both cases the node marks the limit wanted, and its next call asks. A rate admits without a
lease so that a node is never stalled by the lease system, a leader change or a dead parent
costing a round trip or two. But that is safe only as a transient: the cluster's rate is the
sum of the shares held, and an unleased node is outside it. Without the rule nothing would
make such a node ask, since for a rate no write is ever refused, and it would stay outside
the limit for good. Node C has a rate of 2 per tick and no lease:

| tick | without the rule | with the rule |
|---|---|---|
| 1 | admitted, unleased; nothing asked | admitted, unleased; C calls with `wanted: 2` |
| 3 | admitted, unleased | leased at 2; admitted against the bucket |
| 100 | still admitted, still unleased | throttled at 2 per tick like everyone |

In the simulator, ten of two hundred nodes ran unlimited for a whole run before this rule,
and no table showed it, because a rate's overshoot is not what the tables measure. The cost
is one call, and then the ordinary lease.

**A node that moves tells its old parent it holds nothing from it, but only once the new
parent has answered.** A move has two halves, adoption by the new parent and release by the
old, and the rule fixes the order: adoption first, and only the new parent's answer counts as
adoption. In between, the lease is booked by both parents, on purpose: double-booked room is
room nobody can lend. Releasing first would free the room at the old parent before anyone
had booked the lease, and a lease booked nowhere can be spent twice. C holds 100 from P and
moves to P2, one tick of latency:

| tick | C | P books | P2 books |
|---|---|---|---|
| 14 | calls P2 with `granted: 100` | 100 | 0 |
| 15 | P2 receives it | 100 | 100 |
| 16 | P2's answer arrives; C now tells P `granted: 0` | 100 | 100 |
| 17 | P receives it | 0 | 100 |

The step itself is not needed for safety: the old booking would lapse at `ttl` on its own.
It is there for the room it frees: without it every moved lease is stranded at its old
parent for up to a TTL, and a leader change moves most leases. In the simulator, at N=200 and
TTL 40, a leader change refused five times as many writes without it (5534 against 1089) and
the new leader took 35 ticks instead of 14 to see the true total. If the new parent's answer
never comes, the old parent is never told, the booking lapses at `ttl`, and the lease is
unspendable meanwhile: a false denial, never an overshoot. An answer from anyone but the
current parent carries only the term. The double booking is what the simulator's
`overbooked` column shows during a leader change.

**A parent that booked more than it holds cuts a stock, after a grace period and never below
what the child's subtree has used; a rate is never cut.** A parent ends up having lent more
than it holds by adoption: a child arrives from elsewhere holding 100, the parent books it,
and its bookings exceed its own lease by 100. That is not itself a risk, since a parent with
no room lends nothing. But the excess is spendable below and covered by nothing above. The
parent first asks upward for it; if the quota has room, the over-commit is fetched and
nothing is cut. If not, the parent cuts: its answer's `hold` is below what the child holds.
The child gives back what it has not spent itself, down to the hold. What it had lent to its
own children it cannot give back, so it is now over-committed towards them, and they learn
it in their next answers. A node is never asked to hold less than its subtree has used,
which the child reported in the same call, because that cannot be unspent. P holds 500, has
lent 400 to A, and adopts C, which holds 100 and has used 30, while the quota is full:

| tick | P books | P holds | what happens |
|---|---|---|---|
| 14 | 500 | 500 | C adopted; P asks upward for 100 and gets nothing |
| 19 | 500 | 500 | grace over; C's next call is answered with `hold: 30` |
| 20 | 430 | 500 | C gave back its 70 unspent; P's bookings fit again |

The three qualifiers, and why each. *A stock only:* a rate admits without a lease and so
beyond one; cutting it would only stall nodes, and its over-commit resolves by fetching. *After
a grace period,* `ttl / 8`: a moving lease is booked by both parents for a round trip on
purpose, and a parent that cut on the first call would cut every lease that merely passed
through. *Never below usage:* it cannot be unspent.

What this rule does not do: a cut reaches a child in that child's next call, and a grandchild
in the grandchild's, which with nothing wanted is the keepalive, so it walks down at up to
`ttl / 2` per level, and the subtree keeps writing against the over-committed lease
meanwhile. That is the stock's overshoot: what a subtree writes during the walk, and only
when adoption over-commit could not be fetched, that is, when the quota is full during a
re-orientation. In the simulator at N=200 it is about 1.3% of the limit after a leader change.
Fetching first keeps it rare; only a second, parent-initiated RPC would make the walk fast,
and that is not worth the second RPC: a stock is capacity that fills over days, and a leader
change is seconds. This crate is not a billing boundary.

**A node's parent is the peer the overlay delivers the leader's traffic through; it moves to
a new deliverer once that peer has delivered twice in a row, and never to its own child.**
The lease tree has no shape of its own. It follows the overlay's tree rooted at the leader,
and follows its changes: when plumtree swaps a link for a cheaper one, the deliverer changes
for good, and two deliveries later so does the lease's parent, with one release and one
adoption as under the rule above. What it does not follow is a single detour: a lost copy
fetched from a lazy peer changes the deliverer for one message and then changes back, and
without the two-in-a-row rule every such repair moved a lease. Node C, parent P, the leader
sending every 5 ticks:

| tick | delivered through | in a row | C's parent |
|---|---|---|---|
| 100 | P | | P |
| 105 | Q, P's copy was lost | 1 | P |
| 110 | P | | P |
| 115 | Q, plumtree swapped to Q | 1 | P |
| 120 | Q | 2 | Q; the lease moves |

Measured in the simulator at N=200: with no loss, every node's parent is the peer the
overlay delivers through, in one data centre and across two; at 5% loss, 188 and 176 of 199,
the rest being nodes inside the two-delivery lag of a swap in progress. So the lease tree is
as local to a data centre as the overlay is, and the overlay's cost rules decide that. The
own-child refusal is for the turn-around after a leader change: the old leader L, with child
A whose child L' is now the leader, hears the new leader through A; taking A as parent while
A still had L as parent would book each other's leases in a loop with no path to the leader.
L waits; A switches to L' after two deliveries and releases from L; then L may take A. The
cost of the rule is that a dead parent is left after two of the leader's periods rather than
one, while the lease is still good for a full `ttl`.

The remaining rules, in short:

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
| `on_request(from, LeaseRequest) -> LeaseResponse` | the RPC handler | on a child's call; returns the answer to send |
| `on_response(from, LeaseResponse)` | the RPC client | on the parent's answer |
| `tick(n)` | the timer | every tick |

An input returns `true` when it took the outbound queue from empty to non-empty: the edge on
which to wake a caller, which drains `ready()` and makes each `Call`. `on_request` returns the
answer instead; the handler sends it back.

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

// The RPC handler: a child called us.
fn lease_rpc(req: LeaseRequest) -> LeaseResponse { lease.on_request(caller, req) }

// The timer.
wake_if(lease.tick(1));

// The caller fibre, woken on the edge: make each call, feed the answer back.
for Action::Call(peer, req) in lease.ready() {
    let resp = pool.call(peer, "lease", &req).await?;
    wake_if(lease.on_response(peer, resp));
}

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
| keepalive call | every `ttl / 2` |
| ask again for what is wanted | after a round trip, doubling after each empty answer, up to `ttl / 8` |
| cut a child, once over-committed for | `ttl / 8` |
| leave a parent | after it missed two deliveries of the leader's traffic |
| a new leader's fallback window | `ttl` |

Drive `tick` from a **monotonic clock**, never wall time. A node compares only its own clock
readings, so skew between nodes is harmless; but a clock that steps back keeps a lapsed lease
spendable for as long as it stepped. A pause or a forward jump is fine: a large `tick(n)`
lapses everything at once.

With a tick of 100 ms, `ttl: 40` is a four-second lease, called for every two seconds. A node
whose parent dies re-parents at the next two deliveries and is re-leased within a few ticks. A
new leader lends again as soon as its reports cover every live member, or after four seconds
if some member never speaks: a node with nothing in play sends nothing, and is not counted.

## Open

- **Usage that leaves with a node.** An expelled node's last reported slot lives on in the
  maps that merged it, so the total is right while the cluster is up, but nothing durable
  holds it: after a full restart the leader counts only what live nodes restore from their own
  rows. The test `a_full_restart_after_an_expulsion_keeps_the_expelled_node_s_usage` exposes
  it and is ignored until this is solved. Persisting departed rows would fix the restart but
  not the growth of the map with every node that ever lived; the aim is a bounded fix with no
  change to the RPC or the API.

## References

- **Leases, and dating them from the request:** Mike Burrows. *The Chubby lock service for
  loosely-coupled distributed systems.* OSDI 2006. Also Sanjay Ghemawat, Howard Gobioff,
  Shun-Tak Leung. *The Google File System.* SOSP 2003, section 3.1, on chunk leases.
- **Escrow and the bounded counter:** Valter Balegas et al. *Extending Eventually Consistent
  Cloud Databases for Enforcing Numeric Invariants.* IEEE SRDS 2015
  ([arXiv:1503.09052](https://arxiv.org/abs/1503.09052)); the accounting is the
  [`bcounter`](https://github.com/kostja/bcounter) crate, whose README has the model.
- **The overlay:** João Leitão, José Pereira, Luís Rodrigues. *Epidemic Broadcast Trees.*
  IEEE SRDS 2007; the [`plumtree-fsm`](https://github.com/kostja/plumtree) crate.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
