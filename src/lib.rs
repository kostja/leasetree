// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (c) 2026 Konstantin Osipov.

//! A pure state machine for distributed rate limits.
//!
//! A rate limit is a flow: requests per tick, bytes per tick. The cluster's leader holds the
//! whole rate. It lends shares of it down a spanning tree: to its children, who lend out of
//! what they hold to their own. Every node runs a token bucket from its share and admits
//! against it locally, with no message on the request path. Shares have a TTL and lapse
//! when not renewed.
//!
//! This crate is the algorithm only. It reads no clock and no socket, and it does not know
//! who the leader is or which peer is upstream: the caller tells it. It is meant to sit
//! beside `plumtree-fsm`, whose spanning tree it follows, but knows nothing of any network.
//!
//! # What a rate limit is for
//!
//! Availability first. A node without a share, because it has none yet, or its parent died,
//! or the leader changed, admits everything and asks for a share; it is throttled again once
//! it has one. So the limit can be overshot for a round trip or two around such events, and
//! that is the trade: a rate limit shapes load, it is not a boundary. A limit that must never
//! be exceeded, a stock of bytes or objects, is a different problem and not this crate.
//!
//! # One call
//!
//! The whole protocol is one RPC from a child to its parent: a [`LeaseRequest`] answered by
//! a [`LeaseResponse`]. The child calls every `ttl / 2`, at once when what it holds or wants
//! changed or the term did, and while it wants something: after a round trip at first, then
//! twice as long after each empty answer, up to `ttl / 8`. A request reports, per limit,
//! what the child holds from the parent (less than before is a release) and what more it
//! wants, apart from what it *needs*: what it has already lent beyond its share, a moved
//! subtree it adopted. A response grants. The parent serves needs before wants, from all the
//! tree, and reserves room for a child's need it could not fill; what it fetches for a
//! waiting child it never hands back as spare. Without that, a share that moved is counted
//! twice for as long as the rate is contended. The parent never calls the child.
//!
//! # The contract
//!
//! Every input mutates state; some append to one FIFO outbound queue of calls to make,
//! drained with [`ready`](Lease::ready), and those return `true` when they took the queue
//! from empty to non-empty: the edge on which to wake a caller. [`on_request`](Lease::on_request)
//! returns the response directly, for the RPC handler to send back. Enforcement,
//! [`acquire`](Lease::acquire), is local and never calls; a refusal only marks the limit
//! wanted, and the next [`tick`](Lease::tick) asks.
//!
//! What the caller supplies:
//!
//! - the limits, from its configuration: [`set_limit`](Lease::set_limit);
//! - the cluster view, from Raft: [`set_cluster_view`](Lease::set_cluster_view) -- members,
//!   leader and term;
//! - the upstream peer, from its overlay: [`set_upstream`](Lease::set_upstream) -- whichever
//!   peer last delivered the leader's traffic;
//! - liveness, from its failure detector: [`down`](Lease::down) and [`up`](Lease::up);
//! - the network and the clock: [`on_request`](Lease::on_request),
//!   [`on_response`](Lease::on_response) and [`tick`](Lease::tick).
//!
//! # The rules
//!
//! - A share is dated from the tick the request was *sent*, so a parent that lapses it
//!   (dated from arrival, later) never re-lends a share the child still considers its own.
//!   No clock is ever compared across nodes: the answer carries the parent's tick, and the
//!   child echoes the last one it applied, so the parent can tell on its own clock whether
//!   a report includes its last gift.
//! - A child calls whenever what it holds or wants changed, or the term did, and every
//!   `ttl / 2` as a keepalive. A parent books what the child reports.
//! - A node without a share admits everything and asks for one.
//! - A node's parent is the peer the overlay delivers the leader's traffic through; it moves
//!   to a new deliverer once that peer has delivered twice in a row, and never to its own
//!   child. The lease tree follows the overlay's tree and its changes, two deliveries behind,
//!   and does not follow a single detour.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::collections::{BTreeMap, BTreeSet};

/// Timing, in ticks. The caller decides what a tick is, and drives [`tick`](Lease::tick)
/// from a **monotonic** clock: a node compares only its own clock readings, so skew between
/// nodes is harmless, but a clock that steps back keeps a lapsed share spendable for as long
/// as it stepped. A pause or a forward jump is fine: a large `tick(n)` lapses everything at
/// once.
#[derive(Clone, Copy, Debug)]
pub struct Config {
    /// A share is good for this long without renewal. Renewed at `ttl / 2`. A request is
    /// repeated after `ttl / 8` at most.
    pub ttl: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config { ttl: 40 }
    }
}

/// One rate limit, as configured. Every node needs `chunk`; only the leader uses `limit`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limit {
    /// Units per tick, cluster-wide.
    pub limit: u64,
    /// How much a node asks for at a time, and keeps in hand when idle.
    pub chunk: u64,
}

/// One limit in a [`LeaseRequest`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RequestItem<K> {
    /// The limit.
    pub key: K,
    /// What the child holds from the parent. Less than before is a release; zero to an old
    /// parent lets it go entirely.
    pub granted: u64,
    /// What the child has already lent beyond its share: a moved subtree it adopted. Served
    /// before any `wanted`, by everyone up the tree, or a moved share is counted twice for
    /// as long as the rate is contended.
    pub needed: u64,
    /// What more the child would take.
    pub wanted: u64,
}

/// child -> parent: the one call. A report and a request in one, for every limit in play.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct LeaseRequest<Id, K> {
    /// The child's term.
    pub term: u64,
    /// The child's leader.
    pub leader: Option<Id>,
    /// The child's tick. A share is dated from it.
    pub sent: u64,
    /// The `sent` of the last answer the child applied from this parent, so the parent can
    /// tell, on its own clock, whether the report includes its last gift.
    pub seen: u64,
    /// The limits.
    pub items: Vec<RequestItem<K>>,
}

/// One limit in a [`LeaseResponse`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ResponseItem<K> {
    /// The limit.
    pub key: K,
    /// This much more is the child's.
    pub grant: u64,
}

/// parent -> child: the answer. A stale leader learns of its successor from `term` on the
/// first answer it gets; so does a child from any ancestor's.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct LeaseResponse<Id, K> {
    /// The parent's term.
    pub term: u64,
    /// The parent's leader.
    pub leader: Option<Id>,
    /// The `sent` of the request answered.
    pub in_reply_to: u64,
    /// The parent's tick. The child echoes it as `seen` in its next request.
    pub sent: u64,
    /// The limits, in the request's order.
    pub items: Vec<ResponseItem<K>>,
}

/// Something the caller must do.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action<Id, K> {
    /// Call this peer, and feed its answer to [`on_response`](Lease::on_response).
    Call(Id, LeaseRequest<Id, K>),
}

/// An `acquire` that could not be honoured.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Denied<K> {
    /// The first limit that refused.
    pub key: K,
    /// Tokens it had.
    pub available: u64,
}

/// A limit as this node sees it, for metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    /// The share this node holds, in units per tick.
    pub granted: u64,
    /// What it lent to its children.
    pub lent: u64,
    /// What it lent beyond what it holds.
    pub overcommit: u64,
    /// What it still wants from its parent.
    pub wanted: u64,
    /// Children it books for this limit.
    pub children: usize,
    /// Tokens in the bucket.
    pub tokens: u64,
    /// Whether the share is good right now.
    pub good: bool,
}

/// What a parent books for one child on one limit.
#[derive(Clone, Debug)]
struct Booking {
    granted: u64,
    expires: u64,
    /// When we last gave this child more: a report sent before that is stale by the gift.
    given_at: u64,
    /// What the child asked for and did not get: earmarked, so that what we fetch for it is
    /// not handed back as spare before it asks again.
    wanted: u64,
    /// What the child needs and did not get: reserved, so that no want is served from room
    /// while a need is outstanding.
    needed: u64,
}

/// One limit's state on this node.
struct Quota<Id: Ord + Clone> {
    limit: Limit,
    /// The share: units per tick.
    granted: u64,
    tokens: f64,
    lent: u64,
    children: BTreeMap<Id, Booking>,
    /// Until when the share is good; `None` while there is none.
    valid_until: Option<u64>,
    /// How much more we want from the parent; asked at the next tick.
    wanted: u64,
    /// The share last reported to the parent, or given by it: a change is reported once
    /// more, even to zero.
    reported: u64,
    last_request: Option<u64>,
    /// Asks answered with nothing, in a row: the next one waits twice as long, up to
    /// `ttl / 8`.
    refusals: u32,
    /// Share a child gave back or lapsed: fetched for it, and handed back up at the next
    /// call, or a moved share would be counted twice.
    returning: u64,
}

impl<Id: Ord + Clone> Quota<Id> {
    fn new(limit: Limit) -> Self {
        Quota {
            limit,
            granted: 0,
            tokens: 0.0,
            lent: 0,
            children: BTreeMap::new(),
            valid_until: None,
            wanted: 0,
            reported: 0,
            last_request: None,
            refusals: 0,
            returning: 0,
        }
    }
    /// What we may still lend.
    fn room(&self) -> u64 {
        self.granted.saturating_sub(self.lent)
    }
    fn overcommit(&self) -> u64 {
        self.lent.saturating_sub(self.granted)
    }
    /// Our own refill: the share minus what we lent.
    fn refill(&self) -> f64 {
        self.room() as f64
    }
    /// What the children asked for and did not get.
    fn earmarked(&self) -> u64 {
        self.children.values().map(|b| b.wanted).sum()
    }
    /// What the children need and did not get.
    fn reserved(&self) -> u64 {
        self.children.values().map(|b| b.needed).sum()
    }
    /// What we need from the parent: what we and our children lent beyond our share.
    fn need(&self) -> u64 {
        self.overcommit().saturating_add(self.reserved())
    }
    /// What we would take from the parent beyond that: our own want and the children's.
    fn want(&self) -> u64 {
        self.wanted.saturating_add(self.earmarked())
    }
    /// Everything we ask for.
    fn ask(&self) -> u64 {
        self.need().saturating_add(self.want())
    }
}

/// One node's lease state, for every limit.
pub struct Lease<Id: Ord + Clone, K: Ord + Clone> {
    me: Id,
    cfg: Config,
    now: u64,
    term: u64,
    leader: Option<Id>,
    members: BTreeSet<Id>,
    down: BTreeSet<Id>,
    parent: Option<Id>,
    /// Deliveries of the leader's traffic through other peers since the parent last delivered.
    parent_misses: u32,
    quotas: BTreeMap<K, Quota<Id>>,
    /// The `sent` of the last answer applied from the current parent.
    seen: u64,
    /// Something changed since the last call: call at the next tick.
    dirty: bool,
    last_call: u64,
    outbound: Vec<Action<Id, K>>,
}

impl<Id: Ord + Clone, K: Ord + Clone> Lease<Id, K> {
    /// A new node. It holds nothing until the leader lends to it, or until it is the leader.
    pub fn new(me: Id, cfg: Config) -> Self {
        Lease {
            me,
            cfg,
            now: 0,
            term: 0,
            leader: None,
            members: BTreeSet::new(),
            down: BTreeSet::new(),
            parent: None,
            parent_misses: 0,
            quotas: BTreeMap::new(),
            seen: 0,
            dirty: false,
            last_call: 0,
            outbound: Vec::new(),
        }
    }

    // ------------------------------------------------------------ configuration

    /// Add or change a limit. On the leader, the share becomes the whole rate at once.
    pub fn set_limit(&mut self, key: K, limit: Limit) -> bool {
        let was_empty = self.outbound.is_empty();
        let leader = self.is_leader();
        let q = self.quotas.entry(key).or_insert_with(|| Quota::new(limit));
        q.limit = limit;
        if leader {
            Self::hold_limit(q);
        }
        self.woke(was_empty)
    }

    /// Forget a limit: tell the parent we hold nothing of it, and drop the key entirely.
    /// Children holding it lapse; their own configuration drops it too.
    pub fn remove_limit(&mut self, key: &K) -> bool {
        let was_empty = self.outbound.is_empty();
        if let Some(q) = self.quotas.remove(key) {
            if let Some(p) = self.parent.clone() {
                if q.granted > 0 || q.reported > 0 {
                    let req = self.request_for(std::slice::from_ref(key));
                    self.outbound.push(Action::Call(p, req));
                }
            }
        }
        self.woke(was_empty)
    }

    // ------------------------------------------------------------ the cluster

    /// The cluster's members, its leader, and the term, as the caller's Raft shows them. Call
    /// it on every change; `members` may be omitted when only the leader changed. A term older
    /// than the current one is ignored.
    pub fn set_cluster_view(&mut self, members: Option<&[Id]>, leader: Id, term: u64) -> bool {
        let was_empty = self.outbound.is_empty();
        if let Some(m) = members {
            self.members = m.iter().cloned().collect();
            let gone: Vec<Id> = self
                .children()
                .filter(|c| !self.members.contains(c))
                .cloned()
                .collect();
            for c in gone {
                self.forget_child(&c);
            }
            if self
                .parent
                .as_ref()
                .is_some_and(|p| !self.members.contains(p))
            {
                self.parent = None;
            }
        }
        if term < self.term {
            return self.woke(was_empty);
        }
        let changed = term > self.term || self.leader.as_ref() != Some(&leader);
        let was_leader = self.is_leader();
        let old_leader = self.leader.clone();
        self.term = term;
        self.leader = Some(leader.clone());
        if changed {
            if leader == self.me && !was_leader {
                self.crown();
            } else if leader != self.me && was_leader {
                self.demote();
            }
            // The old leader is most likely gone: a node under it takes the first upstream
            // it is offered rather than waiting for two deliveries.
            let leader_changed = old_leader.as_ref() != Some(&leader);
            if leader_changed && self.parent.is_some() && self.parent == old_leader {
                self.parent = None;
                self.parent_misses = 0;
            }
            // A new term: call now, so the new tree books what we hold.
            self.dirty = true;
        }
        self.woke(was_empty)
    }

    /// `peer` just delivered the leader's traffic: it is on a live path to the leader. Taken
    /// as parent if we have none, or if the current parent missed the last two deliveries;
    /// never if it is our own child.
    pub fn set_upstream(&mut self, peer: Id) -> bool {
        let was_empty = self.outbound.is_empty();
        if self.is_leader() || peer == self.me {
            return false;
        }
        if self.parent.as_ref() == Some(&peer) {
            self.parent_misses = 0;
            return false;
        }
        self.parent_misses += 1;
        if self.parent.is_some() && self.parent_misses < 2 {
            return false;
        }
        if self.children().any(|c| *c == peer) {
            return false;
        }
        self.reparent(peer);
        self.woke(was_empty)
    }

    /// `peers` are unreachable. A child among them is lapsed at once; a parent among them is
    /// left, and the next delivery picks a new one.
    pub fn down(&mut self, peers: &[Id]) -> bool {
        for p in peers {
            self.down.insert(p.clone());
            if self.children().any(|c| c == p) {
                self.forget_child(p);
            }
            if self.parent.as_ref() == Some(p) {
                self.parent = None;
                self.parent_misses = 0;
            }
        }
        false
    }

    /// `peers` are reachable again. They re-attach by themselves.
    pub fn up(&mut self, peers: &[Id]) -> bool {
        for p in peers {
            self.down.remove(p);
        }
        false
    }

    // ------------------------------------------------------------ the protocol

    /// A call from a child. Returns the answer, for the RPC handler to send back.
    pub fn on_request(&mut self, from: Id, req: LeaseRequest<Id, K>) -> LeaseResponse<Id, K> {
        if req.term > self.term {
            if let Some(l) = req.leader.clone() {
                self.set_cluster_view(None, l, req.term);
            }
        }
        let ttl = self.cfg.ttl;
        let now = self.now;
        let can_ask = self.parent.is_some();
        let mut items = Vec::with_capacity(req.items.len());
        for it in req.items {
            let Some(q) = self.quotas.get_mut(&it.key) else {
                continue;
            };
            // Book what the child reports. A report sent before our last gift reached the
            // child does not include it: keep the booking.
            let (old, given_at) = q
                .children
                .get(&from)
                .map_or((0, 0), |b| (b.granted, b.given_at));
            let booked = if req.seen >= given_at {
                it.granted
            } else {
                old.max(it.granted)
            };
            if old != booked {
                self.dirty = true;
            }
            if booked < old {
                q.returning += old - booked;
            }
            q.lent = q.lent - old + booked;
            // Needs first, from all the room; wants only from room not reserved for other
            // children's needs. Remember the rest and ask upward for it.
            let others_need = q.reserved() - q.children.get(&from).map_or(0, |b| b.needed);
            let give_need = it.needed.min(q.room());
            let free = q
                .room()
                .saturating_sub(give_need)
                .saturating_sub(others_need);
            let give_want = it.wanted.min(free);
            let give = give_need + give_want;
            q.lent += give;
            let short_need = it.needed - give_need;
            let short_want = it.wanted - give_want;
            if (short_need > 0 || short_want > 0) && can_ask {
                // New demand from below: ask upward at the next tick.
                q.last_request = None;
            }
            let b = q.children.entry(from.clone()).or_insert(Booking {
                granted: 0,
                expires: 0,
                given_at: 0,
                wanted: 0,
                needed: 0,
            });
            b.granted = booked + give;
            if give > 0 {
                b.given_at = now;
            }
            b.expires = now + ttl;
            b.wanted = short_want;
            b.needed = short_need;
            items.push(ResponseItem {
                key: it.key,
                grant: give,
            });
        }
        // A child that holds nothing and wants nothing of any limit is no longer a child.
        let still = self.quotas.values().any(|q| {
            q.children
                .get(&from)
                .is_some_and(|b| b.granted > 0 || b.wanted > 0 || b.needed > 0)
        });
        if !still {
            for q in self.quotas.values_mut() {
                q.children.remove(&from);
            }
        }
        LeaseResponse {
            term: self.term,
            leader: self.leader.clone(),
            in_reply_to: req.sent,
            sent: now,
            items,
        }
    }

    /// The answer to a call. Answers from anyone but the current parent only carry news of
    /// the term.
    pub fn on_response(&mut self, from: Id, resp: LeaseResponse<Id, K>) -> bool {
        let was_empty = self.outbound.is_empty();
        if resp.term > self.term {
            if let Some(l) = resp.leader.clone() {
                self.set_cluster_view(None, l, resp.term);
            }
        }
        if self.parent.as_ref() != Some(&from) {
            return self.woke(was_empty);
        }
        let ttl = self.cfg.ttl;
        self.seen = self.seen.max(resp.sent);
        for it in resp.items {
            let Some(q) = self.quotas.get_mut(&it.key) else {
                continue;
            };
            q.granted += it.grant;
            if it.grant > 0 {
                q.reported += it.grant;
                // What arrived goes to the children waiting for it first; only the rest
                // counts against our own want, or a child would take what we asked for.
                let spoken_for = q.need().saturating_add(q.earmarked()).min(it.grant);
                q.wanted = q.wanted.saturating_sub(it.grant - spoken_for);
                q.refusals = 0;
            } else if q.ask() > 0 {
                q.refusals = q.refusals.saturating_add(1);
            }
            q.valid_until = Some(q.valid_until.unwrap_or(0).max(resp.in_reply_to + ttl));
        }
        self.woke(was_empty)
    }

    /// Advance the clock by `n` ticks: refill the buckets, lapse children, drop lapsed
    /// shares, and call the parent when it is time.
    pub fn tick(&mut self, n: u64) -> bool {
        let was_empty = self.outbound.is_empty();
        self.now = self.now.saturating_add(n);
        let now = self.now;
        let leader = self.is_leader();
        for q in self.quotas.values_mut() {
            let refill = q.refill();
            q.tokens = (q.tokens + refill * n as f64).min(refill.max(1.0) * 2.0);
            let gone: Vec<Id> = q
                .children
                .iter()
                .filter(|(_, b)| b.expires < now)
                .map(|(c, _)| c.clone())
                .collect();
            for c in gone {
                if let Some(b) = q.children.remove(&c) {
                    q.lent -= b.granted;
                    q.returning += b.granted;
                    self.dirty = true;
                }
            }
            // A lapsed share has been re-lent by the parent: keep only what our children
            // hold, so the next report does not claim it.
            // No share at all counts as lapsed too: a demoted leader's leftover, say.
            let lapsed = q.valid_until.map_or(true, |v| now > v);
            if !leader && lapsed && q.granted > q.lent {
                q.granted = q.lent;
                self.dirty = true;
            }
        }
        if !leader && self.parent.is_some() && self.call_due() {
            self.call();
        }
        self.woke(was_empty)
    }

    /// Drain the outbound queue, in FIFO order.
    #[must_use]
    pub fn ready(&mut self) -> Vec<Action<Id, K>> {
        std::mem::take(&mut self.outbound)
    }

    /// Whether the outbound queue holds anything.
    #[must_use]
    pub fn has_ready(&self) -> bool {
        !self.outbound.is_empty()
    }

    // ------------------------------------------------------------ enforcement

    /// Draw `amount` tokens on each limit, all or none. A limit not configured is unlimited.
    /// Without a good share a node admits, and asks for one. A refusal, or a missing share,
    /// marks the limit wanted; the next tick asks.
    pub fn acquire(&mut self, keys: &[(K, u64)]) -> Result<(), Denied<K>> {
        let leader = self.is_leader();
        let now = self.now;
        let mut denied = None;
        for (key, amount) in keys {
            let Some(q) = self.quotas.get_mut(key) else {
                continue;
            };
            let good = leader || q.valid_until.is_some_and(|v| now <= v);
            let have = q.tokens.floor() as u64;
            if !good || have < *amount {
                // Short, or unleased: ask. A key newly in play is reported at once, so the
                // parent books us.
                if q.wanted == 0 {
                    self.dirty = true;
                }
                q.wanted = q.wanted.max(q.limit.chunk).max(*amount);
            }
            if good && have < *amount && denied.is_none() {
                denied = Some(Denied {
                    key: key.clone(),
                    available: have,
                });
            }
        }
        if let Some(d) = denied {
            return Err(d);
        }
        for (key, amount) in keys {
            let Some(q) = self.quotas.get_mut(key) else {
                continue;
            };
            let good = leader || q.valid_until.is_some_and(|v| now <= v);
            if good {
                q.tokens -= *amount as f64;
            }
        }
        Ok(())
    }

    // ------------------------------------------------------------ observation

    /// Whether this node is the leader.
    pub fn is_leader(&self) -> bool {
        self.leader.as_ref() == Some(&self.me)
    }

    /// This node's parent in the tree, if any.
    pub fn parent(&self) -> Option<&Id> {
        self.parent.as_ref()
    }

    /// This node's children in the tree: the peers it books anything for.
    pub fn children(&self) -> impl Iterator<Item = &Id> {
        let mut out: Vec<&Id> = self
            .quotas
            .values()
            .flat_map(|q| q.children.keys())
            .collect();
        out.sort();
        out.dedup();
        out.into_iter()
    }

    /// A limit's figures, for metrics.
    pub fn stats(&self, key: &K) -> Option<Stats> {
        let q = self.quotas.get(key)?;
        Some(Stats {
            granted: q.granted,
            lent: q.lent,
            overcommit: q.overcommit(),
            wanted: q.wanted,
            children: q.children.len(),
            tokens: q.tokens.floor() as u64,
            good: self.is_leader() || q.valid_until.is_some_and(|v| self.now <= v),
        })
    }

    /// The current term.
    pub fn term(&self) -> u64 {
        self.term
    }

    // ------------------------------------------------------------ internals

    fn woke(&self, was_empty: bool) -> bool {
        was_empty && !self.outbound.is_empty()
    }

    fn retry_cap(ttl: u64) -> u64 {
        (ttl / 8).max(1)
    }

    fn hold_limit(q: &mut Quota<Id>) {
        q.granted = q.limit.limit.max(q.lent);
        q.valid_until = Some(u64::MAX);
        q.wanted = 0;
        q.refusals = 0;
    }

    /// Become the leader: hold the whole rate, and let the old parent go.
    fn crown(&mut self) {
        let keys: Vec<K> = self
            .quotas
            .iter()
            .filter(|(_, q)| q.granted > 0 || q.reported > 0)
            .map(|(k, _)| k.clone())
            .collect();
        for q in self.quotas.values_mut() {
            Self::hold_limit(q);
        }
        if let Some(op) = self.parent.take() {
            if !keys.is_empty() {
                let req = self.request_for(&keys);
                self.outbound.push(Action::Call(op, req));
            }
        }
        self.parent_misses = 0;
    }

    /// Stop being the leader: keep what is lent, drop the rest of the rate.
    fn demote(&mut self) {
        for q in self.quotas.values_mut() {
            q.granted = q.lent;
            q.valid_until = None;
            q.reported = 0;
        }
        self.parent = None;
        self.parent_misses = 0;
    }

    fn forget_child(&mut self, c: &Id) {
        for q in self.quotas.values_mut() {
            if let Some(b) = q.children.remove(c) {
                q.lent -= b.granted;
                q.returning += b.granted;
                self.dirty = true;
            }
        }
    }

    fn reparent(&mut self, peer: Id) {
        let now = self.now;
        // The old parent books what we hold: tell it we hold nothing from it now. What we
        // hold, we report to the new parent, which adopts it.
        let keys: Vec<K> = self
            .quotas
            .iter()
            .filter(|(_, q)| q.granted > 0 || q.reported > 0)
            .map(|(k, _)| k.clone())
            .collect();
        for q in self.quotas.values_mut() {
            let gone: Vec<Id> = q
                .children
                .iter()
                .filter(|(_, b)| b.expires < now)
                .map(|(c, _)| c.clone())
                .collect();
            for c in gone {
                if let Some(b) = q.children.remove(&c) {
                    q.lent -= b.granted;
                }
            }
            if q.valid_until.is_some_and(|v| now > v) && q.granted > q.lent {
                q.granted = q.lent;
            }
        }
        if let Some(op) = self.parent.take() {
            if !keys.is_empty() {
                let req = self.request_for(&keys);
                self.outbound.push(Action::Call(op, req));
            }
        }
        self.parent = Some(peer);
        self.parent_misses = 0;
        self.seen = 0;
        for q in self.quotas.values_mut() {
            // The new parent has given us nothing yet; what we hold is all news to it.
            q.reported = 0;
            q.refusals = 0;
        }
        self.call();
    }

    /// A call that tells `to` we hold nothing from it: to an old parent, or for a removed
    /// limit.
    fn request_for(&self, keys: &[K]) -> LeaseRequest<Id, K> {
        LeaseRequest {
            term: self.term,
            leader: self.leader.clone(),
            sent: self.now,
            seen: self.seen,
            items: keys
                .iter()
                .map(|k| RequestItem {
                    key: k.clone(),
                    granted: 0,
                    needed: 0,
                    wanted: 0,
                })
                .collect(),
        }
    }

    /// Whether it is time to call the parent: something changed, the keepalive is due, or
    /// something is wanted and the retry has run out. A retry waits a round trip at first --
    /// the parent may be fetching it -- and twice as long after each empty answer, up to
    /// `ttl / 8`.
    fn call_due(&self) -> bool {
        let now = self.now;
        if self.dirty || now >= self.last_call + self.cfg.ttl / 2 {
            return true;
        }
        let cap = Self::retry_cap(self.cfg.ttl);
        self.quotas.values().any(|q| {
            let wait = (2u64 << q.refusals.saturating_sub(1).min(16))
                .min(cap)
                .max(1);
            q.ask() > 0 && q.last_request.map_or(true, |t| now >= t + wait)
        })
    }

    /// Call the parent: every limit in play, what we hold and what we want, after handing
    /// back what we do not use. Nothing in play: nothing on the wire.
    fn call(&mut self) {
        let Some(parent) = self.parent.clone() else {
            return;
        };
        let now = self.now;
        self.dirty = false;
        self.last_call = now;
        let mut items = Vec::new();
        for (key, q) in self.quotas.iter_mut() {
            let in_play = q.granted > 0 || q.reported > 0 || q.lent > 0 || q.ask() > 0;
            if !in_play {
                continue;
            }
            // What a child gave back was fetched for it and is the tree's, not ours: hand it
            // back up, whatever we want ourselves, or a share that moved is used twice, here
            // and under its new parent. If we want more, we ask like anyone.
            if q.returning > 0 {
                let back = q.returning.min(q.room());
                q.granted -= back;
                q.returning = 0;
            }
            // While the bucket is full we are not using our share: keep one chunk beyond
            // what we lent, and never what a child is waiting for; hand back the rest.
            let full = q.tokens >= q.refill().max(1.0) * 2.0;
            if full {
                let keep = q
                    .lent
                    .saturating_add(q.limit.chunk)
                    .saturating_add(q.earmarked())
                    .saturating_add(q.reserved());
                if q.granted > keep {
                    q.granted = keep;
                }
            }
            let (need, want) = (q.need(), q.want());
            if need > 0 || want > 0 {
                q.last_request = Some(now);
            }
            q.reported = q.granted;
            items.push(RequestItem {
                key: key.clone(),
                granted: q.granted,
                needed: need,
                wanted: want,
            });
        }
        if items.is_empty() {
            return;
        }
        let req = LeaseRequest {
            term: self.term,
            leader: self.leader.clone(),
            sent: now,
            seen: self.seen,
            items,
        };
        self.outbound.push(Action::Call(parent, req));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RPS: &str = "rps";

    fn rate() -> Limit {
        Limit {
            limit: 30,
            chunk: 2,
        }
    }

    fn node(id: u32) -> Lease<u32, &'static str> {
        let mut n = Lease::new(id, Config { ttl: 40 });
        n.set_limit(RPS, rate());
        n
    }

    /// Make every queued call among `nodes` (ids 1..) and feed back the answers, until quiet.
    fn route(nodes: &mut [Lease<u32, &'static str>]) {
        for _ in 0..16 {
            let mut moved = false;
            for i in 0..nodes.len() {
                let from = nodes[i].me;
                for Action::Call(to, req) in nodes[i].ready() {
                    let resp = nodes[(to - 1) as usize].on_request(from, req);
                    nodes[i].on_response(to, resp);
                    moved = true;
                }
            }
            if !moved {
                break;
            }
        }
    }

    fn dump(ns: &[Lease<u32, &'static str>]) -> String {
        ns.iter()
            .map(|n| {
                format!(
                    "{}: {:?} parent {:?}",
                    n.me,
                    n.stats(&RPS).unwrap(),
                    n.parent()
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// One tick everywhere, then every call and answer.
    fn step(ns: &mut [Lease<u32, &'static str>]) {
        for n in ns.iter_mut() {
            n.tick(1);
        }
        route(ns);
    }

    /// A leader (1) and a child (2), linked.
    fn pair() -> Vec<Lease<u32, &'static str>> {
        let mut ns = vec![node(1), node(2)];
        for n in ns.iter_mut() {
            n.set_cluster_view(Some(&[1, 2]), 1, 1);
        }
        ns[1].set_upstream(1);
        route(&mut ns);
        ns
    }

    #[test]
    fn the_leader_holds_the_rate_and_everyone_else_nothing() {
        let mut ns = pair();
        assert!(ns[0].is_leader());
        assert_eq!(ns[0].stats(&RPS).unwrap().granted, 30);
        assert_eq!(ns[1].stats(&RPS).unwrap().granted, 0);
        ns[0].tick(1);
        assert!(ns[0].acquire(&[(RPS, 1)]).is_ok());
    }

    #[test]
    fn a_node_without_a_share_admits_and_asks() {
        let mut ns = pair();
        assert!(ns[1].acquire(&[(RPS, 1)]).is_ok(), "no share: admitted");
        step(&mut ns);
        assert_eq!(
            ns[1].stats(&RPS).unwrap().granted,
            2,
            "and it asked for a chunk"
        );
        assert!(ns[1].stats(&RPS).unwrap().good);
        // Unleased, any amount is admitted; leased, the bucket decides.
        let mut lone = node(3);
        lone.set_cluster_view(Some(&[1, 3]), 1, 1);
        assert!(lone.acquire(&[(RPS, 100)]).is_ok(), "any amount");
    }

    #[test]
    fn a_share_is_a_token_bucket() {
        let mut ns = pair();
        let _ = ns[1].acquire(&[(RPS, 1)]);
        step(&mut ns);
        ns[1].tick(1);
        assert!(ns[1].acquire(&[(RPS, 1)]).is_ok());
        assert!(ns[1].acquire(&[(RPS, 1)]).is_ok());
        assert_eq!(
            ns[1].acquire(&[(RPS, 1)]).unwrap_err().key,
            RPS,
            "two per tick"
        );
        ns[1].tick(1);
        assert!(ns[1].acquire(&[(RPS, 1)]).is_ok());
    }

    #[test]
    fn a_throttled_node_asks_for_more_and_an_idle_one_hands_it_back() {
        let mut ns = pair();
        let _ = ns[1].acquire(&[(RPS, 1)]);
        step(&mut ns);
        // Throttled: it asks for another chunk.
        ns[1].tick(1);
        let _ = ns[1].acquire(&[(RPS, 1)]);
        let _ = ns[1].acquire(&[(RPS, 1)]);
        let _ = ns[1].acquire(&[(RPS, 1)]);
        for _ in 0..3 {
            step(&mut ns);
        }
        assert_eq!(ns[1].stats(&RPS).unwrap().granted, 4);
        assert_eq!(ns[0].stats(&RPS).unwrap().lent, 4);
        // Idle: the bucket fills, and the next keepalive hands back all but one chunk.
        for _ in 0..25 {
            step(&mut ns);
        }
        assert_eq!(ns[1].stats(&RPS).unwrap().granted, 2);
        assert_eq!(ns[0].stats(&RPS).unwrap().lent, 2);
    }

    #[test]
    fn a_share_lapses_without_renewal_and_the_node_admits_meanwhile() {
        let mut ns = pair();
        let _ = ns[1].acquire(&[(RPS, 1)]);
        step(&mut ns);
        assert_eq!(ns[1].stats(&RPS).unwrap().granted, 2);
        ns[1].tick(41); // no exchange: the parent never answers
        let s = ns[1].stats(&RPS).unwrap();
        assert!(!s.good);
        assert_eq!(s.granted, 0, "the share was dropped");
        assert!(
            ns[1].acquire(&[(RPS, 5)]).is_ok(),
            "and the node admits anyway"
        );
        ns[0].tick(42);
        assert_eq!(
            ns[0].stats(&RPS).unwrap().lent,
            0,
            "the parent lapsed it too"
        );
    }

    #[test]
    fn the_child_s_share_ends_before_the_parent_s_booking() {
        let mut ns = pair();
        let _ = ns[1].acquire(&[(RPS, 1)]);
        // The call goes at 100, arrives at 101, is answered at 102.
        ns[1].tick(100);
        ns[0].tick(101);
        let Action::Call(_, req) = ns[1].ready().remove(0);
        let resp = ns[0].on_request(2, req);
        ns[1].on_response(1, resp);
        assert_eq!(ns[1].stats(&RPS).unwrap().granted, 2);
        ns[1].tick(40); // 140: still good
        assert!(ns[1].stats(&RPS).unwrap().good);
        ns[1].tick(1); // 141: lapsed
        assert!(!ns[1].stats(&RPS).unwrap().good);
        ns[0].tick(40); // 141: the parent still books it
        assert_eq!(ns[0].stats(&RPS).unwrap().lent, 2);
        ns[0].tick(1); // 142: and only now lets it go
        assert_eq!(ns[0].stats(&RPS).unwrap().lent, 0);
    }

    #[test]
    fn a_shortfall_is_fetched_by_the_parent_and_found_on_the_child_s_retry() {
        let mut ns = vec![node(1), node(2), node(3)];
        for n in ns.iter_mut() {
            n.set_cluster_view(Some(&[1, 2, 3]), 1, 1);
        }
        ns[1].set_upstream(1);
        ns[2].set_upstream(2);
        route(&mut ns);
        let _ = ns[2].acquire(&[(RPS, 1)]);
        ns[2].tick(1);
        let Action::Call(to, req) = ns[2].ready().remove(0);
        assert_eq!(to, 2);
        let resp = ns[1].on_request(3, req);
        assert_eq!(resp.items[0].grant, 0, "the middle node has nothing");
        ns[2].on_response(2, resp);
        ns[1].tick(1);
        route(&mut ns);
        ns[2].tick(2);
        route(&mut ns);
        assert_eq!(ns[2].stats(&RPS).unwrap().granted, 2);
    }

    #[test]
    fn a_refused_ask_backs_off_up_to_ttl_over_eight() {
        let mut l = node(1);
        l.set_cluster_view(Some(&[1, 2]), 1, 1);
        l.set_limit(RPS, Limit { limit: 0, chunk: 2 }); // nothing to lend
        let mut c = node(2);
        c.set_cluster_view(Some(&[1, 2]), 1, 1);
        c.set_upstream(1);
        let _ = c.acquire(&[(RPS, 1)]);
        let mut asked_at = Vec::new();
        for t in 1..=19 {
            c.tick(1);
            for Action::Call(_, req) in c.ready() {
                if req.items.iter().any(|i| i.wanted > 0) {
                    asked_at.push(t);
                }
                let resp = l.on_request(2, req);
                c.on_response(1, resp);
            }
        }
        let gaps: Vec<u64> = asked_at.windows(2).map(|w| w[1] - w[0]).collect();
        assert_eq!(&gaps[..3], &[2, 4, 5], "{gaps:?}");
    }

    #[test]
    fn a_node_with_nothing_in_play_is_silent() {
        let mut c = node(2);
        c.set_cluster_view(Some(&[1, 2]), 1, 1);
        c.set_upstream(1);
        assert!(c.ready().is_empty());
        c.tick(50);
        assert!(c.ready().is_empty(), "nor a keepalive");
    }

    #[test]
    fn removing_a_limit_hands_it_back_and_forgets_it() {
        let mut ns = pair();
        let _ = ns[1].acquire(&[(RPS, 1)]);
        step(&mut ns);
        assert_eq!(ns[0].stats(&RPS).unwrap().lent, 2);
        ns[1].remove_limit(&RPS);
        route(&mut ns);
        assert_eq!(ns[0].stats(&RPS).unwrap().lent, 0);
        assert!(ns[1].stats(&RPS).is_none());
    }

    #[test]
    fn a_stale_leader_steps_down_on_the_first_call_with_a_newer_term() {
        let mut ns = pair();
        let _ = ns[1].acquire(&[(RPS, 1)]);
        step(&mut ns);
        // Raft moved on; the child heard, the old leader did not.
        ns[1].set_cluster_view(None, 2, 2);
        assert!(ns[1].is_leader());
        assert_eq!(ns[1].stats(&RPS).unwrap().granted, 30);
        route(&mut ns); // the crown tells the old parent
        assert!(!ns[0].is_leader());
        assert_eq!(ns[0].term(), 2);
        assert_eq!(ns[0].stats(&RPS).unwrap().lent, 0);
        ns[0].tick(1);
        assert_eq!(
            ns[0].stats(&RPS).unwrap().granted,
            0,
            "the rate was dropped"
        );
    }

    #[test]
    fn the_parent_is_kept_while_it_delivers_and_left_after_two_misses() {
        let mut c = node(3);
        c.set_cluster_view(Some(&[1, 2, 3]), 1, 1);
        c.set_upstream(1);
        assert_eq!(c.parent(), Some(&1));
        c.set_upstream(2);
        assert_eq!(c.parent(), Some(&1), "one miss is noise");
        c.set_upstream(1);
        c.set_upstream(2);
        assert_eq!(c.parent(), Some(&1), "the parent delivered in between");
        c.set_upstream(2);
        assert_eq!(c.parent(), Some(&2), "two misses in a row");
    }

    #[test]
    fn a_node_never_takes_its_own_child_as_parent() {
        let mut ns = vec![node(1), node(2), node(3)];
        for n in ns.iter_mut() {
            n.set_cluster_view(Some(&[1, 2, 3]), 3, 1);
        }
        ns[1].set_upstream(1);
        let _ = ns[1].acquire(&[(RPS, 1)]);
        step(&mut ns); // 1 now books 2
        ns[0].set_upstream(2);
        ns[0].set_upstream(2);
        assert_eq!(ns[0].parent(), None);
    }

    #[test]
    fn a_move_releases_the_old_parent_and_the_new_one_adopts() {
        let mut ns = vec![node(1), node(2), node(3), node(4)];
        for n in ns.iter_mut() {
            n.set_cluster_view(Some(&[1, 2, 3, 4]), 1, 1);
        }
        ns[1].set_upstream(1);
        ns[2].set_upstream(1);
        ns[3].set_upstream(2);
        route(&mut ns);
        for n in ns[1..].iter_mut() {
            let _ = n.acquire(&[(RPS, 1)]);
        }
        for _ in 0..6 {
            step(&mut ns);
        }
        assert_eq!(ns[3].stats(&RPS).unwrap().granted, 2);
        assert_eq!(ns[1].stats(&RPS).unwrap().lent, 2, "2 books 4");
        ns[3].set_upstream(3);
        ns[3].set_upstream(3);
        route(&mut ns);
        assert_eq!(ns[1].stats(&RPS).unwrap().lent, 0, "the old parent let go");
        assert_eq!(ns[2].stats(&RPS).unwrap().lent, 2, "the new one adopted");
        assert_eq!(
            ns[3].stats(&RPS).unwrap().granted,
            2,
            "the child kept its share"
        );
    }

    #[test]
    fn an_answer_from_anyone_but_the_parent_carries_only_the_term() {
        let mut ns = pair();
        let _ = ns[1].acquire(&[(RPS, 1)]);
        step(&mut ns);
        ns[1].on_response(
            9,
            LeaseResponse {
                term: 5,
                leader: Some(9),
                in_reply_to: 0,
                sent: 0,
                items: vec![ResponseItem {
                    key: RPS,
                    grant: 500,
                }],
            },
        );
        assert_eq!(ns[1].stats(&RPS).unwrap().granted, 2);
        assert_eq!(ns[1].term(), 5);
    }

    #[test]
    fn an_acquire_is_all_or_none() {
        let mut l = node(1);
        l.set_limit("bps", Limit { limit: 5, chunk: 1 });
        l.set_cluster_view(Some(&[1]), 1, 1);
        l.tick(1);
        assert!(l.acquire(&[(RPS, 1), ("bps", 100)]).is_err());
        assert_eq!(l.stats(&RPS).unwrap().tokens, 30, "nothing drawn");
        assert!(l.acquire(&[(RPS, 1), ("bps", 1)]).is_ok());
        assert_eq!(l.stats(&RPS).unwrap().tokens, 29);
    }

    #[test]
    fn a_share_a_child_gave_back_goes_back_up() {
        let mut ns = vec![node(1), node(2), node(3)];
        for n in ns.iter_mut() {
            n.set_cluster_view(Some(&[1, 2, 3]), 1, 1);
        }
        ns[1].set_upstream(1);
        ns[2].set_upstream(2);
        route(&mut ns);
        let _ = ns[2].acquire(&[(RPS, 1)]);
        for _ in 0..6 {
            step(&mut ns);
        }
        assert_eq!(ns[2].stats(&RPS).unwrap().granted, 2);
        assert_eq!(ns[1].stats(&RPS).unwrap().lent, 2);
        assert_eq!(
            ns[0].stats(&RPS).unwrap().lent,
            2,
            "the leader lent 2 to the middle node"
        );
        // The grandchild moves under the leader; the middle node is told.
        ns[2].set_upstream(1);
        ns[2].set_upstream(1);
        route(&mut ns);
        assert_eq!(ns[1].stats(&RPS).unwrap().lent, 0);
        // The middle node does not keep the 2 for itself: at its next call it hands it up.
        for _ in 0..3 {
            step(&mut ns);
        }
        assert_eq!(
            ns[1].stats(&RPS).unwrap().granted,
            0,
            "handed back, it wanted nothing"
        );
        assert_eq!(
            ns[0].stats(&RPS).unwrap().lent,
            2,
            "the leader books only the grandchild"
        );
    }

    #[test]
    fn a_moved_share_is_needed_and_served_before_any_want() {
        // The rate is fully lent: A and B hold 2 each, D holds 2 under B. D moves under A: A
        // is over-committed by 2, B gives the 2 back up, and C wants 2 at the same time. A's
        // need is served first.
        let mut ns: Vec<Lease<u32, &'static str>> = (1..=5)
            .map(|id| {
                let mut n = Lease::new(id, Config { ttl: 40 });
                n.set_limit(RPS, Limit { limit: 6, chunk: 2 });
                n
            })
            .collect();
        for n in ns.iter_mut() {
            n.set_cluster_view(Some(&[1, 2, 3, 4, 5]), 1, 1);
        }
        ns[1].set_upstream(1); // A
        ns[2].set_upstream(1); // B
        ns[3].set_upstream(3); // D under B
        ns[4].set_upstream(1); // C
        route(&mut ns);
        // A, B and D keep writing, as real nodes do; a write that fails asks again.
        for _ in 0..16 {
            for i in [1, 2, 3] {
                let _ = ns[i].acquire(&[(RPS, 1)]);
            }
            step(&mut ns);
        }
        assert_eq!(
            ns[0].stats(&RPS).unwrap().lent,
            6,
            "fully lent\n{}",
            dump(&ns)
        );
        ns[3].set_upstream(2);
        ns[3].set_upstream(2); // D moves under A
        for _ in 0..12 {
            for i in [1, 2, 3, 4] {
                let _ = ns[i].acquire(&[(RPS, 1)]); // C wants too, and keeps asking
            }
            step(&mut ns);
        }
        let a = ns[1].stats(&RPS).unwrap();
        assert_eq!(a.overcommit, 0, "A's need was served\n{}", dump(&ns));
        assert_eq!(ns[4].stats(&RPS).unwrap().granted, 0, "C's want was not");
        let refills: u64 = ns
            .iter()
            .map(|n| {
                let s = n.stats(&RPS).unwrap();
                s.granted.saturating_sub(s.lent)
            })
            .sum();
        assert!(refills <= 6, "the rate is not over-allocated: {refills}");
    }
}
