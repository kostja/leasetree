// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (c) 2026 Konstantin Osipov.

//! A pure state machine for distributed quotas.
//!
//! Every node of a cluster holds a lease on each limit it uses. Leases are handed down a
//! spanning tree rooted at the cluster's leader: the leader holds the whole limit, lends
//! chunks to its children, and each child lends out of what it holds to its own. Usage is
//! reported up the same tree as a per-node map, merged at every level, so the leader's total
//! is exact and a branch that moves is never counted twice. Every lease has a TTL and is
//! fenced by the leader's term.
//!
//! This crate is the algorithm only. It reads no clock and no socket, and it does not know
//! who the leader is or which peer is upstream: the caller tells it. It is built on
//! [`bcounter`] for the accounting and is meant to sit beside `plumtree-fsm` for the
//! overlay, but knows nothing of either network.
//!
//! # Two kinds of limit
//!
//! A **stock** is a total: bytes stored, objects stored. A node draws on its lease, reports
//! what it drew, and gives back a delete. The leader's `usage` is the cluster's total. A stock
//! is precious: without a good lease -- lapsed, or not yet confirmed in the current term -- a
//! node refuses, and a false denial is the price.
//!
//! A **rate** is a flow: requests or bytes per tick. A node holds a share of the refill and
//! runs a token bucket from it. Nothing is reported; a share not renewed lapses. A rate is
//! about availability: without a good lease a node admits everything and asks for a lease.
//!
//! # One call
//!
//! The whole protocol is one RPC from a child to its parent: a [`LeaseRequest`] answered by a
//! [`LeaseResponse`]. The child calls every `ttl / 2`, at once when what it holds, wants or
//! covers changed, and while it wants something: after a round trip at first, then twice as
//! long after each empty answer, up to `ttl / 8`. A request reports, per limit, what the
//! child holds from the parent (less than before is a release), what more it wants, and, for
//! a stock, the usage map of its subtree. A response grants, and tells the child all the
//! parent books for it (less than it holds is a cut). The parent gives what it has, remembers
//! the rest for this child and asks its own parent for it, and what it fetches for a waiting
//! child it never hands back as spare. The parent never calls the child.
//!
//! # The contract
//!
//! Every input mutates state; some append to one FIFO outbound queue of calls to make,
//! drained with [`ready`](Lease::ready), and those return `true` when they took the queue from
//! empty to non-empty: the edge on which to wake a caller. [`on_request`](Lease::on_request)
//! returns the response directly, for the RPC handler to send back. Enforcement,
//! [`acquire`](Lease::acquire) and [`release`](Lease::release), is local and never calls; a
//! refusal only marks the limit wanted, and the next [`tick`](Lease::tick) asks.
//!
//! What the caller supplies:
//!
//! - the limits, from its configuration, a stock's own usage from durable storage inside:
//!   [`set_limit`](Lease::set_limit);
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
//! Each of these was found necessary by a simulation that went wrong without it.
//!
//! - A lease is dated from the tick the request was *sent*, so a parent that lapses it
//!   (dated from arrival, later) never re-lends room the child still considers its own.
//! - A child calls whenever what it holds, wants or covers changed, or its term did, and
//!   every `ttl / 2` as a keepalive. A parent books what the child reports.
//! - A node without a good lease asks for one whatever the kind decides about the write.
//! - A node that moves to a new parent reports to the old one that it holds nothing from it,
//!   but only once the new parent has confirmed the adoption; until then both book it and
//!   nobody re-lends it.
//! - A parent that booked more than it holds cuts a stock only after a grace period, never
//!   below what the child's subtree has used; the child gives back what it can and passes
//!   the rest on to its own children in their next answers. A rate is never cut.
//! - A node keeps its parent while that parent keeps delivering the leader's traffic, and
//!   never takes its own child as parent.
//! - A node drops a lease the moment it lapses: the parent has re-lent that room.
//! - A new leader grants nothing and cuts nobody until its reports cover every live member,
//!   or one `ttl` has passed.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::collections::{BTreeMap, BTreeSet};

use bcounter::BCounter;

/// Timing, in ticks. The caller decides what a tick is, and drives [`tick`](Lease::tick)
/// from a **monotonic** clock: a node compares only its own clock readings, so skew between
/// nodes is harmless, but a clock that steps back keeps a lapsed lease spendable for as long
/// as it stepped. A pause or a forward jump is fine: a large `tick(n)` lapses everything at
/// once.
#[derive(Clone, Copy, Debug)]
pub struct Config {
    /// A lease is good for this long without renewal. Renewed at `ttl / 2`. A request is
    /// repeated, and a cut waits, `ttl / 8`.
    pub ttl: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config { ttl: 40 }
    }
}

/// One limit, as configured. Which arm it is decides what a node does without a good lease.
/// Every node needs `chunk`; only the leader uses `limit`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Limit {
    /// A flow: `limit` units per tick, asked for `chunk` at a time. A share of the refill,
    /// run as a token bucket. Admitted without a lease.
    Rate {
        /// Units per tick, cluster-wide.
        limit: u64,
        /// How much a node asks for at a time, and keeps in hand when idle.
        chunk: u64,
    },
    /// A stock: bytes, objects. Drawn on, reported, given back. Refused without a lease.
    /// Comes with this node's own usage from durable storage; usage only grows and merges by
    /// max, so passing it again later, even stale, is harmless.
    Stock {
        /// Units, cluster-wide.
        limit: u64,
        /// How much a node asks for at a time, and keeps in hand when idle.
        chunk: u64,
        /// What this node has ever acquired.
        acquired: u64,
        /// What this node has ever released.
        released: u64,
    },
}

impl Limit {
    fn is_stock(&self) -> bool {
        matches!(self, Limit::Stock { .. })
    }
    fn limit(&self) -> u64 {
        match self {
            Limit::Rate { limit, .. } | Limit::Stock { limit, .. } => *limit,
        }
    }
    fn chunk(&self) -> u64 {
        match self {
            Limit::Rate { chunk, .. } | Limit::Stock { chunk, .. } => *chunk,
        }
    }
}

/// One limit in a [`LeaseRequest`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestItem<Id, K> {
    /// The limit.
    pub key: K,
    /// What the child holds from the parent. Less than before is a release; zero to an old
    /// parent lets it go entirely.
    pub granted: u64,
    /// What more the child would take.
    pub wanted: u64,
    /// Stocks only, when there is news: `(node, acquired, released)` for every node under the
    /// child, itself included. Empty means no change.
    pub usage: Vec<(Id, u64, u64)>,
}

/// child -> parent: the one call. A report and a request in one, for every limit in play.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseRequest<Id, K> {
    /// The child's term.
    pub term: u64,
    /// The child's leader.
    pub leader: Option<Id>,
    /// The child's tick. A lease is dated from it.
    pub sent: u64,
    /// The child's subtree, itself included.
    pub members: Vec<Id>,
    /// The limits.
    pub items: Vec<RequestItem<Id, K>>,
}

/// One limit in a [`LeaseResponse`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResponseItem<K> {
    /// The limit.
    pub key: K,
    /// This much more is the child's.
    pub grant: u64,
    /// All the parent books for the child. Less than the child holds is a cut.
    pub hold: u64,
}

/// parent -> child: the answer. A stale leader learns of its successor from `term` on the
/// first answer it gets; so does a child from any ancestor's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseResponse<Id, K> {
    /// The parent's term.
    pub term: u64,
    /// The parent's leader.
    pub leader: Option<Id>,
    /// The `sent` of the request answered.
    pub in_reply_to: u64,
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
    /// What it could have given.
    pub available: u64,
}

/// A limit as this node sees it, for metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    /// What this node holds.
    pub granted: u64,
    /// This node's own usage (stocks).
    pub used: u64,
    /// What it lent to its children.
    pub lent: u64,
    /// What it lent beyond what it holds.
    pub overcommit: u64,
    /// What it still wants from its parent.
    pub wanted: u64,
    /// Children it books for this limit.
    pub children: usize,
    /// Tokens in the bucket (rates).
    pub tokens: u64,
    /// Whether the lease is good right now.
    pub good: bool,
    /// Until when, in ticks (`u64::MAX` on the leader).
    pub valid_until: u64,
}

/// What a parent books for one child on one limit.
#[derive(Clone, Debug)]
struct Booking {
    granted: u64,
    /// The child's subtree usage from its last report: the floor for a cut.
    used: u64,
    expires: u64,
    /// When we last gave this child more: a report sent before that is stale by the gift.
    given_at: u64,
    /// What the child asked for and did not get: earmarked, so that what we fetch for it is
    /// not handed back as spare before it asks again.
    wanted: u64,
}

/// One limit's state on this node.
struct Quota<Id: Ord + Clone> {
    limit: Limit,
    /// The grant and, for a stock, the usage map: our own slot and the children's, merged.
    cap: BCounter<Id>,
    /// A rate's bucket.
    tokens: f64,
    lent: u64,
    children: BTreeMap<Id, Booking>,
    /// The term this lease was last confirmed in, and until when it is good.
    term: u64,
    valid_until: u64,
    /// How much more we want from the parent; asked at the next tick.
    wanted: u64,
    /// The grant last reported to the parent, or given by it: a change is reported once
    /// more, even to zero.
    reported: u64,
    last_request: Option<u64>,
    /// Asks answered with nothing, in a row: the next one waits twice as long, up to
    /// `ttl / 8`.
    refusals: u32,
    /// Since when we have lent more than we hold.
    over_since: Option<u64>,
}

impl<Id: Ord + Clone> Quota<Id> {
    fn new(me: Id, limit: Limit) -> Self {
        Quota {
            limit,
            cap: BCounter::new(me, 0),
            tokens: 0.0,
            lent: 0,
            children: BTreeMap::new(),
            term: 0,
            valid_until: 0,
            wanted: 0,
            reported: 0,
            last_request: None,
            refusals: 0,
            over_since: None,
        }
    }
    /// What we hold and have not used (stocks) or hold (rates).
    fn available(&self) -> u64 {
        self.cap.local_available()
    }
    /// What we may still lend or spend.
    fn room(&self) -> u64 {
        self.available().saturating_sub(self.lent)
    }
    fn overcommit(&self) -> u64 {
        self.lent.saturating_sub(self.available())
    }
    fn refill(&self) -> f64 {
        self.cap.granted().saturating_sub(self.lent) as f64
    }
    /// What the children asked for and did not get.
    fn earmarked(&self) -> u64 {
        self.children.values().map(|b| b.wanted).sum()
    }
    /// The parent books less than we hold: give the difference back, what is unspent of it.
    /// What we cannot give back our children hold; they get it in their next answers.
    fn cut_to(&mut self, hold: u64) -> bool {
        let cut = self.cap.granted().saturating_sub(hold);
        if cut == 0 {
            return false;
        }
        let _ = self.cap.reclaim(cut);
        true
    }
}

/// A tree link to a child: what it last reported about its subtree.
struct Link<Id> {
    members: BTreeSet<Id>,
    expires: u64,
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
    leader_since: u64,
    links: BTreeMap<Id, Link<Id>>,
    quotas: BTreeMap<K, Quota<Id>>,
    /// After a parent change: the old parent, told that we hold nothing from it once the new
    /// parent has confirmed the adoption.
    owed: Option<(Id, Vec<K>)>,
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
            leader_since: 0,
            links: BTreeMap::new(),
            quotas: BTreeMap::new(),
            owed: None,
            dirty: false,
            last_call: 0,
            outbound: Vec::new(),
        }
    }

    // ------------------------------------------------------------ configuration

    /// Add or change a limit. A stock's usage is merged in (by max, so it may be repeated);
    /// on the leader, the grant becomes the limit at once.
    pub fn set_limit(&mut self, key: K, limit: Limit) -> bool {
        let was_empty = self.outbound.is_empty();
        let me = self.me.clone();
        let (leader, term) = (self.is_leader(), self.term);
        let q = self
            .quotas
            .entry(key)
            .or_insert_with(|| Quota::new(me.clone(), limit));
        q.limit = limit;
        if let Limit::Stock {
            acquired, released, ..
        } = limit
        {
            q.cap.apply(&[(me, acquired, released)]);
        }
        if leader {
            Self::hold_limit(q, term);
        }
        self.woke(was_empty)
    }

    /// Forget a limit: tell the parent we hold nothing of it, and drop the key entirely.
    /// Children holding it lapse; their own configuration drops it too.
    pub fn remove_limit(&mut self, key: &K) -> bool {
        let was_empty = self.outbound.is_empty();
        if let Some(q) = self.quotas.remove(key) {
            if let Some(p) = self.parent.clone() {
                if q.cap.granted() > 0 || q.reported > 0 {
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
                .links
                .keys()
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
        self.term = term;
        self.leader = Some(leader.clone());
        if changed {
            if leader == self.me && !was_leader {
                self.crown();
            } else if leader != self.me && was_leader {
                self.demote();
            }
            // A new term: call now, so the answer confirms the lease in it.
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
        if self.links.contains_key(&peer) {
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
            if self.links.contains_key(p) {
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
        let grace = Self::grace(ttl);
        // The link: what the child covers.
        let members: BTreeSet<Id> = req.members.iter().cloned().collect();
        let link = self.links.entry(from.clone()).or_insert(Link {
            members: BTreeSet::new(),
            expires: 0,
        });
        if link.members != members {
            // Our subtree changed: report it up at once, so a new leader's coverage does not
            // wait for the periodic call.
            self.dirty = true;
        }
        link.members = members;
        link.expires = now + ttl;
        // With the link in place: is a new leader still waiting to hear from everyone?
        let settling = self.is_leader() && !self.may_grant();
        let can_ask = self.parent.is_some();

        let mut items = Vec::with_capacity(req.items.len());
        for it in req.items {
            let Some(q) = self.quotas.get_mut(&it.key) else {
                continue;
            };
            let used: u64 = it.usage.iter().map(|(_, a, r)| a.saturating_sub(*r)).sum();
            if q.limit.is_stock() && !it.usage.is_empty() {
                q.cap.apply(&it.usage);
            }
            // Book what the child reports. A report sent before our last gift reached the
            // child does not include it: keep the booking.
            let (old, given_at) = q
                .children
                .get(&from)
                .map_or((0, 0), |b| (b.granted, b.given_at));
            let booked = if req.sent > given_at {
                it.granted
            } else {
                old.max(it.granted)
            };
            if old != booked {
                self.dirty = true;
            }
            q.lent = q.lent - old + booked;
            // Give what we can of what is wanted; earmark and ask upward for the rest.
            let give = if settling {
                0
            } else {
                it.wanted.min(q.available().saturating_sub(q.lent))
            };
            q.lent += give;
            let short = it.wanted - give;
            if short > 0 && !settling && can_ask {
                if q.wanted == 0 {
                    q.last_request = None;
                }
                q.wanted = q.wanted.max(short);
            }
            let overcommit = q.overcommit();
            let is_stock = q.limit.is_stock();
            let b = q.children.entry(from.clone()).or_insert(Booking {
                granted: 0,
                used: 0,
                expires: 0,
                given_at: 0,
                wanted: 0,
            });
            b.granted = booked + give;
            if give > 0 {
                b.given_at = now;
            }
            if !it.usage.is_empty() {
                b.used = used;
            }
            b.expires = now + ttl;
            b.wanted = short;
            // Under a stock, what we booked beyond our own lease has to come back: the answer
            // tells the child to keep less, down to what its subtree used. Not while a new
            // leader is settling, and not before a round trip: a moving lease is booked by
            // both parents for that long, on purpose.
            let long_enough = q.over_since.is_some_and(|s| now >= s + grace);
            let hold = if !is_stock || settling || !long_enough {
                b.granted
            } else {
                b.granted - overcommit.min(b.granted.saturating_sub(b.used))
            };
            items.push(ResponseItem {
                key: it.key,
                grant: give,
                hold,
            });
        }
        // A child that holds nothing and wants nothing of any limit is no longer a child.
        let still = self.quotas.values().any(|q| {
            q.children
                .get(&from)
                .is_some_and(|b| b.granted > 0 || b.wanted > 0)
        });
        if !still {
            self.links.remove(&from);
            for q in self.quotas.values_mut() {
                q.children.remove(&from);
            }
        }
        LeaseResponse {
            term: self.term,
            leader: self.leader.clone(),
            in_reply_to: req.sent,
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
        for it in resp.items {
            let Some(q) = self.quotas.get_mut(&it.key) else {
                continue;
            };
            q.cap.grant(it.grant);
            if it.grant > 0 {
                q.reported += it.grant;
                q.wanted = q.wanted.saturating_sub(it.grant);
                q.refusals = 0;
            } else if q.wanted > 0 {
                q.refusals = q.refusals.saturating_add(1);
            }
            q.term = q.term.max(resp.term);
            q.valid_until = q.valid_until.max(resp.in_reply_to + ttl);
            if q.cut_to(it.hold) {
                self.dirty = true;
            }
        }
        // The parent has booked us: the old parent may let go.
        if let Some((op, keys)) = self.owed.take() {
            let req = self.request_for(&keys);
            self.outbound.push(Action::Call(op, req));
        }
        self.woke(was_empty)
    }

    /// Advance the clock by `n` ticks: refill rates, lapse children, drop lapsed leases, and
    /// call the parent when it is time.
    pub fn tick(&mut self, n: u64) -> bool {
        let was_empty = self.outbound.is_empty();
        self.now = self.now.saturating_add(n);
        let now = self.now;
        let leader = self.is_leader();

        for q in self.quotas.values_mut() {
            if !q.limit.is_stock() {
                let refill = q.refill();
                q.tokens = (q.tokens + refill * n as f64).min(refill.max(1.0) * 2.0);
            }
            let gone: Vec<Id> = q
                .children
                .iter()
                .filter(|(_, b)| b.expires < now)
                .map(|(c, _)| c.clone())
                .collect();
            for c in gone {
                if let Some(b) = q.children.remove(&c) {
                    q.lent -= b.granted;
                    self.dirty = true;
                }
            }
            q.over_since = if q.overcommit() > 0 {
                q.over_since.or(Some(now))
            } else {
                None
            };
        }
        let gone: Vec<Id> = self
            .links
            .iter()
            .filter(|(_, l)| l.expires < now)
            .map(|(c, _)| c.clone())
            .collect();
        for c in gone {
            self.links.remove(&c);
        }
        if !leader {
            self.drop_lapsed();
            if self.parent.is_some() && self.call_due() {
                self.call();
            }
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

    /// Draw `amount` on each limit, all or none. A limit not configured is unlimited. A stock
    /// draws on the held lease and refuses without a good one; a rate draws tokens and admits
    /// without a good one. A refusal, or a missing lease, marks the limit wanted; the next
    /// tick asks.
    pub fn acquire(&mut self, keys: &[(K, u64)]) -> Result<(), Denied<K>> {
        let leader = self.is_leader();
        let (term, now) = (self.term, self.now);
        for (key, amount) in keys {
            let Some(q) = self.quotas.get_mut(key) else {
                continue;
            };
            let good = leader || (q.term == term && now <= q.valid_until);
            let have = if q.limit.is_stock() {
                q.room()
            } else {
                q.tokens.floor() as u64
            };
            let rate = !q.limit.is_stock();
            let ok = (good && have >= *amount) || (!good && rate);
            if !good || have < *amount {
                // Short, or unleased: ask, whatever becomes of this write. A key newly in
                // play is reported at once, so the parent links us and the leader counts us.
                if q.wanted == 0 {
                    self.dirty = true;
                }
                q.wanted = q.wanted.max(q.limit.chunk()).max(*amount);
            }
            if !ok {
                return Err(Denied {
                    key: key.clone(),
                    available: have,
                });
            }
        }
        for (key, amount) in keys {
            let Some(q) = self.quotas.get_mut(key) else {
                continue;
            };
            let good = leader || (q.term == term && now <= q.valid_until);
            match q.limit {
                Limit::Stock { .. } => {
                    if !good || q.room() < *amount {
                        // Writing without a lease: self-granted, reported, absorbed above.
                        q.cap.grant(*amount);
                    }
                    q.cap.acquire(*amount).ok();
                }
                Limit::Rate { .. } => {
                    if good {
                        q.tokens -= *amount as f64;
                    }
                }
            }
        }
        Ok(())
    }

    /// Give `amount` of a stock back: a delete, an abort, an expiry.
    pub fn release(&mut self, key: &K, amount: u64) {
        if let Some(q) = self.quotas.get_mut(key) {
            if q.limit.is_stock() {
                q.cap.release(amount);
            }
        }
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

    /// This node's children in the tree.
    pub fn children(&self) -> impl Iterator<Item = &Id> {
        self.links.keys()
    }

    /// This node's view of a stock's usage under it, itself included. Exact at the leader,
    /// up to report lag.
    pub fn usage(&self, key: &K) -> u64 {
        self.quotas.get(key).map_or(0, |q| q.cap.global_used())
    }

    /// A limit's figures, for metrics.
    pub fn stats(&self, key: &K) -> Option<Stats> {
        let q = self.quotas.get(key)?;
        Some(Stats {
            granted: q.cap.granted(),
            used: q.cap.granted().saturating_sub(q.cap.local_available()),
            lent: q.lent,
            overcommit: q.overcommit(),
            wanted: q.wanted,
            children: q.children.len(),
            tokens: q.tokens.floor() as u64,
            good: self.is_leader() || (q.term == self.term && self.now <= q.valid_until),
            valid_until: q.valid_until,
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

    fn grace(ttl: u64) -> u64 {
        (ttl / 8).max(1)
    }

    fn hold_limit(q: &mut Quota<Id>, term: u64) {
        let more = q.limit.limit().saturating_sub(q.cap.granted());
        q.cap.grant(more);
        let over = q.cap.granted().saturating_sub(q.limit.limit());
        let _ = q.cap.reclaim(over);
        q.term = term;
        q.valid_until = u64::MAX;
        q.wanted = 0;
        q.refusals = 0;
    }

    /// Become the leader: hold every limit outright, and let the old parent go.
    fn crown(&mut self) {
        let term = self.term;
        self.leader_since = self.now;
        let keys: Vec<K> = self
            .quotas
            .iter()
            .filter(|(_, q)| q.cap.granted() > 0 || q.reported > 0)
            .map(|(k, _)| k.clone())
            .collect();
        for q in self.quotas.values_mut() {
            Self::hold_limit(q, term);
        }
        if let Some(op) = self.parent.take() {
            if !keys.is_empty() {
                let req = self.request_for(&keys);
                self.outbound.push(Action::Call(op, req));
            }
        }
        if let Some((op, keys)) = self.owed.take() {
            let req = self.request_for(&keys);
            self.outbound.push(Action::Call(op, req));
        }
        self.parent_misses = 0;
    }

    /// Stop being the leader: keep what is used or lent, drop the rest of the root grant.
    fn demote(&mut self) {
        for q in self.quotas.values_mut() {
            let excess = q.room();
            let _ = q.cap.reclaim(excess);
            q.valid_until = 0;
            q.reported = 0;
        }
        self.parent = None;
        self.parent_misses = 0;
    }

    fn forget_child(&mut self, c: &Id) {
        self.links.remove(c);
        for q in self.quotas.values_mut() {
            if let Some(b) = q.children.remove(c) {
                q.lent -= b.granted;
                self.dirty = true;
            }
        }
    }

    /// A lapsed lease has been re-lent by the parent: drop what is not spent or lent, so the
    /// next report does not claim it and the next answer cannot revive it.
    fn drop_lapsed(&mut self) {
        let now = self.now;
        for q in self.quotas.values_mut() {
            if now > q.valid_until {
                let room = q.room();
                if q.cap.reclaim(room) > 0 {
                    self.dirty = true;
                }
            }
        }
    }

    fn reparent(&mut self, peer: Id) {
        let now = self.now;
        // The old parent books what we held before dropping anything: it is told we hold
        // nothing from it, once the new parent has confirmed us.
        let keys: Vec<K> = self
            .quotas
            .iter()
            .filter(|(_, q)| q.cap.granted() > 0 || q.reported > 0)
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
        }
        self.drop_lapsed();
        // A release still owed to an even older parent goes out now; this one waits for the
        // new parent's confirmation.
        if let Some((op, keys)) = self.owed.take() {
            let req = self.request_for(&keys);
            self.outbound.push(Action::Call(op, req));
        }
        if let Some(op) = self.parent.take() {
            if !keys.is_empty() {
                self.owed = Some((op, keys));
            }
        }
        self.parent = Some(peer);
        self.parent_misses = 0;
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
            members: Vec::new(),
            items: keys
                .iter()
                .map(|k| RequestItem {
                    key: k.clone(),
                    granted: 0,
                    wanted: 0,
                    usage: Vec::new(),
                })
                .collect(),
        }
    }

    fn subtree(&self) -> Vec<Id> {
        let mut out = BTreeSet::new();
        out.insert(self.me.clone());
        for (c, l) in &self.links {
            out.insert(c.clone());
            out.extend(l.members.iter().cloned());
        }
        out.into_iter().collect()
    }

    /// The leader's rule: nothing new until the reports cover every live member, or one
    /// `ttl` has passed since the change.
    fn may_grant(&self) -> bool {
        if self.now >= self.leader_since + self.cfg.ttl {
            return true;
        }
        let covered: BTreeSet<Id> = self.subtree().into_iter().collect();
        self.members
            .iter()
            .filter(|m| !self.down.contains(m))
            .all(|m| covered.contains(m))
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
        let cap = Self::grace(self.cfg.ttl);
        self.quotas.values().any(|q| {
            let want = q.wanted.max(q.overcommit());
            let wait = (2u64 << q.refusals.saturating_sub(1).min(16))
                .min(cap)
                .max(1);
            want > 0 && q.last_request.map_or(true, |t| now >= t + wait)
        })
    }

    /// Call the parent: every limit in play, what we hold, what we want, and, for a stock,
    /// the usage map under us, plus what we can hand back. Nothing in play: nothing on the
    /// wire.
    fn call(&mut self) {
        let Some(parent) = self.parent.clone() else {
            return;
        };
        let now = self.now;
        let full = self.dirty || now >= self.last_call + self.cfg.ttl / 2;
        self.dirty = false;
        self.last_call = now;
        let mut items = Vec::new();
        for (key, q) in self.quotas.iter_mut() {
            let in_play = q.cap.granted() > 0
                || q.reported > 0
                || q.lent > 0
                || q.wanted > 0
                || (q.limit.is_stock() && q.cap.global_used() > 0);
            if !in_play {
                continue;
            }
            // Hand back room beyond two chunks (a stock), or a share beyond what is lent plus
            // one chunk while the bucket is full (a rate); never what a child is waiting for.
            let keep = match q.limit {
                Limit::Stock { .. } => 2 * q.limit.chunk(),
                Limit::Rate { .. } => {
                    if q.tokens >= q.refill().max(1.0) * 2.0 {
                        q.limit.chunk()
                    } else {
                        u64::MAX
                    }
                }
            }
            .saturating_add(q.earmarked());
            let spare = q.room();
            if spare > keep {
                let _ = q.cap.reclaim(spare - keep);
            }
            let want = q.wanted.max(q.overcommit());
            if want > 0 {
                q.last_request = Some(now);
            }
            q.reported = q.cap.granted();
            items.push(RequestItem {
                key: key.clone(),
                granted: q.cap.granted(),
                wanted: want,
                usage: if full && q.limit.is_stock() {
                    q.cap.delta()
                } else {
                    Vec::new()
                },
            });
        }
        if items.is_empty() {
            return;
        }
        let req = LeaseRequest {
            term: self.term,
            leader: self.leader.clone(),
            sent: now,
            members: self.subtree(),
            items,
        };
        self.outbound.push(Action::Call(parent, req));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BYTES: &str = "bytes";
    const RPS: &str = "rps";

    fn stock() -> Limit {
        Limit::Stock {
            limit: 1000,
            chunk: 100,
            acquired: 0,
            released: 0,
        }
    }

    fn rate() -> Limit {
        Limit::Rate {
            limit: 30,
            chunk: 2,
        }
    }

    fn node(id: u32) -> Lease<u32, &'static str> {
        let mut n = Lease::new(id, Config { ttl: 40 });
        n.set_limit(BYTES, stock());
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

    /// The same for two nodes.
    fn exchange(a: &mut Lease<u32, &'static str>, b: &mut Lease<u32, &'static str>) {
        for _ in 0..8 {
            let mut moved = false;
            for Action::Call(to, req) in a.ready() {
                assert_eq!(to, b.me);
                let resp = b.on_request(a.me, req);
                a.on_response(b.me, resp);
                moved = true;
            }
            for Action::Call(to, req) in b.ready() {
                assert_eq!(to, a.me);
                let resp = a.on_request(b.me, req);
                b.on_response(a.me, resp);
                moved = true;
            }
            if !moved {
                break;
            }
        }
    }

    fn pair() -> (Lease<u32, &'static str>, Lease<u32, &'static str>) {
        let mut l = node(1);
        let mut c = node(2);
        l.set_cluster_view(Some(&[1, 2]), 1, 1);
        c.set_cluster_view(Some(&[1, 2]), 1, 1);
        c.set_upstream(1);
        exchange(&mut c, &mut l);
        (l, c)
    }

    #[test]
    fn the_leader_holds_the_limit_and_everyone_else_nothing() {
        let (mut l, mut c) = pair();
        assert!(l.is_leader());
        assert_eq!(l.stats(&BYTES).unwrap().granted, 1000);
        assert_eq!(c.stats(&BYTES).unwrap().granted, 0);
        assert!(l.acquire(&[(BYTES, 10)]).is_ok());
        assert_eq!(c.acquire(&[(BYTES, 10)]).unwrap_err().key, BYTES);
    }

    #[test]
    fn a_child_asks_its_parent_and_is_leased_a_chunk() {
        let (mut l, mut c) = pair();
        assert!(c.acquire(&[(BYTES, 10)]).is_err(), "nothing held yet");
        c.tick(1);
        exchange(&mut c, &mut l);
        assert_eq!(c.stats(&BYTES).unwrap().granted, 100);
        assert!(c.stats(&BYTES).unwrap().good);
        assert!(c.acquire(&[(BYTES, 10)]).is_ok());
        assert_eq!(l.stats(&BYTES).unwrap().lent, 100);
    }

    #[test]
    fn usage_flows_up_as_a_map_and_a_delete_flows_back() {
        let (mut l, mut c) = pair();
        let _ = c.acquire(&[(BYTES, 30)]);
        c.tick(1);
        exchange(&mut c, &mut l);
        c.acquire(&[(BYTES, 30)]).unwrap();
        l.acquire(&[(BYTES, 5)]).unwrap();
        c.tick(20); // the keepalive carries the map
        exchange(&mut c, &mut l);
        assert_eq!(l.usage(&BYTES), 35);
        c.release(&BYTES, 10);
        c.tick(20);
        exchange(&mut c, &mut l);
        assert_eq!(l.usage(&BYTES), 25);
    }

    #[test]
    fn a_lease_lapses_without_renewal_and_a_stock_refuses() {
        let (mut l, mut c) = pair();
        let _ = c.acquire(&[(BYTES, 10)]);
        c.tick(1);
        exchange(&mut c, &mut l);
        assert!(c.acquire(&[(BYTES, 10)]).is_ok());
        c.tick(41); // no exchange: the parent never answers
        assert!(!c.stats(&BYTES).unwrap().good);
        assert!(c.acquire(&[(BYTES, 10)]).is_err());
        assert_eq!(
            c.stats(&BYTES).unwrap().granted,
            10,
            "unspent room was dropped"
        );
        l.tick(42);
        assert_eq!(l.stats(&BYTES).unwrap().lent, 0, "and the parent lapsed it");
    }

    #[test]
    fn a_rate_admits_without_a_lease_and_asks_for_one() {
        let mut l = node(1);
        let mut c = node(2);
        l.set_cluster_view(Some(&[1, 2]), 1, 1);
        c.set_cluster_view(Some(&[1, 2]), 1, 1);
        assert!(c.acquire(&[(RPS, 1)]).is_ok(), "no lease, but a rate");
        assert!(c.acquire(&[(BYTES, 1)]).is_err(), "no lease, and a stock");
        c.set_upstream(1);
        exchange(&mut c, &mut l);
        c.tick(1);
        exchange(&mut c, &mut l);
        assert!(c.stats(&RPS).unwrap().granted > 0, "and it asked");
    }

    #[test]
    fn a_node_with_nothing_in_play_is_silent() {
        let mut c = node(2);
        c.set_cluster_view(Some(&[1, 2]), 1, 1);
        c.set_upstream(1);
        assert!(
            c.ready().is_empty(),
            "nothing held, nothing wanted: no call"
        );
        c.tick(50);
        assert!(c.ready().is_empty(), "nor a keepalive");
        let mut e = Lease::<u32, &str>::new(3, Config { ttl: 40 });
        e.set_cluster_view(Some(&[1, 3]), 1, 1);
        e.set_upstream(1);
        e.tick(50);
        assert!(
            e.ready().is_empty(),
            "no limits at all: nothing on the wire"
        );
    }

    #[test]
    fn removing_a_limit_hands_it_back_and_forgets_it() {
        let (mut l, mut c) = pair();
        let _ = c.acquire(&[(BYTES, 10)]);
        c.tick(1);
        exchange(&mut c, &mut l);
        assert_eq!(l.stats(&BYTES).unwrap().lent, 100);
        c.remove_limit(&BYTES);
        exchange(&mut c, &mut l);
        assert_eq!(l.stats(&BYTES).unwrap().lent, 0);
        assert!(c.stats(&BYTES).is_none());
        c.tick(20);
        for Action::Call(_, req) in c.ready() {
            assert!(!req.items.iter().any(|i| i.key == BYTES));
        }
    }

    #[test]
    fn a_stale_leader_steps_down_on_the_first_call_with_a_newer_term() {
        let (mut old, mut c) = pair();
        let _ = c.acquire(&[(BYTES, 1)]);
        c.tick(1);
        exchange(&mut c, &mut old); // c holds a chunk from the old leader
                                    // Raft moved on; the child heard, the old leader did not. The child's crown tells the
                                    // old parent it holds nothing from it, and that call carries the new term.
        c.set_cluster_view(None, 2, 2);
        assert!(c.is_leader());
        c.tick(1);
        exchange(&mut c, &mut old);
        assert!(!old.is_leader());
        assert_eq!(old.term(), 2);
        let s = old.stats(&BYTES).unwrap();
        assert_eq!(s.lent, 0, "the chunk came back");
        assert_eq!(s.granted, 100, "and is all that is left of the root grant");
        assert!(!s.good, "not spendable until a new parent books it");
    }

    #[test]
    fn a_rate_share_is_a_token_bucket() {
        let (mut l, mut c) = pair();
        assert!(
            c.acquire(&[(RPS, 1)]).is_ok(),
            "unleased: admitted, and asked"
        );
        c.tick(1);
        exchange(&mut c, &mut l);
        assert_eq!(c.stats(&RPS).unwrap().granted, 2);
        c.tick(1);
        assert!(c.acquire(&[(RPS, 1)]).is_ok());
        assert!(c.acquire(&[(RPS, 1)]).is_ok());
        assert!(c.acquire(&[(RPS, 1)]).is_err(), "two per tick");
        c.tick(1);
        assert!(c.acquire(&[(RPS, 1)]).is_ok());
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
        let mut p = node(1);
        let mut c = node(2);
        p.set_cluster_view(Some(&[1, 2, 3]), 3, 1);
        c.set_cluster_view(Some(&[1, 2, 3]), 3, 1);
        c.set_upstream(1);
        let _ = c.acquire(&[(BYTES, 1)]);
        c.tick(1);
        exchange(&mut c, &mut p); // p now has c as a child
        p.set_upstream(2);
        p.set_upstream(2);
        assert_eq!(p.parent(), None);
    }

    #[test]
    fn a_lapsed_lease_carried_to_a_new_parent_is_fully_released_from_the_old() {
        let mut ns = vec![node(1), node(2), node(3)];
        for n in ns.iter_mut() {
            n.set_cluster_view(Some(&[1, 2, 3]), 1, 1);
        }
        ns[1].set_upstream(1);
        ns[2].set_upstream(2);
        route(&mut ns);
        let _ = ns[2].acquire(&[(BYTES, 10)]);
        for _ in 0..4 {
            for n in ns.iter_mut() {
                n.tick(1);
            }
            route(&mut ns);
        }
        assert_eq!(
            ns[2].stats(&BYTES).unwrap().granted,
            100,
            "the chunk came down"
        );
        assert_eq!(ns[1].stats(&BYTES).unwrap().lent, 100);
        // The middle node goes silent; the child's lease lapses, then it moves under the
        // leader. The adoption is confirmed, and the old parent is told.
        ns[2].tick(41);
        ns[2].set_upstream(1);
        ns[2].set_upstream(1);
        route(&mut ns);
        assert_eq!(
            ns[1].stats(&BYTES).unwrap().lent,
            0,
            "the old parent books nothing"
        );
        let booked: u64 = ns[1]
            .quotas
            .values()
            .filter_map(|q| q.children.get(&3))
            .map(|b| b.granted)
            .sum();
        assert_eq!(booked, 0, "nor does any booking for it survive");
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
        let _ = ns[2].acquire(&[(BYTES, 10)]);
        ns[2].tick(1);
        // The child's call reaches the middle node, which has nothing and gives nothing.
        let Action::Call(to, req) = ns[2].ready().remove(0);
        assert_eq!(to, 2);
        let resp = ns[1].on_request(3, req);
        assert_eq!(resp.items[0].grant, 0);
        ns[2].on_response(2, resp);
        // The middle node asks the leader at its next tick; the child retries after a round
        // trip and finds the chunk the middle node fetched for it.
        ns[1].tick(1);
        route(&mut ns);
        ns[2].tick(2);
        route(&mut ns);
        assert_eq!(ns[2].stats(&BYTES).unwrap().granted, 100);
    }

    #[test]
    fn a_refused_ask_backs_off_up_to_ttl_over_eight() {
        let mut l = node(1);
        l.set_cluster_view(Some(&[1, 2]), 1, 1);
        l.acquire(&[(BYTES, 1000)]).unwrap(); // the quota is exhausted
        let mut c = node(2);
        c.set_cluster_view(Some(&[1, 2]), 1, 1);
        c.set_upstream(1);
        let _ = c.acquire(&[(BYTES, 10)]);
        let mut asked_at = Vec::new();
        for t in 1..=40 {
            c.tick(1);
            for Action::Call(_, req) in c.ready() {
                if req.items.iter().any(|i| i.wanted > 0) {
                    asked_at.push(t);
                }
                let resp = l.on_request(2, req);
                c.on_response(1, resp);
            }
        }
        // Gaps of 2, 4, 5, 5, ...: a round trip, doubled, capped at ttl / 8 = 5. The
        // keepalive at ttl / 2 carries the want too, so a shorter gap appears there.
        let gaps: Vec<u64> = asked_at.windows(2).map(|w| w[1] - w[0]).collect();
        assert_eq!(&gaps[..3], &[2, 4, 5], "{gaps:?}");
        assert!(gaps[3..].iter().all(|&g| g <= 5), "{gaps:?}");
    }

    #[test]
    fn an_acquire_is_all_or_none() {
        let mut l = node(1);
        l.set_cluster_view(Some(&[1]), 1, 1);
        l.tick(1);
        assert!(l.acquire(&[(BYTES, 10), (RPS, 100)]).is_err());
        assert_eq!(l.stats(&BYTES).unwrap().used, 0);
        assert!(l.acquire(&[(BYTES, 10), (RPS, 1)]).is_ok());
        assert_eq!(l.stats(&BYTES).unwrap().used, 10);
    }

    #[test]
    fn a_stock_s_usage_comes_with_the_limit_and_merges_by_max() {
        let mut n = node(1);
        n.set_cluster_view(Some(&[1]), 1, 1);
        let with = |acquired, released| Limit::Stock {
            limit: 1000,
            chunk: 100,
            acquired,
            released,
        };
        n.set_limit(BYTES, with(300, 50));
        assert_eq!(n.usage(&BYTES), 250);
        n.acquire(&[(BYTES, 10)]).unwrap();
        n.set_limit(BYTES, with(0, 0)); // a stale restore changes nothing
        assert_eq!(n.usage(&BYTES), 260);
        n.set_limit(BYTES, with(400, 50)); // a newer one is taken
        assert_eq!(n.usage(&BYTES), 350);
    }

    #[test]
    fn the_child_s_lease_ends_before_the_parent_s_booking() {
        // Dated from sending at the child, from arrival at the parent: the parent can never
        // re-lend room the child still considers its own.
        let (mut l, mut c) = pair();
        let _ = c.acquire(&[(BYTES, 10)]);
        // The child's call goes at 100, arrives at 101, is answered at 102.
        c.tick(100);
        l.tick(101);
        let Action::Call(_, req) = c.ready().remove(0);
        let resp = l.on_request(2, req);
        c.on_response(1, resp);
        assert_eq!(c.stats(&BYTES).unwrap().granted, 100);
        assert_eq!(c.stats(&BYTES).unwrap().valid_until, 140, "100 + ttl");
        c.tick(40); // 140: the last good tick
        assert!(c.acquire(&[(BYTES, 10)]).is_ok());
        c.tick(1); // 141
        assert!(c.acquire(&[(BYTES, 10)]).is_err(), "the child stopped");
        l.tick(40); // 141: the parent still books it
        assert_eq!(l.stats(&BYTES).unwrap().lent, 100);
        l.tick(1); // 142: and only now lets it go
        assert_eq!(l.stats(&BYTES).unwrap().lent, 0);
    }

    #[test]
    fn a_new_term_fences_a_stock_until_confirmed_and_not_a_rate() {
        let (mut l, mut c) = pair();
        let _ = c.acquire(&[(BYTES, 10), (RPS, 1)]);
        c.tick(1);
        exchange(&mut c, &mut l);
        c.tick(1);
        assert!(c.acquire(&[(BYTES, 10)]).is_ok());
        assert!(c.acquire(&[(RPS, 1)]).is_ok());
        // Raft moves on, the same leader wins the new term.
        c.set_cluster_view(None, 1, 2);
        assert!(
            c.acquire(&[(BYTES, 10)]).is_err(),
            "a stock waits for the new term"
        );
        assert!(c.acquire(&[(RPS, 1)]).is_ok(), "a rate admits regardless");
        assert_eq!(
            c.stats(&BYTES).unwrap().granted,
            100,
            "the lease is kept, not dropped"
        );
        l.set_cluster_view(None, 1, 2);
        c.tick(1);
        exchange(&mut c, &mut l); // the call goes out at once; the answer carries term 2
        assert!(c.acquire(&[(BYTES, 10)]).is_ok(), "confirmed");
    }

    /// A three-node cluster, leader 1 with children 2 and 3, where 3 has written 60 and
    /// reported it. Returns the nodes and each node's own durable row.
    fn cluster_with_writes() -> Vec<Lease<u32, &'static str>> {
        let mut ns = vec![node(1), node(2), node(3)];
        for n in ns.iter_mut() {
            n.set_cluster_view(Some(&[1, 2, 3]), 1, 1);
        }
        ns[1].set_upstream(1);
        ns[2].set_upstream(1);
        route(&mut ns);
        let _ = ns[1].acquire(&[(BYTES, 40)]);
        let _ = ns[2].acquire(&[(BYTES, 60)]);
        for _ in 0..12 {
            for n in ns.iter_mut() {
                n.tick(1);
            }
            route(&mut ns);
        }
        ns[1].acquire(&[(BYTES, 40)]).unwrap();
        ns[2].acquire(&[(BYTES, 60)]).unwrap();
        for _ in 0..21 {
            for n in ns.iter_mut() {
                n.tick(1);
            }
            route(&mut ns);
        }
        assert_eq!(ns[0].usage(&BYTES), 100, "the leader has both reports");
        ns
    }

    /// What a node would have in its durable table: its own row of the map.
    fn own_row(n: &Lease<u32, &'static str>) -> (u64, u64) {
        n.quotas[&BYTES]
            .cap
            .delta()
            .into_iter()
            .find(|(id, _, _)| *id == n.me)
            .map_or((0, 0), |(_, a, r)| (a, r))
    }

    #[test]
    fn an_expelled_node_s_reported_usage_survives_and_its_unreported_usage_is_lost() {
        let mut ns = cluster_with_writes();
        ns[2].acquire(&[(BYTES, 10)]).unwrap(); // written, not yet reported
        for n in ns.iter_mut() {
            n.set_cluster_view(Some(&[1, 2]), 1, 1); // 3 is expelled
        }
        for _ in 0..21 {
            for n in ns.iter_mut() {
                n.tick(1);
            }
            route(&mut ns[..2]);
        }
        assert_eq!(
            ns[0].usage(&BYTES),
            100,
            "the reported 60 stays; the unreported 10 is gone"
        );
    }

    #[test]
    #[ignore = "exposes the loss: usage that only lived in memory does not survive a full restart"]
    fn a_full_restart_after_an_expulsion_keeps_the_expelled_node_s_usage() {
        let mut ns = cluster_with_writes();
        for n in ns.iter_mut() {
            n.set_cluster_view(Some(&[1, 2]), 1, 1); // 3 is expelled, its table with it
        }
        // Every remaining node restarts from its own durable row.
        let rows: Vec<(u64, u64)> = ns[..2].iter().map(own_row).collect();
        let mut fresh = vec![
            Lease::new(1, Config { ttl: 40 }),
            Lease::new(2, Config { ttl: 40 }),
        ];
        for (n, (acquired, released)) in fresh.iter_mut().zip(rows) {
            n.set_limit(
                BYTES,
                Limit::Stock {
                    limit: 1000,
                    chunk: 100,
                    acquired,
                    released,
                },
            );
            n.set_limit(RPS, rate());
            n.set_cluster_view(Some(&[1, 2]), 1, 2);
        }
        fresh[1].set_upstream(1);
        for _ in 0..21 {
            for n in fresh.iter_mut() {
                n.tick(1);
            }
            route(&mut fresh);
        }
        assert_eq!(
            fresh[0].usage(&BYTES),
            100,
            "node 3's 60 bytes are still stored; the leader should still count them"
        );
    }
}
