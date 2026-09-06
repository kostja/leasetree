// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (c) 2026 Konstantin Osipov.

//! A pure state machine for distributed quotas.
//!
//! Every node of a cluster holds a lease on each quota it uses. Leases are handed down a
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
//! # Two kinds of quota
//!
//! A **stock** is a total: bytes stored, objects stored. A node draws on its lease, reports
//! what it drew, and gives back a delete. The leader's `usage` is the cluster's total.
//!
//! A **flow** is a rate: requests or bytes per tick. A node holds a share of the refill and
//! runs a token bucket from it. Nothing is reported; a share not renewed lapses.
//!
//! # Two policies
//!
//! Without a good lease -- lapsed, or not yet confirmed in the current term -- a node either
//! writes anyway (`Allow`: availability first; the write is reported and the tree absorbs
//! the over-commit) or refuses (`Deny`: the limit first; a false denial is the price).
//!
//! # The contract
//!
//! Every input mutates state and appends to one FIFO outbound queue, drained with
//! [`ready`](Lease::ready). The inputs that can produce output return `true` when they took
//! the queue from empty to non-empty: the edge on which to wake a sender. Enforcement,
//! [`acquire`](Lease::acquire) and [`release`](Lease::release), is local and never sends; a
//! refusal only marks the quota wanted, and the next [`tick`](Lease::tick) asks the parent.
//!
//! What the caller supplies:
//!
//! - the quotas, from its configuration: [`set_limit`](Lease::set_limit);
//! - its own usage at boot, from durable storage: [`restore`](Lease::restore);
//! - the cluster view, from Raft: [`set_cluster_view`](Lease::set_cluster_view) -- members,
//!   leader and term;
//! - the upstream peer, from its overlay: [`set_upstream`](Lease::set_upstream) -- whichever
//!   peer last delivered the leader's traffic;
//! - liveness, from its failure detector: [`down`](Lease::down) and [`up`](Lease::up);
//! - the network and the clock: [`on_message`](Lease::on_message) and [`tick`](Lease::tick).
//!
//! # The rules
//!
//! Each of these was found necessary by a simulation that went wrong without it.
//!
//! - A lease is dated from the tick the request was *sent*, so a parent that lapses it
//!   (dated from arrival, later) never re-lends room the child still considers its own.
//! - A child reports whenever its bookings or its term changed, and every `ttl / 2` as a
//!   keepalive. A parent books what the child reports.
//! - A parent that cannot fill a request asks its own parent for the shortfall.
//! - A node that moves to a new parent keeps the old one's booking until the new parent has
//!   confirmed the adoption; until then both book it and nobody re-lends it.
//! - Under `Deny`, a parent that booked more than it holds cuts a child only after a grace
//!   period, never below what the child's subtree has used, and the child passes the cut down.
//! - A node keeps its parent while that parent keeps delivering the leader's traffic, and
//!   never takes its own child as parent.
//! - A node drops a lease the moment it lapses: the parent has re-lent that room.
//! - A new leader grants nothing and cuts nobody until its reports cover every live member,
//!   or one `ttl` has passed.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::collections::{BTreeMap, BTreeSet};

use bcounter::BCounter;

/// Timing, in ticks. The caller decides what a tick is.
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

/// What a quota counts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// A total: bytes, objects. Drawn on, reported, given back.
    Stock,
    /// A rate, in units per tick. A share of the refill, run as a token bucket.
    Flow,
}

/// What a node does without a good lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Policy {
    /// Write anyway; the tree absorbs the over-commit.
    Allow,
    /// Refuse until the lease is confirmed.
    Deny,
}

/// One quota, as configured. Every node needs `kind`, `policy` and `chunk`; only the leader
/// uses `limit`.
#[derive(Clone, Copy, Debug)]
pub struct Limit {
    /// What it counts.
    pub kind: Kind,
    /// What to do without a good lease.
    pub policy: Policy,
    /// The ceiling: units for a stock, units per tick for a flow.
    pub limit: u64,
    /// How much a node asks for at a time, and keeps in hand when idle.
    pub chunk: u64,
}

/// One item of a [`Message`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Item<Id, K> {
    /// child -> parent: lend me up to `want` more.
    Request {
        /// The quota.
        key: K,
        /// How much more.
        want: u64,
    },
    /// child -> parent: keep-alive and report. The child's whole grant, so a new parent can
    /// adopt it, and the usage map of its subtree (stocks only).
    Renew {
        /// The quota.
        key: K,
        /// All the child holds.
        granted: u64,
        /// `(node, acquired, released)` for every node under it, itself included.
        usage: Vec<(Id, u64, u64)>,
    },
    /// child -> parent: I no longer hold `amount` of what you lent me.
    Release {
        /// The quota.
        key: K,
        /// How much.
        amount: u64,
    },
    /// parent -> child: `amount` more is yours. As the answer to a `Renew` (`renewal`), `hold`
    /// is all the parent books for you: less than you hold is a cut, and it confirms an
    /// adoption. The answer to a `Request` says nothing about the booking as a whole.
    Grant {
        /// The quota.
        key: K,
        /// How much more.
        amount: u64,
        /// Whether this answers a `Renew`.
        renewal: bool,
        /// All the parent books for the child (renewals only; `u64::MAX` otherwise).
        hold: u64,
    },
    /// parent -> child: your grant is now `to`. Unsolicited; the next renewal's `hold` says
    /// the same, so a lost one heals.
    Shrink {
        /// The quota.
        key: K,
        /// The new grant.
        to: u64,
    },
}

/// Everything one node has for one peer, in one go.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message<Id, K> {
    /// The sender's term.
    pub term: u64,
    /// The sender's leader. A stale leader learns of its successor from the first message
    /// it receives.
    pub leader: Option<Id>,
    /// The sender's tick. A lease is dated from the tick its request was sent.
    pub sent: u64,
    /// When answering: the `sent` of the message answered.
    pub in_reply_to: Option<u64>,
    /// The sender's subtree, itself included (going up only).
    pub members: Vec<Id>,
    /// The items.
    pub items: Vec<Item<Id, K>>,
}

/// Something the caller must do.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action<Id, K> {
    /// Send this message to this peer.
    Send(Id, Message<Id, K>),
}

/// An `acquire` that could not be honoured.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Denied<K> {
    /// The first quota that refused.
    pub key: K,
    /// What it could have given.
    pub available: u64,
}

/// A quota as this node sees it, for metrics.
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
    /// Whether the lease is good right now.
    pub good: bool,
}

/// What a parent books for one child on one quota.
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

/// One quota's state on this node.
struct Quota<Id: Ord + Clone> {
    limit: Limit,
    /// The grant and, for a stock, the usage map: our own slot and the children's, merged.
    cap: BCounter<Id>,
    /// A flow's bucket.
    tokens: f64,
    lent: u64,
    children: BTreeMap<Id, Booking>,
    /// The term this lease was last confirmed in, and until when it is good.
    term: u64,
    valid_until: u64,
    /// How much more we want from the parent; asked at the next tick.
    wanted: u64,
    last_request: Option<u64>,
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
            last_request: None,
            over_since: None,
        }
    }
    /// What we hold and have not used (stocks) or hold (flows).
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
}

/// A tree link to a child: what it last reported about its subtree.
struct Link<Id> {
    members: BTreeSet<Id>,
    expires: u64,
}

/// One node's lease state, for every quota.
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
    /// After a parent change: the old parent and what we held from it, released once the new
    /// parent has confirmed the adoption.
    pending_release: Option<(Id, Vec<(K, u64)>)>,
    /// Bookings changed since the last report: report at the next tick.
    dirty: bool,
    last_renew: u64,
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
            pending_release: None,
            dirty: false,
            last_renew: 0,
            outbound: Vec::new(),
        }
    }

    // ------------------------------------------------------------ configuration

    /// Add or change a quota. On the leader, the grant becomes the limit at once.
    pub fn set_limit(&mut self, key: K, limit: Limit) -> bool {
        let was_empty = self.outbound.is_empty();
        let me = self.me.clone();
        let (leader, term) = (self.is_leader(), self.term);
        let q = self
            .quotas
            .entry(key)
            .or_insert_with(|| Quota::new(me, limit));
        q.limit = limit;
        if leader {
            Self::hold_limit(q, term);
        }
        self.woke(was_empty)
    }

    /// Forget a quota. Whatever it held is dropped; the parent lapses it.
    pub fn remove_limit(&mut self, key: &K) -> bool {
        self.quotas.remove(key);
        self.dirty = true;
        false
    }

    /// This node's own usage of a stock, from durable storage, at boot.
    pub fn restore(&mut self, key: &K, acquired: u64, released: u64) {
        let me = self.me.clone();
        if let Some(q) = self.quotas.get_mut(key) {
            q.cap.apply(&[(me, acquired, released)]);
        }
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
            // A new term: report now, so the ack confirms the lease in it.
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

    /// A message from `from`.
    pub fn on_message(&mut self, from: Id, msg: Message<Id, K>) -> bool {
        let was_empty = self.outbound.is_empty();
        if msg.term > self.term {
            if let Some(l) = msg.leader.clone() {
                self.set_cluster_view(None, l, msg.term);
            }
        }
        let from_parent = self.parent.as_ref() == Some(&from);
        let ttl = self.cfg.ttl;
        let now = self.now;
        let mut reply: Vec<Item<Id, K>> = Vec::new();
        let mut cuts: Vec<(Id, K, u64)> = Vec::new();
        let mut confirmed = false;
        for item in msg.items {
            match item {
                Item::Request { key, want } => {
                    if self.is_leader() && !self.may_grant() {
                        continue;
                    }
                    let Some(q) = self.quotas.get_mut(&key) else {
                        continue;
                    };
                    let give = want.min(q.room());
                    if give > 0 {
                        let b = q.children.entry(from.clone()).or_insert(Booking {
                            granted: 0,
                            used: 0,
                            expires: now + ttl,
                            given_at: now,
                            wanted: 0,
                        });
                        b.granted += give;
                        b.expires = now + ttl;
                        b.given_at = now;
                        q.lent += give;
                    }
                    let short = want - give;
                    if let Some(b) = q.children.get_mut(&from) {
                        b.wanted = short;
                    }
                    if short > 0 {
                        q.wanted = q.wanted.max(short);
                    }
                    reply.push(Item::Grant {
                        key,
                        amount: give,
                        renewal: false,
                        hold: u64::MAX,
                    });
                }
                Item::Renew {
                    key,
                    granted,
                    usage,
                } => {
                    let link = self.links.entry(from.clone()).or_insert(Link {
                        members: BTreeSet::new(),
                        expires: 0,
                    });
                    link.members = msg.members.iter().cloned().collect();
                    link.expires = now + ttl;
                    let settling = self.is_leader() && !self.may_grant();
                    let Some(q) = self.quotas.get_mut(&key) else {
                        continue;
                    };
                    let used: u64 = usage.iter().map(|(_, a, r)| a.saturating_sub(*r)).sum();
                    if q.limit.kind == Kind::Stock {
                        q.cap.apply(&usage);
                    }
                    let b = q.children.entry(from.clone()).or_insert(Booking {
                        granted: 0,
                        used: 0,
                        expires: 0,
                        given_at: 0,
                        wanted: 0,
                    });
                    // A report sent before our last gift reached the child does not include
                    // it: keep the booking.
                    let booked = if msg.sent > b.given_at {
                        granted
                    } else {
                        b.granted.max(granted)
                    };
                    if b.granted != booked {
                        self.dirty = true;
                    }
                    q.lent = q.lent - b.granted + booked;
                    b.granted = booked;
                    b.used = used;
                    b.expires = now + ttl;
                    let grace = Self::grace(ttl);
                    let long_enough = q.over_since.is_some_and(|s| now >= s + grace);
                    let hold = if q.limit.policy == Policy::Allow || settling || !long_enough {
                        booked
                    } else {
                        booked - q.overcommit().min(booked.saturating_sub(used))
                    };
                    reply.push(Item::Grant {
                        key,
                        amount: 0,
                        renewal: true,
                        hold,
                    });
                }
                Item::Release { key, amount } => {
                    if let Some(q) = self.quotas.get_mut(&key) {
                        if let Some(b) = q.children.get_mut(&from) {
                            let back = amount.min(b.granted);
                            b.granted -= back;
                            q.lent -= back;
                            if b.granted == 0 {
                                q.children.remove(&from);
                            }
                            self.dirty = true;
                        }
                    }
                }
                Item::Grant {
                    key,
                    amount,
                    renewal,
                    hold,
                } => {
                    if !from_parent {
                        continue;
                    }
                    let Some(q) = self.quotas.get_mut(&key) else {
                        continue;
                    };
                    q.cap.grant(amount);
                    if amount > 0 {
                        q.wanted = q.wanted.saturating_sub(amount);
                    }
                    q.term = q.term.max(msg.term);
                    let dated = msg.in_reply_to.unwrap_or(msg.sent);
                    q.valid_until = q.valid_until.max(dated + ttl);
                    if renewal {
                        confirmed = true;
                        cuts.extend(Self::cut(&from, q, key, hold));
                    }
                }
                Item::Shrink { key, to } => {
                    if !from_parent {
                        continue;
                    }
                    if let Some(q) = self.quotas.get_mut(&key) {
                        cuts.extend(Self::cut(&from, q, key, to));
                    }
                }
            }
        }
        if !cuts.is_empty() {
            self.dirty = true;
            self.send_shrinks(cuts);
        }
        if confirmed {
            self.release_old_parent();
        }
        if !reply.is_empty() {
            let m = self.envelope(Some(msg.sent), false, reply);
            self.outbound.push(Action::Send(from, m));
        }
        self.woke(was_empty)
    }

    /// Advance the clock by `n` ticks: refill flows, lapse children, drop lapsed leases,
    /// report, and ask.
    pub fn tick(&mut self, n: u64) -> bool {
        let was_empty = self.outbound.is_empty();
        self.now = self.now.saturating_add(n);
        let now = self.now;
        let ttl = self.cfg.ttl;
        let leader = self.is_leader();

        // Flows refill; children lapse; over-commit is timed.
        for q in self.quotas.values_mut() {
            if q.limit.kind == Kind::Flow {
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
        }

        // Report and ask.
        if !leader && self.parent.is_some() {
            let due = now >= self.last_renew + ttl / 2;
            if self.dirty || due {
                self.renew();
            } else {
                self.ask();
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

    /// Draw `amount` on each quota, all or none. A quota not configured is unlimited. A stock
    /// draws on the held lease; a flow draws tokens. Without a good lease, `Allow` writes
    /// anyway and `Deny` refuses. A refusal marks the quota wanted; the next tick asks.
    pub fn acquire(&mut self, keys: &[(K, u64)]) -> Result<(), Denied<K>> {
        let leader = self.is_leader();
        let (term, now) = (self.term, self.now);
        for (key, amount) in keys {
            let Some(q) = self.quotas.get_mut(key) else {
                continue;
            };
            let good = leader || (q.term == term && now <= q.valid_until);
            let have = match q.limit.kind {
                Kind::Stock => q.room(),
                Kind::Flow => q.tokens.floor() as u64,
            };
            let ok = (good && have >= *amount) || (!good && q.limit.policy == Policy::Allow);
            if !ok {
                q.wanted = q.wanted.max(q.limit.chunk).max(*amount);
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
            match q.limit.kind {
                Kind::Stock => {
                    if !good || q.room() < *amount {
                        // Writing without a lease: self-granted, reported, absorbed above.
                        q.cap.grant(*amount);
                    }
                    q.cap.acquire(*amount).ok();
                }
                Kind::Flow => {
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
            if q.limit.kind == Kind::Stock {
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

    /// A quota's figures, for metrics.
    pub fn stats(&self, key: &K) -> Option<Stats> {
        let q = self.quotas.get(key)?;
        Some(Stats {
            granted: q.cap.granted(),
            used: q.cap.granted().saturating_sub(q.cap.local_available()),
            lent: q.lent,
            overcommit: q.overcommit(),
            good: self.is_leader() || (q.term == self.term && self.now <= q.valid_until),
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
        let more = q.limit.limit.saturating_sub(q.cap.granted());
        q.cap.grant(more);
        let over = q.cap.granted().saturating_sub(q.limit.limit);
        let _ = q.cap.reclaim(over);
        q.term = term;
        q.valid_until = u64::MAX;
        q.wanted = 0;
    }

    /// Become the leader: hold every limit outright, and let the old parent go.
    fn crown(&mut self) {
        let term = self.term;
        self.leader_since = self.now;
        let owed: Vec<(K, u64)> = self
            .quotas
            .iter()
            .filter(|(_, q)| q.cap.granted() > 0)
            .map(|(k, q)| (k.clone(), q.cap.granted()))
            .collect();
        if let Some(op) = self.parent.take() {
            if !owed.is_empty() {
                self.send_releases(op, owed);
            }
        }
        if let Some((op, owed)) = self.pending_release.take() {
            self.send_releases(op, owed);
        }
        self.parent_misses = 0;
        for q in self.quotas.values_mut() {
            Self::hold_limit(q, term);
        }
    }

    /// Stop being the leader: keep what is used or lent, drop the rest of the root grant.
    fn demote(&mut self) {
        for q in self.quotas.values_mut() {
            let excess = q.room();
            let _ = q.cap.reclaim(excess);
            q.valid_until = 0;
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
    /// next report does not claim it and the next ack cannot revive it.
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
        // Lapse what has lapsed, on both sides, before carrying anything over.
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
        if let Some((op, owed)) = self.pending_release.take() {
            self.send_releases(op, owed);
        }
        if let Some(op) = self.parent.take() {
            let owed: Vec<(K, u64)> = self
                .quotas
                .iter()
                .filter(|(_, q)| q.cap.granted() > 0)
                .map(|(k, q)| (k.clone(), q.cap.granted()))
                .collect();
            if !owed.is_empty() {
                self.pending_release = Some((op, owed));
            }
        }
        self.parent = Some(peer);
        self.parent_misses = 0;
        self.renew();
    }

    fn release_old_parent(&mut self) {
        if let Some((op, owed)) = self.pending_release.take() {
            self.send_releases(op, owed);
        }
    }

    fn send_releases(&mut self, to: Id, owed: Vec<(K, u64)>) {
        let items = owed
            .into_iter()
            .map(|(key, amount)| Item::Release { key, amount })
            .collect();
        let m = self.envelope(None, false, items);
        self.outbound.push(Action::Send(to, m));
    }

    /// The parent books less than we hold: give the difference back, what is unspent of it,
    /// and pass on to our children what we could not.
    fn cut(from: &Id, q: &mut Quota<Id>, key: K, hold: u64) -> Vec<(Id, K, u64)> {
        let _ = from;
        let cut = q.cap.granted().saturating_sub(hold);
        if cut == 0 {
            return Vec::new();
        }
        let _ = q.cap.reclaim(cut);
        let mut over = q.overcommit();
        let mut asks = Vec::new();
        for (c, b) in &q.children {
            if over == 0 {
                break;
            }
            let take = over.min(b.granted.saturating_sub(b.used));
            if take > 0 {
                over -= take;
                asks.push((c.clone(), key.clone(), b.granted - take));
            }
        }
        asks
    }

    fn send_shrinks(&mut self, cuts: Vec<(Id, K, u64)>) {
        let mut per_child: BTreeMap<Id, Vec<Item<Id, K>>> = BTreeMap::new();
        for (c, key, to) in cuts {
            per_child
                .entry(c)
                .or_default()
                .push(Item::Shrink { key, to });
        }
        for (c, items) in per_child {
            let m = self.envelope(None, false, items);
            self.outbound.push(Action::Send(c, m));
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

    fn envelope(
        &self,
        in_reply_to: Option<u64>,
        up: bool,
        items: Vec<Item<Id, K>>,
    ) -> Message<Id, K> {
        Message {
            term: self.term,
            leader: self.leader.clone(),
            sent: self.now,
            in_reply_to,
            members: if up { self.subtree() } else { Vec::new() },
            items,
        }
    }

    /// Report to the parent: every quota's grant and usage, capacity we will not use, and
    /// whatever we want.
    fn renew(&mut self) {
        let Some(parent) = self.parent.clone() else {
            return;
        };
        let now = self.now;
        self.dirty = false;
        self.last_renew = now;
        let mut items = Vec::new();
        for (key, q) in self.quotas.iter_mut() {
            // Hand back room beyond two chunks (a stock), or a share beyond what is lent plus
            // one chunk while the bucket is full (a flow).
            let keep = match q.limit.kind {
                Kind::Stock => 2 * q.limit.chunk,
                Kind::Flow => {
                    if q.tokens >= q.refill().max(1.0) * 2.0 {
                        q.limit.chunk
                    } else {
                        u64::MAX
                    }
                }
            }
            .saturating_add(q.earmarked());
            let spare = q.room();
            if spare > keep {
                let back = q.cap.reclaim(spare - keep);
                if back > 0 {
                    items.push(Item::Release {
                        key: key.clone(),
                        amount: back,
                    });
                }
            }
            items.push(Item::Renew {
                key: key.clone(),
                granted: q.cap.granted(),
                usage: if q.limit.kind == Kind::Stock {
                    q.cap.delta()
                } else {
                    Vec::new()
                },
            });
        }
        items.extend(self.requests());
        let m = self.envelope(None, true, items);
        self.outbound.push(Action::Send(parent, m));
    }

    /// Ask the parent for what is wanted, if anything is due.
    fn ask(&mut self) {
        let Some(parent) = self.parent.clone() else {
            return;
        };
        let items = self.requests();
        if items.is_empty() {
            return;
        }
        let m = self.envelope(None, true, items);
        self.outbound.push(Action::Send(parent, m));
    }

    fn requests(&mut self) -> Vec<Item<Id, K>> {
        let now = self.now;
        let retry = Self::grace(self.cfg.ttl);
        let mut items = Vec::new();
        for (key, q) in self.quotas.iter_mut() {
            let want = q.wanted.max(q.overcommit());
            let recent = q.last_request.is_some_and(|t| now < t + retry);
            if want == 0 || recent {
                continue;
            }
            q.last_request = Some(now);
            items.push(Item::Request {
                key: key.clone(),
                want,
            });
        }
        items
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BYTES: &str = "bytes";
    const RPS: &str = "rps";

    fn stock(policy: Policy) -> Limit {
        Limit {
            kind: Kind::Stock,
            policy,
            limit: 1000,
            chunk: 100,
        }
    }

    fn flow() -> Limit {
        Limit {
            kind: Kind::Flow,
            policy: Policy::Deny,
            limit: 30,
            chunk: 2,
        }
    }

    fn node(id: u32) -> Lease<u32, &'static str> {
        let mut n = Lease::new(id, Config { ttl: 40 });
        n.set_limit(BYTES, stock(Policy::Deny));
        n.set_limit(RPS, flow());
        n
    }

    /// Deliver every queued message between two nodes until both are quiet.
    fn exchange(a: &mut Lease<u32, &'static str>, b: &mut Lease<u32, &'static str>) {
        for _ in 0..8 {
            let mut moved = false;
            for Action::Send(to, m) in a.ready() {
                assert_eq!(to, b.me);
                b.on_message(a.me, m);
                moved = true;
            }
            for Action::Send(to, m) in b.ready() {
                assert_eq!(to, a.me);
                a.on_message(b.me, m);
                moved = true;
            }
            if !moved {
                break;
            }
        }
    }

    #[test]
    fn the_leader_holds_the_limit_and_everyone_else_nothing() {
        let mut l = node(1);
        let mut c = node(2);
        l.set_cluster_view(Some(&[1, 2]), 1, 1);
        c.set_cluster_view(Some(&[1, 2]), 1, 1);
        assert!(l.is_leader());
        assert_eq!(l.stats(&BYTES).unwrap().granted, 1000);
        assert_eq!(c.stats(&BYTES).unwrap().granted, 0);
        assert!(l.acquire(&[(BYTES, 10)]).is_ok());
        assert_eq!(c.acquire(&[(BYTES, 10)]).unwrap_err().key, BYTES);
    }

    #[test]
    fn a_child_asks_its_parent_and_is_leased_a_chunk() {
        let mut l = node(1);
        let mut c = node(2);
        l.set_cluster_view(Some(&[1, 2]), 1, 1);
        c.set_cluster_view(Some(&[1, 2]), 1, 1);
        c.set_upstream(1);
        exchange(&mut c, &mut l); // the first report; the leader now covers everyone
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
        let mut l = node(1);
        let mut c = node(2);
        l.set_cluster_view(Some(&[1, 2]), 1, 1);
        c.set_cluster_view(Some(&[1, 2]), 1, 1);
        c.set_upstream(1);
        exchange(&mut c, &mut l);
        let _ = c.acquire(&[(BYTES, 30)]); // refused: marks the quota wanted
        c.tick(1);
        exchange(&mut c, &mut l);
        c.acquire(&[(BYTES, 30)]).unwrap();
        l.acquire(&[(BYTES, 5)]).unwrap();
        c.tick(20); // a renewal is due
        exchange(&mut c, &mut l);
        assert_eq!(l.usage(&BYTES), 35);
        c.release(&BYTES, 10);
        c.tick(20);
        exchange(&mut c, &mut l);
        assert_eq!(l.usage(&BYTES), 25);
    }

    #[test]
    fn a_lease_lapses_without_renewal_and_deny_refuses() {
        let mut l = node(1);
        let mut c = node(2);
        l.set_cluster_view(Some(&[1, 2]), 1, 1);
        c.set_cluster_view(Some(&[1, 2]), 1, 1);
        c.set_upstream(1);
        exchange(&mut c, &mut l);
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
    fn allow_writes_without_a_lease_and_reports_it() {
        let mut l = node(1);
        let mut c = node(2);
        l.set_limit(BYTES, stock(Policy::Allow));
        c.set_limit(BYTES, stock(Policy::Allow));
        l.set_cluster_view(Some(&[1, 2]), 1, 1);
        c.set_cluster_view(Some(&[1, 2]), 1, 1);
        assert!(c.acquire(&[(BYTES, 10)]).is_ok(), "no lease, but allow");
        c.set_upstream(1);
        exchange(&mut c, &mut l);
        assert_eq!(l.usage(&BYTES), 10);
        assert_eq!(
            l.stats(&BYTES).unwrap().lent,
            10,
            "the self-grant is booked"
        );
    }

    #[test]
    fn a_stale_leader_steps_down_on_the_first_message_with_a_newer_term() {
        let mut old = node(1);
        let mut c = node(2);
        old.set_cluster_view(Some(&[1, 2]), 1, 1);
        c.set_cluster_view(Some(&[1, 2]), 1, 1);
        c.set_upstream(1);
        exchange(&mut c, &mut old);
        c.acquire(&[(BYTES, 1)]).ok();
        c.tick(1);
        exchange(&mut c, &mut old); // c holds a chunk from the old leader
                                    // Raft moved on; the child heard, the old leader did not. The child's crown hands
                                    // the chunk back, and that message carries the new term.
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
    fn a_flow_share_is_a_token_bucket() {
        let mut l = node(1);
        let mut c = node(2);
        l.set_cluster_view(Some(&[1, 2]), 1, 1);
        c.set_cluster_view(Some(&[1, 2]), 1, 1);
        c.set_upstream(1);
        exchange(&mut c, &mut l);
        assert!(c.acquire(&[(RPS, 1)]).is_err());
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
        exchange(&mut c, &mut p); // p now has c as a child
        p.set_upstream(2);
        p.set_upstream(2);
        assert_eq!(p.parent(), None);
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
}
