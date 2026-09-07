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

/// The tree as a pure function of a cluster view.
pub mod tree;

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
    /// The bucket's capacity on this node, in units, when two ticks of its share are
    /// less: a share below one request per tick still serves a request, after saving up.
    pub burst: u64,
}

/// One limit in a [`LeaseRequest`].
///
/// Each edge of the tree carries two counters that only grow: `given`, kept by the parent,
/// what it ever handed this child; and `returned`, kept by the child, what it ever handed
/// back. The share is the difference. The parent merges `returned` by max, the child adopts
/// `given` from answers in order, so a re-sent request, a lost answer or a restarted parent
/// all reconcile on the next exchange with no ordering state.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RequestItem<K> {
    /// The limit.
    pub key: K,
    /// What the child knows the parent has handed it, ever: the `given` it last adopted.
    /// The parent reads the wants below against this, so a want made before an answer
    /// arrived is not served twice.
    pub given: u64,
    /// What the child has handed back to this parent, ever. A release raises it; raising it
    /// to `given` lets the parent go entirely.
    pub returned: u64,
    /// What the child has already lent beyond its share: a moved subtree it adopted, or a
    /// cut from above it has not passed on yet. Demand like any other, served first within
    /// the child's entitlement.
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
    /// The limits.
    pub items: Vec<RequestItem<K>>,
}

/// One limit in a [`LeaseResponse`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ResponseItem<K> {
    /// The limit.
    pub key: K,
    /// What the parent has handed this child, ever. The child adopts it.
    pub given: u64,
    /// What the parent counts as handed back, ever: the child's own returns, and cuts the
    /// parent made. The child takes the larger of its own and this.
    pub returned: u64,
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
    /// The `sent` of the request answered. The child applies answers in this order.
    pub in_reply_to: u64,
    /// The parent's tick, for the log.
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

/// What a parent books for one child on one limit: the edge's two counters, and what the
/// child asked for and did not get.
#[derive(Clone, Debug, Default)]
struct Booking {
    given: u64,
    returned: u64,
    expires: u64,
    /// What the child asked for and did not get: earmarked, so that what we fetch for it is
    /// not handed back as spare before it asks again.
    wanted: u64,
    /// What the child needs and did not get.
    needed: u64,
}

impl Booking {
    fn share(&self) -> u64 {
        self.given.saturating_sub(self.returned)
    }
    /// Holds something, or waits for something: a child. Otherwise a former one whose
    /// counters we keep, so that it can come back and catch up.
    fn active(&self) -> bool {
        self.share() > 0 || self.wanted > 0 || self.needed > 0
    }
}

/// One limit's state on this node.
struct Quota<Id: Ord + Clone> {
    limit: Limit,
    /// The edge to the parent: what it gave us, ever, and what we gave back, ever. The
    /// share is the difference. On the leader `given` is the whole rate.
    given: u64,
    returned: u64,
    tokens: f64,
    children: BTreeMap<Id, Booking>,
    /// Until when the share is good; `None` while there is none.
    valid_until: Option<u64>,
    /// How much more we want from the parent; asked at the next tick.
    wanted: u64,
    last_request: Option<u64>,
    /// Asks answered with nothing, in a row: the next one waits twice as long, up to
    /// `ttl / 8`.
    refusals: u32,
    /// Tokens drawn since the last tick, and the rate they made over the last tick: our
    /// own use, a claimant in the split beside the children.
    used: f64,
    use_rate: f64,
    /// Since when we have lent more than we hold: adopted children, or a cut from above. A
    /// keepalive round of grace before we cut them for it, since our own ask may cover it.
    over_since: Option<u64>,
}

impl<Id: Ord + Clone> Quota<Id> {
    fn new(limit: Limit) -> Self {
        Quota {
            limit,
            given: 0,
            returned: 0,
            tokens: 0.0,
            children: BTreeMap::new(),
            valid_until: None,
            wanted: 0,
            last_request: None,
            refusals: 0,
            used: 0.0,
            use_rate: 0.0,
            over_since: None,
        }
    }
    /// The share: units per tick.
    fn share(&self) -> u64 {
        self.given.saturating_sub(self.returned)
    }
    /// What the children hold from us.
    fn lent(&self) -> u64 {
        self.children.values().map(Booking::share).sum()
    }
    /// What we may still lend.
    fn room(&self) -> u64 {
        self.share().saturating_sub(self.lent())
    }
    fn overcommit(&self) -> u64 {
        self.lent().saturating_sub(self.share())
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
    /// Hand `amount` back to the parent, from the share.
    fn hand_back(&mut self, amount: u64) {
        self.returned = self.returned.saturating_add(amount.min(self.share()));
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
    /// Each child's weight in a split: the size of its subtree. Unknown children weigh one.
    weights: BTreeMap<Id, u64>,
    /// Something changed since the last call: call at the next tick.
    dirty: bool,
    last_call: u64,
    /// The `sent` of the last call the parent has not answered, and how many times it has
    /// been re-sent: a call is retried until answered, keepalive or not, or one lost
    /// keepalive would lapse the share.
    awaiting: Option<u64>,
    unanswered: u32,
    /// The `in_reply_to` of the last answer applied: `given` is adopted from answers in
    /// order, so an older answer cannot undo a newer gift.
    accepted: u64,
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
            weights: BTreeMap::new(),
            dirty: false,
            last_call: 0,
            awaiting: None,
            unanswered: 0,
            accepted: 0,
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

    /// Drop a limit: hand its share back to the parent and forget it. With nothing left in
    /// play, nothing more goes on the wire.
    pub fn remove_limit(&mut self, key: &K) -> bool {
        let was_empty = self.outbound.is_empty();
        if let Some(q) = self.quotas.remove(key) {
            if let Some(p) = self.parent.clone() {
                if q.given > 0 {
                    let req = self.request_for(&[(key.clone(), q.given)]);
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

    /// The children's weights for a split: each one's subtree size, from the same view the
    /// tree was computed from ([`tree::weights`]). A child not listed weighs one.
    pub fn set_weights(&mut self, weights: &[(Id, u64)]) {
        self.weights = weights.iter().cloned().collect();
    }

    /// The parent is known exactly -- computed from a cluster view every node shares, say --
    /// so take `peer` as parent now, with none of [`set_upstream`](Lease::set_upstream)'s
    /// hysteresis. A peer that was our child stops being one first, so a reordering that
    /// swaps a parent and a child cannot make a cycle. Nothing to do for the leader, for
    /// ourselves, or for the parent we already have.
    pub fn set_parent(&mut self, peer: Id) -> bool {
        let was_empty = self.outbound.is_empty();
        if self.is_leader() || peer == self.me || self.parent.as_ref() == Some(&peer) {
            self.parent_misses = 0;
            return false;
        }
        if self.children().any(|c| *c == peer) {
            self.forget_child(&peer);
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

    // ------------------------------------------------------------ the protocol

    /// A child's call. Book what it returned, work out its entitlement, give or cut, and
    /// answer with the edge's counters. On the TX thread of the parent.
    pub fn on_request(&mut self, from: Id, req: LeaseRequest<Id, K>) -> LeaseResponse<Id, K> {
        if req.term > self.term {
            if let Some(l) = req.leader.clone() {
                self.set_cluster_view(None, l, req.term);
            }
        }
        let ttl = self.cfg.ttl;
        let now = self.now;
        let can_ask = self.parent.is_some();
        let weight = |c: &Id| self.weights.get(c).copied().unwrap_or(1);
        let mut items = Vec::with_capacity(req.items.len());
        for it in req.items {
            let Some(q) = self.quotas.get_mut(&it.key) else {
                continue;
            };
            // Merge what the child returned. Its share is what we gave less that.
            let (was_need, was_want) = q
                .children
                .get(&from)
                .map_or((0, 0), |b| (b.needed, b.wanted));
            let b = q.children.entry(from.clone()).or_default();
            let mut came_back = 0;
            if it.returned > b.returned {
                let returned = it.returned.min(b.given);
                came_back = returned - b.returned;
                b.returned = returned;
                self.dirty = true;
            }
            let booked = b.share();
            // What the child wants is beyond the share it knows of; what we gave since
            // covers it first.
            let known = it.given.min(b.given).saturating_sub(b.returned);
            let (share, lent) = (q.share(), q.lent());
            if lent > share && q.over_since.is_none() {
                q.over_since = Some(now);
            }
            // The child's entitlement: our share split among every claimant by weighted
            // max-min fairness on what each holds and wants -- this child as it just
            // reported, the others as booked, and our own use at weight one. Claimants in
            // id order, so a remainder always lands on the same child whoever is asking.
            let demand = known
                .saturating_add(it.needed)
                .saturating_add(it.wanted)
                .max(booked);
            let mut claims: Vec<(u64, u64)> = Vec::with_capacity(q.children.len() + 2);
            let mut mine = None;
            for (c, b) in &q.children {
                if *c == from {
                    continue;
                }
                if mine.is_none() && *c > from {
                    mine = Some(claims.len());
                    claims.push((weight(&from), demand));
                }
                claims.push((
                    weight(c),
                    b.share().saturating_add(b.needed).saturating_add(b.wanted),
                ));
            }
            let mine = mine.unwrap_or_else(|| {
                claims.push((weight(&from), demand));
                claims.len() - 1
            });
            let own = (q.use_rate.ceil() as u64).saturating_add(q.wanted);
            claims.push((1, own));
            let fair = water_fill(share, &claims)[mine];
            // Past a chunk over its entitlement the child is cut to it -- past anything at
            // all while we are over our own share, or a unit's overcommit would sit on
            // every level for good. Not while a keepalive round of grace runs on that
            // overcommit, though: children we adopted, or a cut from above, are asked for
            // upward first, and our own answer may cover them. A root has nobody to ask.
            // Otherwise the child may take up to its entitlement, but never more than we
            // hold: the others are cut on their own keepalives, and the room frees up.
            let over = lent > share;
            let grace =
                over && can_ask && !q.over_since.is_some_and(|since| now >= since + ttl / 2);
            let slack = if over { 0 } else { q.limit.chunk };
            let room = share.saturating_sub(lent);
            let total = if grace || booked <= fair.saturating_add(slack) {
                demand
                    .min(fair)
                    .max(booked)
                    .min(booked.saturating_add(room))
            } else {
                fair
            };
            // Served: what the child got beyond the share it knew of, needs first.
            let served = total.saturating_sub(known);
            let give_need = served.min(it.needed);
            let give_want = (served - give_need).min(it.wanted);
            let short_need = it.needed - give_need;
            let short_want = it.wanted - give_want;
            // What a child gave back was fetched for it and is the tree's, not ours: hand
            // it back up, or a share that moved is held twice, here and under its new
            // parent. If we want more, we ask like anyone. A root keeps it: it is the rate.
            if came_back > 0 && can_ask {
                let back = came_back.min(share.saturating_sub(lent));
                q.hand_back(back);
            }
            let b = q.children.get_mut(&from).expect("booked above");
            if total > booked {
                b.given += total - booked;
            } else if total < booked {
                // A cut: count the difference as returned. The child takes the larger of
                // its own count and ours.
                b.returned += booked - total;
            }
            if (short_need > was_need || short_want > was_want) && can_ask {
                // New demand from below: ask upward at the next tick. A shortfall already
                // known is already asked for; asking again on every keepalive would be a
                // call per tick with a few short children.
                q.last_request = None;
            }
            b.expires = now + ttl;
            b.wanted = short_want;
            b.needed = short_need;
            items.push(ResponseItem {
                key: it.key,
                given: b.given,
                returned: b.returned,
            });
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
    /// the term. `given` is adopted from answers in order; `returned` is merged by max.
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
        if resp.in_reply_to < self.accepted {
            return self.woke(was_empty);
        }
        self.accepted = resp.in_reply_to;
        if self.awaiting.is_some_and(|sent| resp.in_reply_to >= sent) {
            self.awaiting = None;
            self.unanswered = 0;
        }
        let ttl = self.cfg.ttl;
        for it in resp.items {
            let Some(q) = self.quotas.get_mut(&it.key) else {
                continue;
            };
            let old = q.share();
            q.given = it.given;
            q.returned = q.returned.max(it.returned);
            let share = q.share();
            if share > old {
                let got = share - old;
                // What arrived goes to the children waiting for it first; only the rest
                // counts against our own want, or a child would take what we asked for.
                let spoken_for = q.need().saturating_add(q.earmarked()).min(got);
                q.wanted = q.wanted.saturating_sub(got - spoken_for);
                q.refusals = 0;
            } else if q.ask() > 0 {
                q.refusals = q.refusals.saturating_add(1);
            }
            if share < old {
                // A cut: the bucket shrinks to one tick of the new share, no burst, or every
                // cut would leak one; and the children are cut on their keepalives, since
                // our split now has less to split.
                q.tokens = q.tokens.min(q.refill().max(1.0).max(q.limit.burst as f64));
                self.dirty = true;
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
            q.use_rate = q.used / n as f64;
            q.used = 0.0;
            q.over_since = if q.lent() > q.share() {
                Some(q.over_since.unwrap_or(now))
            } else {
                None
            };
            let refill = q.refill();
            let cap = (refill.max(1.0) * 2.0).max(q.limit.burst as f64);
            q.tokens = (q.tokens + refill * n as f64).min(cap);
            // A child that stopped calling: its share is ours again. The counters stay, so
            // that it can come back and catch up.
            let mut expired = 0;
            for b in q.children.values_mut() {
                if b.expires < now && b.active() {
                    expired += b.share();
                    b.returned = b.given;
                    b.wanted = 0;
                    b.needed = 0;
                    self.dirty = true;
                }
            }
            if expired > 0 && !leader {
                let back = expired.min(q.room());
                q.hand_back(back);
            }
            // A lapsed share has been re-lent by the parent: keep only what our children
            // hold, so the next report does not claim it.
            // No share at all counts as lapsed too: a demoted leader's leftover, say.
            let lapsed = q.valid_until.map_or(true, |v| now > v);
            if !leader && lapsed && q.share() > q.lent() {
                let back = q.share() - q.lent();
                q.hand_back(back);
                self.dirty = true;
            }
        }
        if !leader && self.parent.is_some() && self.call_due() {
            self.call();
        }
        self.woke(was_empty)
    }

    /// Call the parent now if a call is due, without moving the clock: after an `acquire`
    /// that put a key in play, so the ask does not wait for the next tick.
    pub fn poke(&mut self) -> bool {
        let was_empty = self.outbound.is_empty();
        if !self.is_leader() && self.parent.is_some() && self.call_due() {
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
            q.used += *amount as f64;
        }
        Ok(())
    }

    // ------------------------------------------------------------ observation

    /// Whether this node is the leader.
    pub fn is_leader(&self) -> bool {
        self.leader.as_ref() == Some(&self.me)
    }

    /// The node this one leases from, if any.
    pub fn parent(&self) -> Option<&Id> {
        self.parent.as_ref()
    }

    /// The nodes that lease from this one, on any limit.
    pub fn children(&self) -> impl Iterator<Item = &Id> {
        let mut out: Vec<&Id> = self
            .quotas
            .values()
            .flat_map(|q| {
                q.children
                    .iter()
                    .filter(|(_, b)| b.active())
                    .map(|(c, _)| c)
            })
            .collect();
        out.sort();
        out.dedup();
        out.into_iter()
    }

    /// The limits configured on this node.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.quotas.keys()
    }

    /// One limit as this node sees it.
    pub fn stats(&self, key: &K) -> Option<Stats> {
        let q = self.quotas.get(key)?;
        Some(Stats {
            granted: q.share(),
            lent: q.lent(),
            overcommit: q.overcommit(),
            wanted: q.wanted,
            children: q.children.values().filter(|b| b.active()).count(),
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

    /// The leader holds the whole rate: the edge above it is a fiction of `limit`.
    fn hold_limit(q: &mut Quota<Id>) {
        let lent = q.lent();
        q.returned = 0;
        q.given = q.limit.limit.max(lent);
        q.valid_until = Some(u64::MAX);
        q.wanted = 0;
        q.refusals = 0;
    }

    /// Become the leader: hold the whole rate, and let the old parent go.
    fn crown(&mut self) {
        let release: Vec<(K, u64)> = self
            .quotas
            .iter()
            .filter(|(_, q)| q.given > 0)
            .map(|(k, q)| (k.clone(), q.given))
            .collect();
        if let Some(op) = self.parent.take() {
            if !release.is_empty() {
                let req = self.request_for(&release);
                self.outbound.push(Action::Call(op, req));
            }
        }
        for q in self.quotas.values_mut() {
            Self::hold_limit(q);
        }
        self.parent_misses = 0;
    }

    /// Stop being the leader: the rate is not ours; what our children hold, we now need.
    fn demote(&mut self) {
        for q in self.quotas.values_mut() {
            q.given = 0;
            q.returned = 0;
            q.valid_until = None;
        }
        self.parent = None;
        self.parent_misses = 0;
        self.accepted = 0;
    }

    /// A child that left the cluster: its share is ours, and its counters go.
    fn forget_child(&mut self, c: &Id) {
        for q in self.quotas.values_mut() {
            if q.children.remove(c).is_some() {
                self.dirty = true;
            }
        }
    }

    /// Lease from `peer` from now on. The old parent is told we hold nothing from it; the
    /// edge to the new one starts at zero, and what our children hold we ask for as need.
    fn reparent(&mut self, peer: Id) {
        self.awaiting = None;
        self.unanswered = 0;
        self.accepted = 0;
        let release: Vec<(K, u64)> = self
            .quotas
            .iter()
            .filter(|(_, q)| q.given > 0)
            .map(|(k, q)| (k.clone(), q.given))
            .collect();
        if let Some(op) = self.parent.take() {
            if !release.is_empty() {
                let req = self.request_for(&release);
                self.outbound.push(Action::Call(op, req));
            }
        }
        self.parent = Some(peer);
        self.parent_misses = 0;
        for q in self.quotas.values_mut() {
            q.given = 0;
            q.returned = 0;
            q.valid_until = None;
            q.refusals = 0;
        }
        self.call();
    }

    /// A call that hands `returned` back for each key: everything, to an old parent or for
    /// a removed limit.
    fn request_for(&self, release: &[(K, u64)]) -> LeaseRequest<Id, K> {
        LeaseRequest {
            term: self.term,
            leader: self.leader.clone(),
            sent: self.now,
            items: release
                .iter()
                .map(|(k, returned)| RequestItem {
                    key: k.clone(),
                    given: *returned,
                    returned: *returned,
                    needed: 0,
                    wanted: 0,
                })
                .collect(),
        }
    }

    /// Whether it is time to call the parent: something changed, the keepalive is due, a
    /// call went unanswered, or something is wanted and the retry has run out. A retry
    /// waits a round trip at first -- the parent may be fetching it -- and twice as long
    /// after each empty answer, up to `ttl / 8`.
    fn call_due(&self) -> bool {
        let now = self.now;
        if self.dirty || now >= self.last_call + self.cfg.ttl / 2 {
            return true;
        }
        let cap = Self::retry_cap(self.cfg.ttl);
        if let Some(sent) = self.awaiting {
            let wait = (2u64 << self.unanswered.min(16)).min(cap).max(1);
            if now >= sent + wait {
                return true;
            }
        }
        self.quotas.values().any(|q| {
            let wait = (2u64 << q.refusals.saturating_sub(1).min(16))
                .min(cap)
                .max(1);
            q.ask() > 0 && q.last_request.map_or(true, |t| now >= t + wait)
        })
    }

    /// Call the parent: every limit in play, what we returned and what we want, after
    /// handing back what we do not use. Nothing in play: nothing on the wire.
    fn call(&mut self) {
        let Some(parent) = self.parent.clone() else {
            return;
        };
        let now = self.now;
        self.dirty = false;
        self.last_call = now;
        if self.awaiting.is_some() {
            self.unanswered = self.unanswered.saturating_add(1);
        }
        let mut items = Vec::new();
        for (key, q) in self.quotas.iter_mut() {
            let in_play = q.given > 0 || q.lent() > 0 || q.ask() > 0;
            if !in_play {
                continue;
            }
            // While the bucket is full we are not using our share: keep one chunk beyond
            // what we lent, and never what a child is waiting for; hand back the rest.
            let full = q.tokens >= (q.refill().max(1.0) * 2.0).max(q.limit.burst as f64);
            if full {
                let keep = q
                    .lent()
                    .saturating_add(q.limit.chunk)
                    .saturating_add(q.earmarked())
                    .saturating_add(q.reserved());
                if q.share() > keep {
                    let back = q.share() - keep;
                    q.hand_back(back);
                }
            }
            let (need, want) = (q.need(), q.want());
            if need > 0 || want > 0 {
                q.last_request = Some(now);
            }
            items.push(RequestItem {
                key: key.clone(),
                given: q.given,
                returned: q.returned,
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
            items,
        };
        self.outbound.push(Action::Call(parent, req));
        self.awaiting = Some(now);
    }
}

/// Weighted max-min fairness: split `total` among claimants of `(weight, demand)`. A common
/// level per unit of weight rises until the total is spent; a claimant whose demand is below
/// its level gets its demand, and the rest is shared among those still asking, in proportion
/// to weight. Integer; a remainder goes to the earliest claimants still asking.
fn water_fill(total: u64, claims: &[(u64, u64)]) -> Vec<u64> {
    let mut out = vec![0u64; claims.len()];
    let mut active: Vec<usize> = (0..claims.len()).collect();
    let mut left = total;
    loop {
        let weight: u64 = active.iter().map(|&i| claims[i].0.max(1)).sum();
        if weight == 0 || left == 0 {
            break;
        }
        // Satisfy everyone whose demand fits under the level.
        let satisfied: Vec<usize> = active
            .iter()
            .copied()
            .filter(|&i| {
                claims[i].1.saturating_mul(weight) <= left.saturating_mul(claims[i].0.max(1))
            })
            .collect();
        if satisfied.is_empty() {
            // Everyone left wants more than its level: share what is left by weight.
            let mut given = 0;
            for &i in &active {
                out[i] = left * claims[i].0.max(1) / weight;
                given += out[i];
            }
            let mut rest = left - given;
            for &i in &active {
                if rest == 0 {
                    break;
                }
                out[i] += 1;
                rest -= 1;
            }
            break;
        }
        for i in satisfied {
            out[i] = claims[i].1;
            left -= claims[i].1;
            active.retain(|&a| a != i);
        }
        if active.is_empty() {
            break;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const RPS: &str = "rps";

    fn rate() -> Limit {
        Limit {
            limit: 30,
            chunk: 2,
            burst: 0,
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
        assert_eq!(resp.items[0].given, 0, "the middle node has nothing");
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
        l.set_limit(
            RPS,
            Limit {
                limit: 0,
                chunk: 2,
                burst: 0,
            },
        ); // nothing to lend
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
        // The edge to the new parent starts at zero: the child asks it at its next
        // write, and is leased again within a round trip or two.
        assert_eq!(ns[3].stats(&RPS).unwrap().granted, 0);
        for _ in 0..4 {
            let _ = ns[3].acquire(&[(RPS, 1)]);
            step(&mut ns);
        }
        assert_eq!(ns[2].stats(&RPS).unwrap().lent, 2, "the new parent lends");
        assert_eq!(
            ns[3].stats(&RPS).unwrap().granted,
            2,
            "the child is leased again"
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
                    given: 500,
                    returned: 0,
                }],
            },
        );
        assert_eq!(ns[1].stats(&RPS).unwrap().granted, 2);
        assert_eq!(ns[1].term(), 5);
    }

    #[test]
    fn an_acquire_is_all_or_none() {
        let mut l = node(1);
        l.set_limit(
            "bps",
            Limit {
                limit: 5,
                chunk: 1,
                burst: 0,
            },
        );
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
        // The middle node does not keep the 2 for itself: at its next call it hands it up,
        // and the grandchild, writing on, is leased by the leader directly.
        for _ in 0..3 {
            let _ = ns[2].acquire(&[(RPS, 1)]);
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
                n.set_limit(
                    RPS,
                    Limit {
                        limit: 6,
                        chunk: 2,
                        burst: 0,
                    },
                );
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
        // C is entitled to 2 of the 6 by the split, but A is over its own entitlement by one
        // chunk exactly, and a cut only comes past a chunk: C waits.
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

    #[test]
    fn set_parent_switches_at_once_and_unbooks_a_child_that_becomes_the_parent() {
        let cfg = Config { ttl: 40 };
        let mut n = Lease::<u32, &str>::new(2, cfg);
        n.set_limit(
            "k",
            Limit {
                limit: 100,
                chunk: 10,
                burst: 0,
            },
        );
        n.set_cluster_view(Some(&[1, 2, 3]), 1, 1);
        // Node 3 leases from 2, so it is 2's child.
        let req = LeaseRequest {
            term: 1,
            leader: Some(1),
            sent: 1,
            items: vec![RequestItem {
                key: "k",
                given: 0,
                returned: 0,
                needed: 0,
                wanted: 10,
            }],
        };
        n.on_request(3, req);
        assert!(n.children().any(|c| *c == 3));
        // `set_upstream` would wait for a second signal; `set_parent` does not.
        n.set_parent(1);
        assert_eq!(n.parent(), Some(&1));
        n.set_parent(3); // a reorder makes the child the parent
        assert_eq!(n.parent(), Some(&3));
        assert!(
            !n.children().any(|c| *c == 3),
            "no longer our child: no cycle"
        );
        n.set_parent(3);
        assert_eq!(n.parent(), Some(&3), "already the parent: unchanged");
    }

    #[test]
    fn a_lost_keepalive_is_retried_and_the_share_does_not_lapse() {
        let cfg = Config { ttl: 20 };
        let mut leader = Lease::<u32, &str>::new(1, cfg);
        let mut child = Lease::<u32, &str>::new(2, cfg);
        for l in [&mut leader, &mut child] {
            l.set_limit(
                "k",
                Limit {
                    limit: 100,
                    chunk: 10,
                    burst: 0,
                },
            );
            l.set_cluster_view(Some(&[1, 2]), 1, 1);
        }
        child.set_parent(1);
        // The child gets a share.
        child.acquire(&[("k", 1)]).ok();
        child.tick(1);
        let calls = child.ready();
        let Some(Action::Call(1, req)) = calls.into_iter().next() else {
            panic!("the child asks its parent");
        };
        let resp = leader.on_request(2, req);
        child.on_response(1, resp);
        assert!(child.stats(&"k").unwrap().granted > 0);
        assert!(child.stats(&"k").unwrap().good);
        // Keep the bucket busy so nothing is handed back; the keepalive at ttl/2 is lost.
        let mut lost = None;
        while lost.is_none() {
            child.tick(1);
            child.acquire(&[("k", 1)]).ok();
            for a in child.ready() {
                let Action::Call(_, r) = a;
                lost = Some(r);
            }
        }
        let lost = lost.unwrap();
        // Without a retry the next call would be the next keepalive, at the TTL, and its
        // answer would land after the share had lapsed. The retry comes within a few ticks.
        let mut retried = None;
        for _ in 0..(cfg.ttl / 4) {
            child.tick(1);
            child.acquire(&[("k", 1)]).ok();
            for a in child.ready() {
                let Action::Call(_, r) = a;
                retried = Some(r);
            }
            if retried.is_some() {
                break;
            }
        }
        let retried = retried.expect("the lost call is sent again");
        assert!(retried.sent > lost.sent);
        let resp = leader.on_request(2, retried);
        child.on_response(1, resp);
        // Past the old validity the share is still good, and never dropped to zero.
        while child.now <= lost.sent + cfg.ttl + 1 {
            child.tick(1);
            child.acquire(&[("k", 1)]).ok();
            let _ = child.ready();
            assert!(
                child.stats(&"k").unwrap().granted > 0,
                "the share lapsed at {}",
                child.now
            );
        }
        assert!(child.stats(&"k").unwrap().good);
    }

    #[test]
    fn water_fill_gives_demands_below_the_level_and_shares_the_rest_by_weight() {
        // The example: a root of 75, its own use 2, a 25-node child wanting 50, a leaf
        // wanting 100.
        assert_eq!(
            water_fill(75, &[(1, 2), (25, 50), (1, 100)]),
            vec![2, 50, 23]
        );
        // Everyone wants more than its level: by weight, remainder to the first.
        assert_eq!(water_fill(30, &[(1, 100), (3, 100)]), vec![8, 22]);
        assert_eq!(water_fill(10, &[(1, 3), (1, 3)]), vec![3, 3]);
        assert_eq!(water_fill(0, &[(1, 3)]), vec![0]);
    }

    fn leader_with(limit: u64) -> Lease<u32, &'static str> {
        let mut l = Lease::new(1, Config { ttl: 20 });
        l.set_limit(
            RPS,
            Limit {
                limit,
                chunk: 2,
                burst: 0,
            },
        );
        l.set_cluster_view(Some(&[1, 2, 3]), 1, 1);
        l
    }

    fn ask(needed: u64, wanted: u64, sent: u64) -> LeaseRequest<u32, &'static str> {
        LeaseRequest {
            term: 1,
            leader: Some(1),
            sent,
            items: vec![RequestItem {
                key: RPS,
                given: 0,
                returned: 0,
                needed,
                wanted,
            }],
        }
    }

    fn share(r: &LeaseResponse<u32, &'static str>) -> u64 {
        r.items[0].given - r.items[0].returned
    }

    #[test]
    fn two_children_wanting_everything_get_half_each() {
        let mut l = leader_with(75);
        let _ = share(&l.on_request(2, ask(0, 100, 1)));
        let _ = share(&l.on_request(3, ask(0, 100, 1)));
        // The first asker took what its level allowed before the second was known; a
        // second keepalive settles both at half, the leader's own use taking nothing.
        let a2 = share(&l.on_request(2, ask(0, 100, 2)));
        let b2 = share(&l.on_request(3, ask(0, 100, 2)));
        assert!(a2.abs_diff(b2) <= 1, "{a2} vs {b2}");
        assert!((74..=75).contains(&(a2 + b2)), "{a2} + {b2}");
    }

    #[test]
    fn a_child_s_share_follows_its_weight() {
        let mut l = leader_with(80);
        l.set_weights(&[(2, 1), (3, 3)]);
        let mut a = 0;
        let mut b = 0;
        for round in 1..4 {
            a = share(&l.on_request(2, ask(0, 100, round)));
            b = share(&l.on_request(3, ask(0, 100, round)));
        }
        assert_eq!((a, b), (20, 60), "one to three");
    }

    #[test]
    fn a_root_never_gives_more_than_it_holds_and_settles_a_fair_split() {
        // After a leader change two children each need 48 for their subtrees, against a
        // limit of 75. The root gives what it holds, no more, and the keepalives settle
        // both at half.
        let mut l = leader_with(75);
        let a0 = share(&l.on_request(2, ask(48, 0, 1)));
        let b0 = share(&l.on_request(3, ask(48, 0, 1)));
        assert!(a0 + b0 <= 75, "{a0} + {b0}");
        assert_eq!(
            l.stats(&RPS).unwrap().overcommit,
            0,
            "a root cannot be overcommitted"
        );
        let a = share(&l.on_request(2, ask(48, 0, 2)));
        let b = share(&l.on_request(3, ask(48, 0, 2)));
        assert!(a.abs_diff(b) <= 1, "{a} vs {b}");
        assert!((74..=75).contains(&(a + b)), "{a} + {b}");
        assert!(l.stats(&RPS).unwrap().lent <= 75);
    }

    #[test]
    fn a_re_sent_request_gets_the_same_total_and_lends_nothing_more() {
        let mut l = leader_with(75);
        let first = l.on_request(2, ask(0, 10, 1));
        let lent = l.stats(&RPS).unwrap().lent;
        // The same request again, as a retry sends it: the child has not seen the answer.
        let again = l.on_request(2, ask(0, 10, 1));
        assert_eq!(again.items, first.items);
        assert_eq!(l.stats(&RPS).unwrap().lent, lent);
    }

    #[test]
    fn a_child_adopts_given_in_order_and_takes_the_larger_returned() {
        let mut ns = pair();
        let _ = ns[1].acquire(&[(RPS, 1)]);
        ns[1].tick(1);
        let Action::Call(_, req) = ns[1].ready().remove(0);
        let resp = ns[0].on_request(2, req);
        ns[1].on_response(1, resp.clone());
        let held = ns[1].stats(&RPS).unwrap().granted;
        assert!(held > 0);
        // A cut arrives as a larger `returned`: the share follows it down.
        let mut cut = resp.clone();
        cut.items[0].returned = cut.items[0].given - 1;
        ns[1].on_response(1, cut);
        assert_eq!(ns[1].stats(&RPS).unwrap().granted, 1);
        // An answer to an older request is ignored, however large.
        let mut stale = resp;
        stale.in_reply_to = 0;
        stale.items[0].given = 500;
        ns[1].on_response(1, stale);
        assert_eq!(ns[1].stats(&RPS).unwrap().granted, 1);
    }

    #[test]
    fn a_node_cut_below_what_it_lent_waits_a_keepalive_round_before_cutting_its_children() {
        let cfg = Config { ttl: 20 };
        let mut mid = Lease::<u32, &str>::new(2, cfg);
        mid.set_limit(
            "k",
            Limit {
                limit: 60,
                chunk: 1,
                burst: 0,
            },
        );
        mid.set_cluster_view(Some(&[1, 2, 3, 4, 5]), 1, 1);
        mid.set_parent(1);
        let answer = |in_reply_to: u64, given: u64, returned: u64| LeaseResponse {
            term: 1,
            leader: Some(1),
            in_reply_to,
            sent: in_reply_to,
            items: vec![ResponseItem {
                key: "k",
                given,
                returned,
            }],
        };
        let report = |sent: u64| LeaseRequest {
            term: 1,
            leader: Some(1),
            sent,
            items: vec![RequestItem {
                key: "k",
                given: 0,
                returned: 0,
                needed: 0,
                wanted: 2,
            }],
        };
        // The mid node holds 6 and lends 2 to each of three children.
        mid.tick(1);
        mid.on_response(1, answer(1, 6, 0));
        for c in [3, 4, 5] {
            assert_eq!(share(&mid.on_request(c, report(1))), 2);
        }
        assert_eq!(mid.stats(&"k").unwrap().lent, 6);
        // Its parent cuts it to nothing. It is over by 6 and asks upward for the need.
        mid.on_response(1, answer(2, 6, 6));
        assert_eq!(mid.stats(&"k").unwrap().overcommit, 6);
        mid.tick(1);
        let Some(Action::Call(1, req)) = mid.ready().into_iter().next() else {
            panic!("the mid node asks upward");
        };
        assert!(req.items[0].needed >= 6, "{:?}", req.items[0]);
        // For a keepalive round the children keep what they hold.
        for c in [3, 4, 5] {
            assert_eq!(
                share(&mid.on_request(c, report(3))),
                2,
                "child {c} keeps its share"
            );
        }
        // Half a TTL later with nothing covered, they are cut to a fair split of nothing.
        mid.tick(cfg.ttl / 2);
        for c in [3, 4, 5] {
            assert_eq!(share(&mid.on_request(c, report(14))), 0, "child {c} is cut");
        }
        assert_eq!(mid.stats(&"k").unwrap().overcommit, 0);
    }

    #[test]
    fn a_share_below_a_request_per_tick_saves_up_to_the_burst() {
        // A rate of 1 unit per tick, requests of 10 units: two ticks of share never fit
        // one; a burst of 10 does, once every ten ticks.
        let mut l = Lease::<u32, &str>::new(1, Config { ttl: 40 });
        l.set_limit(
            "k",
            Limit {
                limit: 1,
                chunk: 1,
                burst: 10,
            },
        );
        l.set_cluster_view(Some(&[1]), 1, 1);
        l.tick(9);
        assert!(l.acquire(&[("k", 10)]).is_err(), "nine ticks: nine units");
        l.tick(1);
        assert!(l.acquire(&[("k", 10)]).is_ok(), "ten ticks: one request");
        assert!(l.acquire(&[("k", 10)]).is_err());
        l.tick(100);
        assert!(l.acquire(&[("k", 10)]).is_ok());
        assert!(
            l.acquire(&[("k", 10)]).is_err(),
            "the bucket holds one request, not ten"
        );
    }
}
