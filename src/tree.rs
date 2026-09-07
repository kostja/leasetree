//! The tree as a pure function of a cluster view.
//!
//! Membership, who is online and who leads are facts every node learns the same way, from
//! the same replicated source, so every node can compute the same tree from them and no
//! messages are needed to agree on it. Each node needs only its own place.
//!
//! The rule is a trie over the ids themselves, so that a node's position depends on its id
//! alone and a change moves as few nodes as possible:
//!
//! - Inside a failure domain, `parent(x)` is `x` with its lowest nonzero base-`radix` digit
//!   cleared, climbing past ids that are not present. An id with no present ancestor hangs
//!   under the domain's lowest id, which is the domain's root.
//! - The domain roots hang under the leader.
//! - In the leader's own domain the tree is re-rooted at the leader: the path from the
//!   leader up to the domain's root flips direction, and nothing else moves.
//!
//! So a leader change moves the gateways and a path of about `log` nodes; a node leaving
//! moves only its children, up to their grandparent; a node joining lands at its own place
//! and takes back the children that were climbing past it. Depth is the number of nonzero
//! digits in an id, about `log_radix` of the largest id, plus the flipped path; a node has
//! at most `(radix - 1) * digits` children.

/// Where a node stands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Place<Id> {
    /// The leader.
    Root,
    /// A child of this node.
    Under(Id),
}

/// `me`'s place in the tree over `members`, each with its failure domain, rooted at
/// `leader`, with ids read as base-`radix` numbers. `None` when `me` or `leader` is not a
/// member: there is no tree for `me`.
pub fn place<Id, D>(me: &Id, leader: &Id, members: &[(Id, D)], radix: usize) -> Option<Place<Id>>
where
    Id: Ord + Copy + Into<u64>,
    D: Eq,
{
    let k = u64::try_from(radix.max(2)).unwrap_or(u64::MAX);
    let my_domain = &members.iter().find(|(id, _)| id == me)?.1;
    let leader_domain = &members.iter().find(|(id, _)| id == leader)?.1;
    let mine: Vec<Id> = members
        .iter()
        .filter(|(_, d)| d == my_domain)
        .map(|(id, _)| *id)
        .collect();
    let min = *mine.iter().min()?;
    let by_value = |v: u64| mine.iter().copied().find(|id| (*id).into() == v);
    // The nearest present ancestor, else the domain's root; `None` for the root itself.
    let trie_parent = |id: Id| -> Option<Id> {
        if id == min {
            return None;
        }
        let mut x: u64 = id.into();
        loop {
            let mut p = 1u64;
            while (x / p) % k == 0 && p <= x {
                p *= k;
            }
            if p > x {
                return Some(min);
            }
            x -= (x / p % k) * p;
            if let Some(a) = by_value(x) {
                return Some(a);
            }
            if x == 0 {
                return Some(min);
            }
        }
    };
    if me == leader {
        return Some(Place::Root);
    }
    if my_domain != leader_domain {
        return Some(match trie_parent(*me) {
            None => Place::Under(*leader),
            Some(p) => Place::Under(p),
        });
    }
    // The leader's domain: the path from the leader up to the root flips direction.
    let mut path = vec![*leader];
    let mut at = *leader;
    while let Some(p) = trie_parent(at) {
        path.push(p);
        at = p;
    }
    if let Some(i) = path.iter().position(|x| x == me) {
        return Some(Place::Under(path[i - 1]));
    }
    Some(Place::Under(trie_parent(*me)?))
}

/// The subtree size of each of `me`'s children in the tree over `members` rooted at
/// `leader`: the number of members whose chain of parents passes through the child, the
/// child included. The weights for [`Lease::set_weights`](crate::Lease::set_weights).
pub fn weights<Id, D>(me: &Id, leader: &Id, members: &[(Id, D)], radix: usize) -> Vec<(Id, u64)>
where
    Id: Ord + Copy + Into<u64>,
    D: Eq,
{
    let parents: Vec<(Id, Option<Id>)> = members
        .iter()
        .filter_map(|(id, _)| {
            place(id, leader, members, radix).map(|p| match p {
                Place::Root => (*id, None),
                Place::Under(p) => (*id, Some(p)),
            })
        })
        .collect();
    let parent_of = |id: Id| parents.iter().find(|(x, _)| *x == id).and_then(|(_, p)| *p);
    let mut out: Vec<(Id, u64)> = parents
        .iter()
        .filter(|(_, p)| *p == Some(*me))
        .map(|(c, _)| (*c, 0))
        .collect();
    for (id, _) in &parents {
        // Climb to the root; every child of `me` on the way counts this member once.
        let mut at = Some(*id);
        let mut hops = 0;
        while let Some(x) = at {
            if let Some(w) = out.iter_mut().find(|(c, _)| *c == x) {
                w.1 += 1;
            }
            at = parent_of(x);
            hops += 1;
            if hops > parents.len() {
                break;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// Every node's parent, and the depth of the deepest node.
    fn tree(members: &[(u64, u8)], leader: u64, k: usize) -> (BTreeMap<u64, Option<u64>>, usize) {
        let mut parents = BTreeMap::new();
        for (id, _) in members {
            let p = match place(id, &leader, members, k).expect("a member has a place") {
                Place::Root => None,
                Place::Under(p) => Some(p),
            };
            parents.insert(*id, p);
        }
        let mut deepest = 0;
        for id in parents.keys() {
            let mut depth = 0;
            let mut at = *id;
            while let Some(p) = parents[&at] {
                depth += 1;
                at = p;
                assert!(depth <= members.len(), "a cycle through {id}");
            }
            deepest = deepest.max(depth);
        }
        (parents, deepest)
    }

    fn by_mod(n: u64, domains: u64) -> Vec<(u64, u8)> {
        (1..=n).map(|id| (id, (id % domains) as u8)).collect()
    }

    #[test]
    fn nine_nodes_in_three_domains_enter_each_domain_once() {
        // Domain = id mod 3, leader 5 in domain 2, radix 2.
        let members = by_mod(9, 3);
        let (parents, depth) = tree(&members, 5, 2);
        let edge = |c: u64| parents[&c];
        assert_eq!(edge(5), None, "the leader is the root");
        // The leader's domain {2, 5, 8}: the root is 2; 5 climbs 4, 0 to it. Re-rooted at
        // 5, the path 5 -> 2 flips; 8 climbs to 2.
        assert_eq!((edge(2), edge(8)), (Some(5), Some(2)));
        // Domain 0 {3, 6, 9}: root 3 under the leader; 6 and 9 climb past absent ids to 3.
        assert_eq!((edge(3), edge(6), edge(9)), (Some(5), Some(3), Some(3)));
        // Domain 1 {1, 4, 7}: root 1 under the leader; 4 climbs to 1; 7 -> 6 absent -> 4.
        assert_eq!((edge(1), edge(4), edge(7)), (Some(5), Some(1), Some(4)));
        assert_eq!(depth, 3);
        let domain = |id: u64| members[(id - 1) as usize].1;
        let cross = parents
            .iter()
            .filter(|(c, p)| p.is_some_and(|p| domain(**c) != domain(p)))
            .count();
        assert_eq!(cross, 2, "one entry per other domain");
    }

    #[test]
    fn a_dead_gateway_is_replaced_by_the_next_id_and_only_its_domain_moves() {
        let mut members = by_mod(9, 3);
        let before = tree(&members, 5, 2).0;
        members.retain(|(id, _)| *id != 3);
        let after = tree(&members, 5, 2).0;
        assert_eq!(
            (after[&6], after[&9]),
            (Some(5), Some(6)),
            "6 is the root now, 9 under it"
        );
        for id in [1, 2, 4, 7, 8] {
            assert_eq!(after[&id], before[&id], "{id} did not move");
        }
    }

    #[test]
    fn a_leader_change_flips_one_path_and_re_hangs_the_gateways() {
        let members = by_mod(9, 3);
        let before = tree(&members, 5, 2).0;
        let after = tree(&members, 6, 2).0; // the new leader is in domain 0
        assert_eq!(after[&6], None);
        assert_eq!(after[&3], Some(6), "the path 6 -> 3 flipped");
        assert_eq!(after[&9], Some(3), "9 did not move");
        assert_eq!(after[&1], Some(6), "domain 1's root follows the leader");
        assert_eq!((after[&4], after[&7]), (before[&4], before[&7]));
        // The old leader's domain goes back to its own shape: root 2 under the leader,
        // 5 and 8 under 2.
        assert_eq!(after[&2], Some(6));
        assert_eq!((after[&5], after[&8]), (Some(2), Some(2)));
        let moved = after.iter().filter(|(id, p)| before[id] != **p).count();
        assert_eq!(moved, 5, "6, 3, 1, 2 and 5 moved; {moved} did");
    }

    #[test]
    fn two_hundred_nodes_in_one_domain_stay_shallow_and_a_join_moves_nobody() {
        let members = by_mod(200, 1);
        let (before, depth) = tree(&members, 7, 4);
        assert!(depth <= 6, "depth {depth}");
        let fan = |parents: &BTreeMap<u64, Option<u64>>, id: u64| {
            parents.values().filter(|p| **p == Some(id)).count()
        };
        assert!(
            fan(&before, 7) <= 16,
            "the root has {} children",
            fan(&before, 7)
        );
        let mut more = members.clone();
        more.push((201, 0));
        let (after, _) = tree(&more, 7, 4);
        for (id, p) in &before {
            assert_eq!(after[id], *p);
        }
        assert_eq!(after[&201], Some(200), "201 is 3021 in base 4: under 3020");
    }

    #[test]
    fn a_join_takes_back_the_children_that_climbed_past_it() {
        // Ids 1..=15 but 4, radix 4: 5, 6 and 7 climb past the absent 4 to the root 1.
        let mut members: Vec<(u64, u8)> =
            (1..=15).filter(|id| *id != 4).map(|id| (id, 0)).collect();
        let before = tree(&members, 1, 4).0;
        assert_eq!(
            (before[&5], before[&6], before[&7]),
            (Some(1), Some(1), Some(1))
        );
        members.push((4, 0));
        let after = tree(&members, 1, 4).0;
        assert_eq!(
            (after[&5], after[&6], after[&7]),
            (Some(4), Some(4), Some(4))
        );
        assert_eq!(after[&4], Some(1));
        let moved = after
            .iter()
            .filter(|(id, p)| before.get(id) != Some(p))
            .count();
        assert_eq!(moved, 4, "4 and its three children");
    }

    #[test]
    fn no_tree_without_a_member_leader_or_for_a_stranger() {
        let members = by_mod(4, 2);
        assert_eq!(
            place(&1, &9, &members, 2),
            None,
            "the leader is not a member"
        );
        assert_eq!(place(&9, &1, &members, 2), None, "we are not a member");
        assert_eq!(place(&1, &1, &members, 2), Some(Place::Root));
    }

    #[test]
    fn weights_are_subtree_sizes() {
        // One domain, radix 4, root 1: 4 carries 5, 6, 7; 16 carries 17..=31; 2 is a leaf.
        let members = by_mod(31, 1);
        let w: BTreeMap<u64, u64> = weights(&1, &1, &members, 4).into_iter().collect();
        assert_eq!(w[&2], 1);
        assert_eq!(w[&4], 4);
        assert_eq!(w[&16], 16);
        assert_eq!(
            w.values().sum::<u64>(),
            30,
            "every other member is under one child"
        );
        // Nine nodes, three domains, leader 5: the gateways carry their domains.
        let members = by_mod(9, 3);
        let w: BTreeMap<u64, u64> = weights(&5, &5, &members, 2).into_iter().collect();
        assert_eq!((w[&3], w[&1], w[&2]), (3, 3, 2));
    }
}
