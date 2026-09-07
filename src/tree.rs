//! The tree as a pure function of a cluster view.
//!
//! Membership, who is online and who leads are facts every node learns the same way, from
//! the same replicated source, so every node can compute the same tree from them and no
//! messages are needed to agree on it. Each node needs only its own place.
//!
//! The rule: the leader is the root. In every other failure domain the lowest id is the
//! gateway and hangs under the leader, so a domain is entered once. Inside a domain the
//! rest, by id, is a heap with fan-in `k`: position `i` hangs under position `(i - 1) / k`,
//! position 0 being the gateway, or the leader in its own domain.
//!
//! A node's parent moves only when the view does: a leader change reorders the old and the
//! new leader's domains and re-hangs the gateways; a node leaving shifts the positions after
//! it in its domain; a node joining lands at the end of its domain and moves nobody.

/// Where a node stands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Place<Id> {
    /// The leader.
    Root,
    /// A child of this node.
    Under(Id),
}

/// `me`'s place in the tree over `members`, each with its failure domain, rooted at
/// `leader`. `None` when `me` or `leader` is not a member: there is no tree for `me`.
pub fn place<Id, D>(me: &Id, leader: &Id, members: &[(Id, D)], fan_in: usize) -> Option<Place<Id>>
where
    Id: Ord + Clone,
    D: Eq,
{
    let my_domain = &members.iter().find(|(id, _)| id == me)?.1;
    let leader_domain = &members.iter().find(|(id, _)| id == leader)?.1;
    let leader_here = my_domain == leader_domain;
    let mut mine: Vec<&Id> = members
        .iter()
        .filter(|(id, d)| d == my_domain && (!leader_here || id != leader))
        .map(|(id, _)| id)
        .collect();
    mine.sort_unstable();
    if leader_here {
        mine.insert(0, leader);
    }
    let i = mine.iter().position(|id| *id == me)?;
    Some(match (i, leader_here) {
        (0, true) => Place::Root,
        (0, false) => Place::Under(leader.clone()),
        (i, _) => Place::Under(mine[(i - 1) / fan_in.max(1)].clone()),
    })
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
        // Domain = id mod 3, leader 5 in domain 2, fan-in 2.
        let members = by_mod(9, 3);
        let (parents, depth) = tree(&members, 5, 2);
        let edge = |c: u64| parents[&c];
        assert_eq!(edge(5), None, "the leader is the root");
        // The leader's domain: 5, then 2 and 8 by id, both under 5 with fan-in 2.
        assert_eq!((edge(2), edge(8)), (Some(5), Some(5)));
        // Domain 0: gateway 3 under the leader, 6 and 9 under 3.
        assert_eq!((edge(3), edge(6), edge(9)), (Some(5), Some(3), Some(3)));
        // Domain 1: gateway 1 under the leader, 4 and 7 under 1.
        assert_eq!((edge(1), edge(4), edge(7)), (Some(5), Some(1), Some(1)));
        assert_eq!(depth, 2);
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
            "6 is the gateway, 9 under it"
        );
        for id in [1, 2, 4, 7, 8] {
            assert_eq!(after[&id], before[&id], "{id} did not move");
        }
    }

    #[test]
    fn a_leader_change_reorders_the_two_domains_involved_and_the_gateways() {
        let members = by_mod(9, 3);
        let before = tree(&members, 5, 2).0;
        let after = tree(&members, 6, 2).0; // the new leader is in domain 0
        assert_eq!(after[&6], None);
        assert_eq!(
            (after[&3], after[&9]),
            (Some(6), Some(6)),
            "the old gateway is a child now"
        );
        assert_eq!(after[&1], Some(6), "domain 1's gateway follows the root");
        assert_eq!(
            (after[&4], after[&7]),
            (before[&4], before[&7]),
            "inside domain 1 nothing moves"
        );
        // The old leader's domain: its lowest id, 2, is the gateway now; 5 and 8 hang under it.
        assert_eq!(after[&2], Some(6));
        assert_eq!((after[&5], after[&8]), (Some(2), Some(2)));
    }

    #[test]
    fn two_hundred_nodes_in_one_domain_are_four_deep_and_a_join_moves_nobody() {
        let members = by_mod(200, 1);
        let (before, depth) = tree(&members, 7, 4);
        assert!(depth <= 4, "depth {depth}");
        let mut more = members.clone();
        more.push((201, 0));
        let (after, _) = tree(&more, 7, 4);
        for (id, p) in &before {
            assert_eq!(after[id], *p);
        }
        assert!(after[&201].is_some());
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
}
