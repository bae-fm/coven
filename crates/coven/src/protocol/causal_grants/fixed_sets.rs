use super::*;

pub(super) fn fixed_sets_from_attack_graph(
    attacks: &BTreeMap<usize, BTreeSet<usize>>,
    mandatory: &BTreeSet<usize>,
    cyclic_source_count: usize,
) -> Result<(Vec<BTreeSet<usize>>, usize), usize> {
    if cyclic_source_count > MAX_CYCLIC_REVOCATION_SOURCES {
        return Err(cyclic_source_count);
    }
    let components = attack_graph_components(attacks);
    let mut memo = BTreeSet::new();
    let mut fixed = BTreeSet::new();
    let mut explored = 0;
    search_fixed_sets(
        attacks,
        &components,
        mandatory.clone(),
        &mut memo,
        &mut fixed,
        &mut explored,
    );
    Ok((fixed.into_iter().collect(), explored))
}

pub(super) fn search_fixed_sets(
    attacks: &BTreeMap<usize, BTreeSet<usize>>,
    components: &[Vec<usize>],
    mut selected: BTreeSet<usize>,
    memo: &mut BTreeSet<BTreeSet<usize>>,
    fixed: &mut BTreeSet<BTreeSet<usize>>,
    explored: &mut usize,
) {
    if !memo.insert(selected.clone()) {
        return;
    }
    *explored += 1;
    loop {
        let attacked = selected
            .iter()
            .flat_map(|source| attacks[source].iter().copied())
            .collect::<BTreeSet<_>>();
        if !selected.is_disjoint(&attacked) {
            return;
        }
        let undecided = attacks
            .keys()
            .copied()
            .filter(|node| !selected.contains(node) && !attacked.contains(node))
            .collect::<BTreeSet<_>>();
        if undecided.is_empty() {
            fixed.insert(selected);
            return;
        }
        let forced = undecided.iter().copied().find(|node| {
            !undecided
                .iter()
                .any(|source| attacks[source].contains(node))
        });
        let Some(forced) = forced else {
            let pivot = components
                .iter()
                .flat_map(|component| component.iter())
                .find(|node| undecided.contains(node))
                .copied()
                .expect("an undecided attack-graph node exists");
            let alternatives = std::iter::once(pivot)
                .chain(
                    undecided
                        .iter()
                        .copied()
                        .filter(|source| attacks[source].contains(&pivot)),
                )
                .collect::<BTreeSet<_>>();
            for source in alternatives {
                let mut branch = selected.clone();
                branch.insert(source);
                search_fixed_sets(attacks, components, branch, memo, fixed, explored);
            }
            return;
        };
        selected.insert(forced);
    }
}

pub(super) fn attack_graph_components(
    attacks: &BTreeMap<usize, BTreeSet<usize>>,
) -> Vec<Vec<usize>> {
    fn visit(
        node: usize,
        edges: &BTreeMap<usize, BTreeSet<usize>>,
        seen: &mut BTreeSet<usize>,
        order: &mut Vec<usize>,
    ) {
        if !seen.insert(node) {
            return;
        }
        for target in &edges[&node] {
            if edges.contains_key(target) {
                visit(*target, edges, seen, order);
            }
        }
        order.push(node);
    }

    let mut order = Vec::new();
    let mut seen = BTreeSet::new();
    for node in attacks.keys().copied() {
        visit(node, attacks, &mut seen, &mut order);
    }
    let mut reverse = attacks
        .keys()
        .copied()
        .map(|node| (node, BTreeSet::new()))
        .collect::<BTreeMap<_, _>>();
    for (source, targets) in attacks {
        for target in targets {
            if let Some(incoming) = reverse.get_mut(target) {
                incoming.insert(*source);
            }
        }
    }
    let mut components = Vec::new();
    seen.clear();
    for node in order.into_iter().rev() {
        if seen.contains(&node) {
            continue;
        }
        let mut component_order = Vec::new();
        visit(node, &reverse, &mut seen, &mut component_order);
        component_order.sort_unstable();
        components.push(component_order);
    }
    components
}
