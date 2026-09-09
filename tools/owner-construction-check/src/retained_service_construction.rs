use super::*;

pub(super) fn find_retained_service_construction_violations(
    files: &[RustFile],
    retained_services: &BTreeSet<String>,
    authorities: &[(&str, &str)],
    composition_roots: &[(&str, &str, &str)],
) -> Vec<RetainedServiceConstructionViolation> {
    let authorities = authorities
        .iter()
        .filter(|(service, authority)| {
            retained_services.contains(*service) && retained_services.contains(*authority)
        })
        .map(|(service, authority)| ((*service).to_string(), (*authority).to_string()))
        .collect::<BTreeMap<_, _>>();
    let retained_services = retained_services
        .iter()
        .filter(|service| {
            !RETAINED_SERVICE_ROOT_TYPES.contains(&service.as_str())
                && !OPERATION_SCOPED_OWNER_TYPES.contains(&service.as_str())
                && !service.ends_with("Inner")
                && (!CAPABILITY_TYPES.contains(&service.as_str())
                    || authorities.contains_key(*service))
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    let associated_factories = collect_associated_factories(files, &retained_services);
    let free_constructors = collect_free_constructors(files, &retained_services);
    let mut violations = BTreeSet::new();
    for file in files {
        if is_test_source(&file.relative_path) {
            continue;
        }
        let mut visitor = ServiceConstructionSiteVisitor {
            path: &file.relative_path,
            retained_services: &retained_services,
            authorities: &authorities,
            composition_roots,
            associated_factories: &associated_factories,
            free_constructors: &free_constructors,
            current_callable: None,
            violations: &mut violations,
        };
        visitor.visit_file(&file.syntax);
    }
    violations.into_iter().collect()
}

pub(crate) fn is_test_source(path: &str) -> bool {
    path.contains("/tests/")
        || path.contains("/test_support/")
        || path.contains("_tests/")
        || path.ends_with("/tests.rs")
        || path.ends_with("_tests.rs")
        || path
            .rsplit('/')
            .next()
            .is_some_and(|name| name.starts_with("test_"))
        || path.ends_with("/test_helpers.rs")
        || path.ends_with("/test_support.rs")
}

pub(crate) fn is_test_only(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attribute| {
        attribute.path().is_ident("test")
            || (attribute.path().is_ident("cfg")
                && matches!(&attribute.meta, syn::Meta::List(list) if list.tokens.to_string().contains("test")))
    })
}

struct ServiceConstructionSiteVisitor<'a> {
    path: &'a str,
    retained_services: &'a BTreeSet<String>,
    authorities: &'a BTreeMap<String, String>,
    composition_roots: &'a [(&'a str, &'a str, &'a str)],
    associated_factories: &'a BTreeMap<(String, String), BTreeSet<String>>,
    free_constructors: &'a BTreeMap<String, BTreeSet<String>>,
    current_callable: Option<Constructor>,
    violations: &'a mut BTreeSet<RetainedServiceConstructionViolation>,
}

impl ServiceConstructionSiteVisitor<'_> {
    fn record(&mut self, service: &str, span: Span) {
        if !self.retained_services.contains(service) {
            return;
        }
        let Some(caller) = &self.current_callable else {
            return;
        };
        if caller.owner == service {
            return;
        }
        let defines_factory = if caller.owner == "<free>" {
            self.free_constructors
                .get(&caller.method)
                .is_some_and(|services| services.contains(service))
        } else {
            self.associated_factories
                .get(&(caller.owner.clone(), caller.method.clone()))
                .is_some_and(|services| services.contains(service))
        };
        if defines_factory {
            return;
        }
        if self.composition_roots.iter().any(|(path, owner, method)| {
            *path == self.path && *owner == caller.owner && *method == caller.method
        }) {
            return;
        }
        let authority = self.authorities.get(service).cloned();
        if authority.as_ref() == Some(&caller.owner) {
            return;
        }
        self.violations
            .insert(RetainedServiceConstructionViolation {
                path: self.path.to_string(),
                line: span.start().line,
                owner: caller.owner.clone(),
                method: caller.method.clone(),
                service: service.to_string(),
                authority,
            });
    }

    fn record_associated_factory(&mut self, owner: &str, method: &str, span: Span) {
        let Some(services) = self
            .associated_factories
            .get(&(owner.to_string(), method.to_string()))
        else {
            return;
        };
        for service in services {
            self.record(service, span);
        }
    }
}

impl<'ast> Visit<'ast> for ServiceConstructionSiteVisitor<'_> {
    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        if is_test_only(&node.attrs) {
            return;
        }
        visit::visit_item_mod(self, node);
    }

    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        if is_test_only(&node.attrs) {
            return;
        }
        let previous = self.current_callable.clone();
        let owner = type_name(&node.self_ty).unwrap_or_else(|| "<impl>".to_string());
        for item in &node.items {
            let syn::ImplItem::Fn(method) = item else {
                continue;
            };
            if is_test_only(&method.attrs) {
                continue;
            }
            self.current_callable = Some(Constructor {
                owner: owner.clone(),
                method: method.sig.ident.to_string(),
            });
            self.visit_block(&method.block);
        }
        self.current_callable = previous;
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        if is_test_only(&node.attrs) {
            return;
        }
        let previous = self.current_callable.replace(Constructor {
            owner: "<free>".to_string(),
            method: node.sig.ident.to_string(),
        });
        self.visit_block(&node.block);
        self.current_callable = previous;
    }

    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        if let syn::Expr::Path(function) = node.func.as_ref() {
            let segments = function.path.segments.iter().collect::<Vec<_>>();
            if could_be_local_associated_function_path(&segments) {
                self.record_associated_factory(
                    &segments[segments.len() - 2].ident.to_string(),
                    &segments[segments.len() - 1].ident.to_string(),
                    node.span(),
                );
            }
            if could_be_free_function_path(&segments) {
                let method = segments
                    .last()
                    .expect("free function path has at least one segment");
                if let Some(services) = self.free_constructors.get(&method.ident.to_string()) {
                    for service in services {
                        self.record(service, node.span());
                    }
                }
            }
        }
        visit::visit_expr_call(self, node);
    }

    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        if matches!(node.receiver.as_ref(), syn::Expr::Path(path) if path.path.is_ident("self")) {
            if let Some(caller) = &self.current_callable {
                self.record_associated_factory(
                    &caller.owner.clone(),
                    &node.method.to_string(),
                    node.span(),
                );
            }
        }
        visit::visit_expr_method_call(self, node);
    }

    fn visit_expr_struct(&mut self, node: &'ast syn::ExprStruct) {
        if node.path.segments.len() == 1 {
            let service = node
                .path
                .segments
                .last()
                .expect("single-segment struct path has a segment");
            self.record(&service.ident.to_string(), node.span());
        }
        visit::visit_expr_struct(self, node);
    }
}

pub(super) fn could_be_local_associated_function_path(segments: &[&syn::PathSegment]) -> bool {
    segments.len() == 2
        || segments.first().is_some_and(|segment| {
            matches!(
                segment.ident.to_string().as_str(),
                "crate" | "self" | "super"
            )
        })
}
