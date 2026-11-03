use super::*;
use crate::crate_code::checked;
use crate::owner_graph::OwnerGraph;

fn policy() -> Policy {
    Policy::fixture(
        r#"
        [database]
        capability = "SQLite"

        [[database.domains]]
        name = "listing"
        homes = ["crates/app-core/src/listing.rs"]
        tables = ["note_listing"]
        owner = "Listing"

        [[database.domains]]
        name = "search"
        homes = ["crates/app-core/src/search.rs"]
        tables = ["note_search"]
        owner = "Listing"
        "#,
    )
}

fn findings(source: &str) -> Vec<(usize, String)> {
    let files = checked(&[RustFile::fixture("crates/app-core/src/lib.rs", source)]);
    let policy = policy();
    find_domain_owner_forwarding(&files, &policy, &OwnerGraph::collect(&files, &policy))
        .into_iter()
        .map(|finding| (finding.line, finding.message))
        .collect()
}

/// A type whose every method passes its call, and its own parameters, on to
/// the owner it holds is a second name for the owner.
#[test]
fn a_type_that_only_forwards_to_a_domain_owner_is_found() {
    assert_eq!(
        findings(
            r#"
            struct Listing { connection: Connection }
            struct Notes { listing: Arc<Listing>, name: String }
            impl Notes {
                fn new(listing: Arc<Listing>) -> Self { Self { listing, name: String::new() } }
                async fn refresh(&self, id: Id, rows: &[Row]) -> Result<(), Error> {
                    self.listing.refresh(&id, rows).await.map_err(Error::from)
                }
                fn count(&self) -> Result<u64, Error> { Ok(self.listing.count()?) }
            }
            "#,
        ),
        [(
            3,
            "every method of Notes only passes its call on to Listing, which owns the listing, search domains"
                .to_string()
        )]
    );
}

/// A method doing any work of its own, passing other arguments, or handing
/// the owner out, is no forwarding; nor is a trait's implementation.
#[test]
fn a_type_doing_work_of_its_own_forwards_nothing() {
    assert!(findings(
        r#"
        struct Listing { connection: Connection }
        struct Notes { listing: Listing }
        impl Notes {
            fn refresh(&self, id: Id) -> Result<(), Error> { self.listing.refresh(id) }
            fn count(&self) -> u64 { self.listing.count() + 1 }
        }
        struct Ids { listing: Listing }
        impl Ids {
            fn refresh(&self, id: Id) -> Result<(), Error> { self.listing.refresh(id.next()) }
            fn listing(&self) -> Listing { self.listing.clone() }
        }
        struct Shown { listing: Listing }
        impl Display for Shown {
            fn fmt(&self, f: &mut Formatter<'_>) -> Result { self.listing.fmt(f) }
        }
        impl Listing {
            fn count(&self) -> u64 { self.connection.count() }
        }
        "#,
    )
    .is_empty());
}
