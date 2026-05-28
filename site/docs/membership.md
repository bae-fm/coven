# Membership

Membership is represented as an append-only signed chain.

## Identity

Members are identified by public keys. Changesets are signed per author so a
receiver can verify who wrote each envelope.

## Library key

The library's symmetric encryption key is wrapped to each member's X25519 key.
Adding a member means appending a signed membership operation and making the
library key available to that member.

## Roles

[`sync::membership::MemberRole`](rustdoc:enum:coven::sync::membership::MemberRole)
describes member permissions. The high-level manager exposes
[`get_members`](rustdoc:method:coven::sync::sync_manager::SyncManager::get_members)
and
[`invite_member`](rustdoc:method:coven::sync::sync_manager::SyncManager::invite_member)
for host UI flows.

## Restore

Restore codes let a configured library be recovered through the cloud provider
and key service.
[`SyncManager::generate_restore_code`](rustdoc:method:coven::sync::sync_manager::SyncManager::generate_restore_code)
delegates to the provider setup layer.
