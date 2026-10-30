# Sharing

A store is shared by several people, each with several devices. Someone must be
allowed to add Dan, and nobody may remove the last admin. There is no server to
ask, so every device reaches the same answer on its own, from the store's
sequence. This page covers members, roles and devices, how a person's new
device gets in, how a new person gets in, and removing people.

## Members, roles and devices

A *member* is a person. Each member has a random, permanent
[`MemberId`](rustdoc:struct:coven::MemberId) and two key pairs: one to sign with
and one that others seal keys to. A *device* is one installation of the app,
belonging to one member, with its own id. Every slot is signed with its
author's member key, and every device checks that signature against the
membership as it stood just before that slot.

`handle.members()` (live: `subscribe_members()`) lists the active members as this
device has applied them:

```rust
pub struct MemberInfo {
    pub id: MemberId,
    pub keys: MemberPublicKeys,
    pub role: MemberRole,          // Admin or Member
    pub devices: Vec<DeviceInfo>,  // id, name, and Active, Removed or Replaced
    pub is_self: bool,             // the member using this device
}
```

coven knows members by id and devices by name; what to call each person is up
to the app. [Members and profiles](/docs/profiles) shows the pattern.

| Action | Who may do it |
| --- | --- |
| Create the store | the first device, which becomes the first admin |
| Add or remove a member, change a role | an admin |
| Add a device | the member it belongs to, from that new device |
| Remove a device | its member, or an admin |
| Replace a member's keys | that member, or an admin |
| Create a circle | any member, who becomes its first member |
| Rename a circle, add or remove its members, delete it | its members only; an admin outside it can't |

A store may have several admins and always keeps at least one: removing or
demoting the last admin is refused. The member whose provider account holds the
store, the Dropbox folder's owner say, can't be removed either, because nobody
else could take provider access back; that fails with `SyncError::StoreOwner`.

## How admin changes are made

Every change to membership or settings is an *admin entry* in a slot, and every
admin call follows one path, an *operation*:

1. **Catch up.** Read and apply the sequence to its end, and check the change is
   allowed. Admin calls need storage; they can't run offline.
2. **Prepare.** Record each piece of preparation in the operation's journal
   before doing it, such as a new key and its copies, or a provider share.
3. **Claim** the next slot with the entry.
4. **If someone else claimed it first,** apply their slot and check again. If
   the change is still allowed, claim the next slot. If not, undo what the
   journal recorded and fail with `SyncError::Rejected(OperationRejection)`.

Ana and Ben, both admins, each try to remove the other. Ana's removal claims
slot 41 first. Ben's device reads it, finds Ben no longer an admin, undoes its
work, and his call fails with `Rejected(NotAllowed)`. No removal of Ana ever
enters history.

A claimed entry is final. An operation survives a crash or a dropped call: it
settles its claim, then finishes or undoes. Waiting operations appear in
`pending()` with their operation id, and `retry_pending_operation(id)` makes a
retry due now.

## A person's new device: the restore code

A *restore code* holds the member's id and current private keys, the store's id
and the storage location. It carries no store name or OAuth token. Setup returns
it, and `handle.restore_code()` shows it again, with no S3 key in it. Its text
form starts with `CVR1-`.

A new device gets the code by scanning a QR code on one of the person's devices,
from iCloud Keychain on Apple platforms, from Google's Block Store on a new
Android phone set up from the old one, or by typing it. Coven saves the code
there after setup, joining and key replacement. Storage access is
separate: on S3 the person enters their access key, which they keep apart, in a
password manager say. The QR code their own device shows comes from
`handle.restore_code_for_transfer()`, which holds the key that device uses, and
the saved entry keeps the key beside the code; either spares them typing it. On
Dropbox and OneDrive the new device signs in to the person's own account.

On Apple platforms and Android, `saved_restore_codes().await?` lists the stores
coven saved,
with their names, ids and provider locations, and whether each has an S3 key
beside its code. The name is the last one any of the person's devices saw: a
label for the list, which a rename made while none of them had the store open
leaves behind. Block Store doesn't sync: each Android phone keeps its own
entries, a rename or a forget reaches only that phone's, and a phone set up
from one whose code is stale lists a code that fails at restore. The app passes
the chosen entry to `restore_from_saved_code`, which
finds the store by its id. `forget_saved_restore_code(store)` removes an entry
the person no longer wants offered. An entry coven can't read is listed as
unreadable, with its store id and why, so the person can forget that one too.

Ana's new laptop scans the code from her phone:

```rust
let info = decode_code_info(&scanned)?;
let metadata = info.metadata();
let mut builder = Coven::builder(layout.clone())
    .synced_tables(tables())
    .migrations(migrations())
    .oauth_clients(oauth_clients.clone());
if let Some(access) = metadata.oauth_access {
    builder.authenticate(access).await?;
}
let s3_credentials = if metadata.cloud_provider == CloudProvider::S3 {
    Some(ask_for_s3_access_key().await?)
} else {
    None
};
let handle = restore_from_code(builder, &scanned, s3_credentials, "Ana's laptop", |step| show_step(step), &cancel_rx)
    .await?;
```

The laptop opens the key copies sealed to Ana, loads the store, checks that the
code's keys are Ana's current ones, and registers itself as a new device. An
old code, whose keys have since been replaced, fails with
`SyncError::MemberKeysReplaced`. Restoring always makes a fresh device; it
doesn't rescue an old installation's unsent writes. `restore_from_saved_code`
does the same from the saved entry.

Anyone who sees the code can open the store, given storage access, for as long
as its keys are current: to coven, they *are* that member. Apps should blur the
QR code until the person taps to show it. Coven offers no recovery from a
stolen code; the household removes that member and starts over.

## Adding a person: invites

Ana adds Dan to Household, on Dropbox, with Dan beside her:

1. Ana enters Dan's name and Dropbox email and chooses to show the invite in
   person. Her phone shares the store's folder with Dan's account and shows an
   *invite*: a QR code holding the store's location, Dan's name, a member id
   chosen for Dan, and a one-time secret.
2. Dan's phone scans it, signs in to his own Dropbox, makes Dan's member keys,
   and writes a *join request* to storage, encrypted with a key from the invite
   secret.
3. Ana's phone, the only device holding that secret, reads the request, checks
   it and approves it on its own.
4. Ana's phone sends Dan copies of every store key, sealed to his new keys,
   then claims the entry adding him.
5. Dan's phone opens its keys, loads the store and registers itself.

The invite only lets Dan ask; Ana's phone lets him in. An invite has one request,
so the phone Ana watched scan the code is the one let in.

When Dan isn't beside her, Ana sends the invite by message or email instead.
Someone else who sees it could use it first, so her phone waits for her to
approve. Dan's phone shows a six-digit *confirmation number* computed from his
new keys, and Ana's phone shows the number of the request it read. Ana asks Dan
for his number over a call or a thread she knows is his, and approves only if
they match:

```rust
// Ana's phone
let invite = handle
    .create_invite(
        InviteTarget::NewMember { role: MemberRole::Member, name: "Dan".into() },
        InviteAccess::ProviderAccount { email: "dan@example.com".into() },
        InviteDelivery::Sent { lifetime: InviteLifetime::Day },
    )
    .await?;
send_to_dan(&invite.code);

let mut invites = handle.subscribe_invites();
loop {
    let open = invites.borrow_and_update().clone();
    if let Some(request) = open.iter().find(|i| i.id == invite.id).and_then(|i| i.request.clone()) {
        // Ana asks Dan for the number his phone shows.
        if confirm(&request.confirmation).await {
            handle.approve_join_request(&request).await?;
        } else {
            handle.decline_join_request(&request).await?;
        }
        break;
    }
    invites.changed().await?;
}
```

```rust
// Dan's phone
let mut builder = Coven::builder(layout.clone())
    .synced_tables(tables())
    .migrations(migrations())
    .oauth_clients(oauth_clients.clone());
builder.authenticate(OAuthAccess::Dropbox(DropboxAccess::FullDropbox)).await?;
// The waiting screen shows Dan's number and when the next check runs; its
// Check now button calls control.check_now().
let control = JoinControl::new();
match join_with_invite(builder, &scanned, "Dan's phone", |step| show_step(step), &control).await? {
    Some(handle) => show_store(handle),
    None => show_declined(),
}
```

On Dropbox, OneDrive and iCloud, only the account that holds the store can share
it, so the inviting admin must be that member. On S3 there is no sharing:
`InviteAccess::S3AccessKey` hands over a key made by hand in the provider's
console ([below](#s3-access-keys)).

An invite shown in person expires an hour after it was made; one sent by
message or email expires after the hour, day or week the admin chose.
Declining, letting it expire, or `cancel_invite` takes back the provider access
it granted. Dan's phone keeps its new keys and request across restarts, and
picks up where it left off; `join_with_invite` returns `None` when the request
is declined, the invite is cancelled, or it expires.

While a join is under way, Dan's phone also keeps the invite code in its own
keychain, never synced, and beside it his Dropbox sign-in. If Dan closes the app
while he waits, the next start finds the join and resumes it without asking him
for the code or a sign-in. The kept sign-in refreshes in the keychain, so a
token Dropbox rejected while he waited is never used again. Only if Dropbox
refuses the kept sign-in for good does resuming fail with
`OAuthError::Reauthorize`; Dan then signs in with the access the join names, and
it resumes:

```rust
// Each call consumes its builder, so each gets one configured as above.
let new_builder = || {
    Coven::builder(layout.clone())
        .synced_tables(tables())
        .migrations(migrations())
        .oauth_clients(oauth_clients.clone())
};
if let Some(join) = new_builder().unfinished_joins().await?.into_iter().next() {
    // join.store_name and join.invitee come from the invite.
    let store = join.metadata.store_id;
    let resumed = match new_builder().resume_join(store, |step| show_step(step), &control).await {
        Err(BootstrapError::OAuth(OAuthError::Reauthorize(_))) => {
            let mut builder = new_builder();
            builder.authenticate(join.metadata.oauth_access.expect("a sign-in join")).await?;
            builder.resume_join(store, |step| show_step(step), &control).await
        }
        resumed => resumed,
    };
    match resumed? {
        Some(handle) => show_store(handle),
        None => show_declined(),
    }
}
```

Once Dan's phone may have sent its request, the join can't be cancelled: Ana's
phone may already be approving it. `control.cancel()` then ends the call with
`BootstrapError::JoinRequestSent` and keeps the join for later, as closing the
app does. Before the request is sent, cancelling removes everything.

## Losing everything

Ben loses every device and his restore code. An admin invites him again with
`InviteTarget::ReplaceMemberKeys { member }`, naming his existing member id.
Approving it replaces his keys: his role, circles and existing device ids stay
attached to his id, writes signed with his old keys that land afterwards are
refused, and his old keys count as exposed, so keys copied to them are replaced
([Encryption](/docs/encryption#which-key-new-data-uses)).

If every member loses every device and restore code, the store is lost:
everything is encrypted.

## Replacing a member's keys

A member who suspects a leak replaces their own keys with
`handle.replace_member_keys()`, which returns a new restore code. While it runs,
app writes are refused with `DbError::MemberKeysChanging`; it first sends the
writes already queued, while the old keys still count. Other devices still
holding the old keys stop sending and report `MemberKeysReplaced`; they restore
from the new code. The member's id, role, circles and devices stay the same.
An admin can publish a prepared pair for another member with
`replace_member_keys_for`.

## Removing people and devices

`handle.remove_member(&member)` publishes one entry removing the member, their
devices and their circle memberships; a circle left empty is deleted at that
slot. `handle.remove_device(device)` removes one device and says how its member
cuts that device off at the provider. `set_member_role` changes a role.

A removed person still has the keys and data they had. Two things stop them
doing more: the sequence stops counting what they send, and the provider stops
letting them reach storage.

### The cut-off

A removal's slot is a line in history. Writes from the removed device or member
that land in later slots don't count; writes that landed before keep counting.
The same line falls at a member-key replacement: later writes signed with the
old keys don't count.

Ana's old phone sends write 8 while her new device claims slot 70, registering
itself in the old phone's place. If write 8 lands at slot 69, it counts. At slot
71, it doesn't. Every device consumes that slot, makes no change to any row,
records no lost value, and reports `WritesRefused` naming the entry that decided
it. There is no grace period.

A device that applies its own removal, or its member's, stops syncing and tells
the app.

### Provider access

Removal in the sequence stops Ben's writes from counting. Taking away his
storage access stops him reading anything new. That second step is provider
work, recorded when the removal applies. `remove_member` returns once its entry
applies; the provider work shows in `pending()`.

On Dropbox, OneDrive and iCloud, only the account that holds the store can
change sharing, so that member's device unshares when it applies the removal.
Ben removes Dan while Ana, the folder's owner, is offline: Dan's removal applies
everywhere at once, Ben's pending list names the wait for Ana, and Ana's next
pass does the unsharing. The owner's device takes back only a grant to that one
account, keeps an account shared while another member or open invite uses it,
and reports grants it can't remove alone, such as a parent folder's share or a
public link, as `PendingReason::AccessRemains`.

These providers can't cut off one device of an account. To lock out a lost
phone, the person does this; [`ProviderSignOut`](rustdoc:enum:coven::ProviderSignOut)
tells the app the step for Dropbox, OneDrive and iCloud, and on S3 there is none
to return, because the phone shares its member's key:

| Provider | What the person does |
| --- | --- |
| Dropbox, OneDrive | remove the app's access from their account, then sign in again on the devices they keep |
| iCloud | remove the lost device from their Apple account |
| S3 | the member makes a new key in the provider's console, replaces their key with it, and deletes the old one there |

Removal changes no ciphertext or key anyone already has; it stops what comes
next. Keys the removed member had seen are replaced before new data uses them
([Encryption](/docs/encryption#which-key-new-data-uses)).

### S3 access keys

On S3 each member has their own access key. The person makes every key in the
provider's console and deletes it there; coven never holds the storage
account's admin access. `guided_access_key()` fills in what the console needs:
the bucket, the store's prefix and the member policy.

```rust
handle.replace_access_key(new_key_id, new_secret).await?;
```

To replace their key, a member makes a new one in the console and gives it to a
device they keep, which installs it and records it. The restore code doesn't
change. Their other devices take the new key with `update_credentials`.

When a key stops being used, because it was replaced, its member was removed, or
its invite ended, coven forgets it and keeps no notice. The app should name the
key then, so the person deletes it in the console; until they do, it still
reaches the store.
