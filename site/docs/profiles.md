# Members and profiles

coven knows each member by a random [`MemberId`](rustdoc:struct:coven::MemberId)
and a pair of public keys, and each device by the name it registered with. It
doesn't know what to call people: "Ana" is something Ana tells the app. This
page shows the pattern for showing who is who: `members()` for who is in the
store and which of them is you, and a synced profile table that each member
writes for themselves.

## Who is in the store

`handle.members()` lists the store's active members, as this device has applied
them, and `subscribe_members()` keeps the list current as people join, leave or
change role. On Ana's phone in Household:

```
id         role     devices                                   is_self
1f0c…      Admin    Ana's phone (Active), Ana's laptop (Active)  true
8a3e…      Member   Ben's phone (Active)                         false
```

Each entry is a [`MemberInfo`](rustdoc:struct:coven::MemberInfo): the member's
`id`, their current public `keys`, their `role`, every device they registered
with its `name` and `state` (`Active`, `Removed` or `Replaced`), and `is_self`,
true for the member using this device. Removed members drop out of the list.
Before storage is set up, the list holds only you.

Device names are what each device called itself when it was set up, restored or
joined. They make a fallback label, not a person's name.

## A profile each member writes

The app keeps names, and anything else people choose to show, in a synced
table keyed by member id:

```sql
CREATE TABLE profiles (
    member_id TEXT PRIMARY KEY NOT NULL,
    display_name TEXT NOT NULL,
    color TEXT
);
```

```rust
SyncedTable::new("profiles", RowIdentity::SharedKey)
    .key_columns(["member_id"])
    .owner_only()
```

- **A shared key.** The key comes from the member id, so Ana's phone and her
  laptop, each setting her profile offline, write one row, not two, and its
  cells merge ([Keys](/docs/local-data#keys)).
- **`owner_only()`.** Only the member who created a row may change or delete it
  afterwards ([owner tables](/docs/merge#owner-tables)). Ana's phone creates
  Ana's profile, so Ana owns it, and Ben's device can't edit it.
- **In the store.** The table declares no audience, so every member reads every
  profile, including members of circles they aren't in.

Bind the member id with [`UuidText`](rustdoc:struct:coven::UuidText), which
writes it as the same canonical text on every device.

## Writing your own profile

Find yourself with `is_self`, then insert or update your own row:

```rust
use coven::{MemberId, UuidText};

async fn set_my_name(handle: &CovenHandle, name: String) -> Result<(), CovenError> {
    let Some(me) = handle.members().await?.into_iter().find(|m| m.is_self) else {
        return Ok(()); // this device's member was removed
    };
    let me = UuidText(me.id.0);
    handle
        .write(move |sql| {
            sql.execute(
                "INSERT INTO profiles (member_id, display_name) VALUES (?1, ?2)
                 ON CONFLICT (member_id) DO UPDATE SET display_name = excluded.display_name",
                (me, name),
            )?;
            Ok(())
        })
        .await
}
```

The app writes only the row whose key is `is_self`'s id. Writing another
member's row once they have created it fails on this device with
`DbError::NotOwner` and commits nothing.

## Showing who is who

`members()` is coven's own state, not a table the app's SQL can read, so the app
joins the two lists itself:

```rust
use std::collections::HashMap;
use coven::{DeviceState, MemberId, MemberInfo, UuidText};

async fn profiles(handle: &CovenHandle) -> Result<HashMap<MemberId, String>, CovenError> {
    let rows = handle
        .read(|sql| {
            Ok(sql.query("SELECT member_id, display_name FROM profiles", [], |row| {
                Ok((MemberId(row.get::<_, UuidText>(0)?.0), row.get::<_, String>(1)?))
            })?)
        })
        .await?;
    Ok(rows.into_iter().collect())
}

fn label(member: &MemberInfo, names: &HashMap<MemberId, String>) -> String {
    let name = names
        .get(&member.id)
        .cloned()
        .or_else(|| {
            member.devices.iter().find(|d| d.state == DeviceState::Active).map(|d| d.name.clone())
        })
        .unwrap_or_else(|| "New member".to_owned());
    if member.is_self { format!("{name} (you)") } else { name }
}
```

For a list that stays current, use `subscribe_members()` and
`handle.subscribe(..)` with the same query, and relabel whenever either
delivers: the first changes when someone joins, leaves or changes role; the
second when someone renames themselves.

Only members `members()` lists are shown. A removed member's profile row stays
in the table, since only its owner could delete it, but nothing shows it.

## Ana and Ben

1. Ana creates Household on her phone and sets her name. Her phone writes the
   row `(1f0c…, "Ana")`, and Ana owns it.
2. Ana invites Ben. The invite fixes Ben's member id, `8a3e…`
   (`invite.member`). Ben's phone joins as "Ben's phone".
3. Before Ben's profile reaches Ana, her list shows "Ana (you)" and "Ben's
   phone", from his device's name.
4. Ben sets his name on his phone: the row `(8a3e…, "Ben")`. Once it syncs,
   Ana's list shows "Ana (you)" and "Ben".
5. Offline, Ben renames himself "Benjamin" on his laptop while his phone still
   says "Ben B." from an earlier edit. Both writes set the same cell; the one
   with the larger timestamp wins on every device, and the other is a lost value
   ([Merge](/docs/merge#lost-values)). For a profile, the app can dismiss it
   without asking.
6. A bug in Ben's app tries to set Ana's `display_name`. Ben's device refuses the
   write with `DbError::NotOwner`; nothing is sent.
7. Ana removes Ben. He drops out of `members()`, so her list shows only "Ana
   (you)". His profile row stays in the table, unseen.

## What coven doesn't check

coven records who created each row and enforces `owner_only()` against it, but
it doesn't check that a profile's key is its creator's own member id, and its
API doesn't expose a row's creator. A member's app that created a profile row
under someone else's id before they did would own it, and the real member's
later write would fail with `DbError::NotOwner`. Current members are trusted not
to do that ([Threat model](/docs/threat-model#a-current-member)); the pattern
relies on each app writing only its own member's row.
