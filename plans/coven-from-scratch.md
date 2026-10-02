# coven, from scratch

## 1. What coven is

- Sync for a small intimate group: a person's own devices, or a household.
- The app's data is SQLite. The app keeps its schema and writes ordinary SQL.
- Rows can carry files, like audio, images or documents, which sync with
  them.
- No server. It syncs through storage the members already have.
- Everything coven writes to the provider is encrypted with the store key.
- Only members' devices hold the store key.

## 2. Threat model

- The storage provider is assumed *honest but curious*, not *hostile*:
  - it may read what it stores;
  - it doesn't alter, withhold, fork<sup>1</sup> or roll back<sup>2</sup> what it
    stores;
  - it sees object sizes, counts, timing, and which device writes.
- A thief with storage credentials but not the store key may be hostile.
- Deletion and withholding can't be prevented. No design can.
- Current members trust each other: each has access to the storage and
  isn't hostile on it.
- An ex-member may keep a copy of the store key they had.

## 3. Guarantees

- **Availability:** the app reads and writes with no network; writes never
  wait on sync.
- **Confidentiality:** members' data can't be read by those it isn't for:
  - the provider and outsiders can't read any of it;
  - a circle's contents are readable only by its members.
- **Integrity:** members' data can't be tampered with:
  - members: no member can write as another member;
  - outsiders: nobody without the store key can alter or forge it;
  - rollback and forks are detected.
- **Authorization:** only admins change membership, and every device
  enforces it.
- **Atomicity:** another device applies a write's changes all together or
  not at all.
- **Convergence:** every device ends up with the same data:
  - a change made after seeing another is ordered after it on every device;
  - a row never shows up before the row it points to.
- **Durability:** a crash loses nothing:
  - every committed write is still uploaded;
  - every operation with several steps resumes and finishes, for example:
    - removing a member, then rotating the key;
    - writing a snapshot, then deleting the logs it covers;
    - uploading a file, then marking it stored.
- **Revocation:** an ex-member can't read anything written after they left.
- **Bounded storage:** cloud history doesn't grow forever.

## 4. Storage providers and access

- A store lives on one provider:
  - S3;
  - Google Drive;
  - Dropbox;
  - OneDrive;
  - iCloud, through CloudKit.
- Every member reaches the storage using their own provider account.
  - On S3, that means their own access key, not a shared one.
- What coven needs from a provider:
  - create an object;
  - read it, whole and by range;
  - list a prefix;
  - delete;
  - grant and revoke a member's access.

## 5. Local database

- The app declares which tables sync. The rest stay on the device.
- A *write* is one transaction that changes synced tables.
- SQLite's session extension records the row changes each write makes.
- Each write commits, together:
  - its rows, as they now stand;
  - the change itself, waiting to be uploaded:
    - which rows;
    - which columns;
    - old and new values;
  - so every committed change gets uploaded, even after a crash.
- A change record, for a write that fixes a note's title and deletes a tag:

  ```
  ana-phone, write 4, 2026-10-02 13:05:12.003
    notes  row 42  update  title: "Grocry list" → "Grocery list"
    tags   row 7   delete
  signed by ana-phone
  ```

- Reads run on several read-only connections at once.
- The app can subscribe to a query; it reruns only when rows it read change.
- Coven's own tables are out of the app's reach.

## 6. Syncing writes

- Each device uploads its writes to its own log in storage, and the other
  devices download them.
- No two devices write the same object, so devices never have to coordinate
  their uploads.
  - Each object is one write from that device: the change record from
    section 5, encrypted.
  - It is named `devices/<device>/<n>`, created once and never changed.
  - Its name is part of its encryption, so the provider can't swap one
    object for another.
  - A retried upload writes the same name with the same bytes.

## 7. Order and merge

- Every change carries a timestamp from a clock that combines the
  wall clock with a counter, so a change made after seeing another always
  sorts after it, whatever the devices' wall clocks say.
- Per column, the newest change wins.
- A delete beats an edit.
- The result doesn't depend on arrival order, so remote changes apply
  straight into the live database.
- A row whose foreign-key parent hasn't arrived waits for it.
- A unique value that names a thing is the row's identity, so two
  inserts of it are one row and merge.
- Synced tables have no other unique constraints.
- A timestamp per column, or one per row with the losing edit's columns kept
  wherever the winning edit left them unchanged?

## 8. Rollback and fork detection

- Each device remembers how far it has read every device's log.
- A gap or a step back means something was withheld.
- Devices post how far they've read, so a fork shows up when they
  compare.
- What does the app see when one is detected?

## 9. Membership and roles

- Membership is a synced table.
- Several equal admins.
- Only admins change membership.
- Removing the last admin isn't allowed.
- Every device applies these rules while going through changes in order.

## 10. Removing a member

- Revoke their storage access.
- Rotate the store key: make a new one and encrypt it to each remaining
  device's public key. Section 11 covers device keys.
- So an ex-member's copy of the old store key reads nothing written after
  they left, even if they regain read access.

## 11. Signatures

- Every device has its own key pair: a private key it never shares, and a
  public key the other members know.
- Every change is signed with the private key of the device that wrote it,
  so who wrote what is authentic.
- Devices check each change's signature against the member list in order,
  which is what makes the roles in section 9 hold.
- This is about authenticity, not trust.

## 12. Joining, restore, and not losing the store key

- An existing device pairs a new one over the local network.
- Pairing hands over the store key and storage access.
- An admin re-invites a person who lost every device.
- A one-person store has a restore code, written down by the
  person, holding the store key and storage credentials.
- Losing every device and the restore code loses the store for good, since
  everything is encrypted.
- Each device keeps the store key in the OS keychain.
- Where the keychain syncs, as iCloud Keychain does, a new device of the
  same person can open the store without pairing.
- Setup makes saving the restore code part of creating a store.
- What stands in for a synced keychain on Android, Windows and Linux?
- How does a device that got the store key from the keychain add its own
  public key to the member list, since changes are checked against it?

## 13. Circles

- A circle is a group of members inside a store who share rows the other
  members can't read.
- Each circle has its own key, encrypted to each circle member's devices'
  public keys.
- All members can see that a circle exists, who writes to it, when, and how
  much.
- A circle's changes travel in the same device logs, encrypted
  with its key.
- Devices outside a circle skip what they can't open.
- Leaving a circle rotates the circle key.
- What happens when a row references a row in another circle?

## 14. Snapshots and bounded history

- Any member writes a snapshot: the synced tables, encrypted, and
  how far into every log they reach.
- A new device loads the latest snapshot, then the logs after it.
- Log objects a snapshot covers are deleted after a while.
- Who writes snapshots, and when?
- How long do covered logs stay?

## 15. Files

- Files are what the app attaches to rows: audio, images, documents.
- Each file is stored encrypted, named by a hash of its content.
- Encrypted in fixed-size chunks, so any range can be read and
  checked on its own.
- A local cache with a size budget; the app can pin a file to keep
  it on the device regardless of the budget.
- A file waits in a local upload queue until it is stored.
- A file is deleted once nothing in the latest snapshot or the
  logs after it refers to it.
- How do files inside a circle work?
- Which device deletes?

## 16. Operations with several steps

- Upload, snapshot, deleting covered logs, and pairing each take several
  steps that a crash can interrupt.
- One shared mechanism records each operation's progress on disk
  and resumes it after a crash.
- Every step is safe to run twice.
- A failure goes to whoever started the operation.

## 17. Schema changes

- How does a synced schema change while devices run different app versions?

## 18. The API apps use

- Open, write, read, subscribed queries, files, membership, circles,
  pairing, sync status.

## Notes

1. Fork: show different devices different histories.
2. Roll back: serve an older version of what it stores.
