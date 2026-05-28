---
layout: home

hero:
    name: coven
    text: End-to-end encrypted SQLite sync without a coordination server
    tagline: 'Multi-writer sync for host-owned SQLite schemas, encrypted blobs, and cryptographic membership over bring-your-own storage.'
    image:
        src: /favicon.svg
        alt: coven
    actions:
        - theme: brand
          text: Read the docs
          link: /docs/
        - theme: alt
          text: Example
          link: /docs/example

features:
    - title: Host-owned schema
      details: 'Your app owns its tables and domain. coven owns sync bookkeeping, change capture, encryption, membership, and storage movement.'
    - title: SQLite session changesets
      details: 'Synced rows are captured through the SQLite session extension and stamped with hybrid logical clock timestamps.'
    - title: Multi-writer by construction
      details: 'Authors sign their changesets; conflicts resolve at row level with last-writer-wins on `_updated_at`.'
    - title: Bring-your-own storage
      details: 'Sync runs through a pluggable CloudHome: S3, Google Drive, Dropbox, OneDrive, iCloud, or local storage.'
    - title: Cryptographic membership
      details: "Membership is an append-only Ed25519-signed chain. The library key is wrapped to each member's X25519 key."
    - title: Encrypted blob store
      details: 'Files referenced by rows move through a cloud outbox as encrypted opaque blobs.'
---
