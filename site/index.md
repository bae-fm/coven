---
layout: home

hero:
    text: 'Local-first apps that scale to the cloud with <span style="white-space: nowrap">nothing to run</span>'
    image:
        src: /favicon.svg
        alt: coven
    actions:
        - theme: brand
          text: Get started
          link: /docs/

features:
    - icon:
          light: /icons/harddrive-light.svg
          dark: /icons/harddrive-dark.svg
          width: 28
          height: 28
      title: Local first
      details: 'Every device has the full database. Writes commit locally, sync follows in the background.'
      link: /docs/
    - icon:
          light: /icons/cloud-light.svg
          dark: /icons/cloud-dark.svg
          width: 28
          height: 28
      title: Serverless
      details: 'Devices sync through cloud storage users bring. Nothing to deploy or operate.'
      link: /docs/storage
    - icon:
          light: /icons/devices-light.svg
          dark: /icons/devices-dark.svg
          width: 28
          height: 28
      title: Everyone writes
      details: 'Any device edits anything, offline included, and concurrent edits merge on their own.'
      link: /docs/merge
    - icon:
          light: /icons/image-light.svg
          dark: /icons/image-dark.svg
          width: 28
          height: 28
      title: Blobs, too
      details: 'Rows carry files. A file commits in the row''s transaction and syncs alongside it.'
      link: /docs/blobs
    - icon:
          light: /icons/layers-light.svg
          dark: /icons/layers-dark.svg
          width: 28
          height: 28
      title: Beyond the disk
      details: 'Files can live in the cloud and stream back when read. Pin what should stay offline.'
      link: /docs/cache
    - icon:
          light: /icons/database-light.svg
          dark: /icons/database-dark.svg
          width: 28
          height: 28
      title: Pick what syncs
      details: 'SQLite in a sync harness: specify which rows should sync, relations follow automatically.'
      link: /docs/local-data
---

<div class="home-body">

<div class="byos">

<p class="kicker">Bring your own storage</p>

<p class="providers">
  <span>Google Drive</span>
  <span>Dropbox</span>
  <span>OneDrive</span>
  <span>iCloud (CloudKit)</span>
  <span>S3-compatible</span>
</p>

[Learn more →](/docs/storage)

</div>

<div class="story-head" id="how-it-works">
<p class="kicker">How it works</p>
<p class="story-title">The life of a store</p>
</div>

<svg width="0" height="0" style="position:absolute" aria-hidden="true"><defs><marker id="fa" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="amf"/></marker><marker id="fam" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="ammf"/></marker></defs></svg>

<div class="flowrow">
<div class="flowtext">

### Local first

The phone inserts a row. It commits on-device; nothing waits on a network.

```rust
handle.sql(|sql| {
    sql.tx().execute("INSERT …", params)?;
    Ok(())
}).await?;
```

</div>
<svg class="flow" viewBox="0 0 660 116" role="img" aria-label="A row exists on the phone only; the cloud and laptop are empty">
<text class="hdr" x="100" y="22" text-anchor="middle">PHONE</text>
<text class="hdr" x="330" y="22" text-anchor="middle">CLOUD</text>
<text class="hdr" x="560" y="22" text-anchor="middle">LAPTOP</text>
<rect class="lane" x="10" y="32" width="180" height="72" rx="10"/>
<rect class="lanec" x="240" y="32" width="180" height="72" rx="10"/>
<rect class="lane" x="470" y="32" width="180" height="72" rx="10"/>
<rect class="chip" x="25" y="52" width="150" height="28" rx="7"/>
<text class="lbl" x="100" y="70" text-anchor="middle">note · “milk run”</text>
<text class="sub" x="330" y="72" text-anchor="middle">(no cloud configured)</text>
<text class="sub" x="560" y="72" text-anchor="middle">(nothing yet)</text>
</svg>
</div>

<div class="flowlink">
<svg viewBox="0 0 14 34" width="14" height="34" aria-hidden="true"><line x1="7" y1="0" x2="7" y2="31" marker-end="url(#fa)"/></svg>
<span>Configure cloud</span>
</div>

<div class="flowrow">
<div class="flowtext">

### Serverless sync

The cloud becomes the sync medium. Devices push sealed, signed objects and
pull each other's; it only ever holds ciphertext.

```rust
handle.connect_sync(Some(key)).await?;
```

</div>
<svg class="flow" viewBox="0 0 660 120" role="img" aria-label="The phone pushes a sealed object to the cloud; the laptop pulls it">
<text class="hdr" x="100" y="22" text-anchor="middle">PHONE</text>
<text class="hdr" x="330" y="22" text-anchor="middle">CLOUD</text>
<text class="hdr" x="560" y="22" text-anchor="middle">LAPTOP</text>
<rect class="lane" x="10" y="32" width="180" height="76" rx="10"/>
<rect class="lanec" x="240" y="32" width="180" height="76" rx="10"/>
<rect class="lane" x="470" y="32" width="180" height="76" rx="10"/>
<rect class="chip" x="25" y="52" width="150" height="28" rx="7"/>
<text class="lbl" x="100" y="70" text-anchor="middle">note · “milk run”</text>
<line class="arr" x1="182" y1="66" x2="248" y2="66" marker-end="url(#fa)"/>
<text class="sub" x="215" y="94" text-anchor="middle">push · sealed</text>
<rect class="chipo" x="255" y="52" width="150" height="28" rx="7"/>
<path class="glyph" d="M271 63v-3a3 3 0 0 1 6 0v3"/>
<rect class="glyphf" x="269.5" y="63" width="9" height="7" rx="1.5"/>
<text class="lbl s11" x="290" y="70">signed object</text>
<line class="arr" x1="428" y1="66" x2="466" y2="66" marker-end="url(#fa)"/>
<text class="sub" x="447" y="94" text-anchor="middle">pull</text>
<rect class="chip" x="485" y="52" width="150" height="28" rx="7"/>
<text class="lbl" x="560" y="70" text-anchor="middle">note · “milk run”</text>
</svg>
</div>

<div class="flowlink">
<svg viewBox="0 0 14 34" width="14" height="34" aria-hidden="true"><line x1="7" y1="0" x2="7" y2="31" marker-end="url(#fa)"/></svg>
<span>Go offline</span>
</div>

<div class="flowrow">
<div class="flowtext">

### Everyone writes

Apart, both devices edit the same row. The next sync applies both: merge is
column by column.

```sql
UPDATE notes SET body = 'oat, 2%' …
```

</div>
<svg class="flow" viewBox="0 0 660 210" role="img" aria-label="Phone and laptop edit different columns of the same row; both edits merge on both devices">
<text class="hdr" x="100" y="22" text-anchor="middle">PHONE</text>
<text class="hdr" x="330" y="22" text-anchor="middle">CLOUD</text>
<text class="hdr" x="560" y="22" text-anchor="middle">LAPTOP</text>
<rect class="lane" x="10" y="32" width="180" height="166" rx="10"/>
<rect class="lanec" x="240" y="32" width="180" height="166" rx="10"/>
<rect class="lane" x="470" y="32" width="180" height="166" rx="10"/>
<rect class="chip" x="25" y="46" width="150" height="28" rx="7"/>
<text class="lbl s11" x="100" y="64" text-anchor="middle">title → “Milk run”</text>
<rect class="chip" x="485" y="46" width="150" height="28" rx="7"/>
<text class="lbl s11" x="560" y="64" text-anchor="middle">body → “oat, 2%”</text>
<line class="arr" x1="182" y1="60" x2="248" y2="60" marker-end="url(#fa)"/>
<line class="arr" x1="478" y1="60" x2="412" y2="60" marker-end="url(#fa)"/>
<rect class="chipo" x="255" y="38" width="150" height="24" rx="6"/>
<text class="lbl s11" x="330" y="54" text-anchor="middle">Δ title — phone</text>
<rect class="chipo" x="255" y="70" width="150" height="24" rx="6"/>
<text class="lbl s11" x="330" y="86" text-anchor="middle">Δ body — laptop</text>
<line class="arrd" x1="100" y1="80" x2="100" y2="140" marker-end="url(#fam)"/>
<line class="arrd" x1="560" y1="80" x2="560" y2="140" marker-end="url(#fam)"/>
<text class="sub" x="110" y="114">merge on pull</text>
<rect class="chipa" x="25" y="146" width="150" height="40" rx="8"/>
<text class="lbl" x="100" y="162" text-anchor="middle">Milk run</text>
<text class="sub" x="100" y="177" text-anchor="middle">oat milk, 2%</text>
<rect class="chipa" x="485" y="146" width="150" height="40" rx="8"/>
<text class="lbl" x="560" y="162" text-anchor="middle">Milk run</text>
<text class="sub" x="560" y="177" text-anchor="middle">oat milk, 2%</text>
</svg>
</div>

<div class="flowlink">
<svg viewBox="0 0 14 34" width="14" height="34" aria-hidden="true"><line x1="7" y1="0" x2="7" y2="31" marker-end="url(#fa)"/></svg>
<span>Attach a photo</span>
</div>

<div class="flowrow">
<div class="flowtext">

### Blobs, too

A photo commits in the row's transaction. The laptop takes the row now and
streams the photo on first view.

```rust
handle.write(
    |b| { b.put_blob(ns, id, bytes); Ok(()) },
    |sql| { /* INSERT the row */ Ok(()) },
).await?;
```

</div>
<svg class="flow" viewBox="0 0 660 150" role="img" aria-label="A row and its photo commit as one transaction and sync; the photo streams to the laptop on read">
<text class="hdr" x="100" y="22" text-anchor="middle">PHONE</text>
<text class="hdr" x="330" y="22" text-anchor="middle">CLOUD</text>
<text class="hdr" x="560" y="22" text-anchor="middle">LAPTOP</text>
<rect class="lane" x="10" y="32" width="180" height="106" rx="10"/>
<rect class="lanec" x="240" y="32" width="180" height="106" rx="10"/>
<rect class="lane" x="470" y="32" width="180" height="106" rx="10"/>
<rect class="tx" x="20" y="42" width="160" height="74" rx="9"/>
<rect class="chip" x="30" y="50" width="140" height="24" rx="6"/>
<text class="lbl s11" x="100" y="66" text-anchor="middle">photos row</text>
<rect class="chip" x="30" y="84" width="140" height="24" rx="6"/>
<text class="lbl s11" x="100" y="100" text-anchor="middle">IMG_204.jpg</text>
<text class="sub" x="100" y="132" text-anchor="middle">one transaction</text>
<line class="arr" x1="184" y1="80" x2="248" y2="80" marker-end="url(#fa)"/>
<rect class="chipo" x="255" y="50" width="150" height="24" rx="6"/>
<text class="lbl s11" x="330" y="66" text-anchor="middle">row object</text>
<rect class="chipo" x="255" y="84" width="150" height="24" rx="6"/>
<path class="glyph" d="M271 94v-3a3 3 0 0 1 6 0v3"/>
<rect class="glyphf" x="269.5" y="94" width="9" height="7" rx="1.5"/>
<text class="lbl s11" x="290" y="100">blob object</text>
<line class="arr" x1="428" y1="62" x2="466" y2="62" marker-end="url(#fa)"/>
<line class="arrd" x1="428" y1="96" x2="466" y2="96" marker-end="url(#fam)"/>
<rect class="chip" x="485" y="50" width="150" height="24" rx="6"/>
<text class="lbl s11" x="560" y="66" text-anchor="middle">photos row</text>
<rect class="chipd" x="485" y="84" width="150" height="24" rx="6"/>
<text class="lbl s11" x="560" y="100" text-anchor="middle">IMG_204.jpg</text>
<text class="sub" x="560" y="132" text-anchor="middle">streams on first view</text>
</svg>
</div>

<div class="flowlink">
<svg viewBox="0 0 14 34" width="14" height="34" aria-hidden="true"><line x1="7" y1="0" x2="7" y2="31" marker-end="url(#fa)"/></svg>
<span>Offload the original</span>
</div>

<div class="flowrow">
<div class="flowtext">

### Beyond the disk

The original moves to the cloud. Devices keep cache copies; a pin keeps one
offline.

```rust
handle.make_remote("photos", id, false).await?;
```

</div>
<svg class="flow" viewBox="0 0 660 150" role="img" aria-label="A large original moves to the cloud; devices keep cache copies and a pin">
<text class="hdr" x="100" y="22" text-anchor="middle">PHONE</text>
<text class="hdr" x="330" y="22" text-anchor="middle">CLOUD</text>
<text class="hdr" x="560" y="22" text-anchor="middle">LAPTOP</text>
<rect class="lane" x="10" y="32" width="180" height="106" rx="10"/>
<rect class="lanec" x="240" y="32" width="180" height="106" rx="10"/>
<rect class="lane" x="470" y="32" width="180" height="106" rx="10"/>
<rect class="chip" x="485" y="48" width="150" height="24" rx="6"/>
<text class="lbl s11" x="560" y="64" text-anchor="middle">RAW original · 48 MB</text>
<line class="arr" x1="478" y1="60" x2="412" y2="60" marker-end="url(#fa)"/>
<text class="sub" x="447" y="44" text-anchor="middle">offload</text>
<rect class="chipo" x="255" y="48" width="150" height="24" rx="6"/>
<path class="glyph" d="M271 58v-3a3 3 0 0 1 6 0v3"/>
<rect class="glyphf" x="269.5" y="58" width="9" height="7" rx="1.5"/>
<text class="lbl s11" x="290" y="64">blob · 48 MB</text>
<rect class="chipd" x="485" y="96" width="150" height="24" rx="6"/>
<text class="lbl s11" x="560" y="112" text-anchor="middle">cache · streams on read</text>
<rect class="chipd" x="25" y="48" width="150" height="24" rx="6"/>
<text class="lbl s11" x="100" y="64" text-anchor="middle">cache copy</text>
<rect class="chip" x="25" y="96" width="150" height="24" rx="6"/>
<circle class="glyphf" cx="40" cy="106" r="3"/>
<line class="glyph" x1="40" y1="109" x2="40" y2="114"/>
<text class="lbl s11" x="50" y="112">pinned copy</text>
</svg>
</div>

<div class="flowlink">
<svg viewBox="0 0 14 34" width="14" height="34" aria-hidden="true"><line x1="7" y1="0" x2="7" y2="31" marker-end="url(#fa)"/></svg>
<span>Flip shared</span>
</div>

<div class="flowrow">
<div class="flowtext">

### Pick what syncs

One flag shares a subtree. Flipping it on cascades the project's rows and
files to peers; flipping it off retracts them from peers and keeps them
local.

```sql
UPDATE projects SET shared = 1 …
```

</div>
<svg class="flow" viewBox="0 0 660 290" role="img" aria-label="Flipping a shared flag cascades a project subtree to peers; flipping it off retracts it from peers while it stays local">
<text class="hdr" x="100" y="22" text-anchor="middle">PHONE</text>
<text class="hdr" x="330" y="22" text-anchor="middle">CLOUD</text>
<text class="hdr" x="560" y="22" text-anchor="middle">LAPTOP</text>
<rect class="lane" x="10" y="32" width="180" height="246" rx="10"/>
<rect class="lanec" x="240" y="32" width="180" height="246" rx="10"/>
<rect class="lane" x="470" y="32" width="180" height="246" rx="10"/>
<rect class="chipa" x="25" y="44" width="150" height="26" rx="7"/>
<text class="lbl s11" x="100" y="61" text-anchor="middle">Zine — shared ✓</text>
<path class="tree" d="M36 70v22h9M36 92v32h9"/>
<rect class="chip" x="47" y="80" width="128" height="24" rx="6"/>
<text class="lbl s11" x="111" y="96" text-anchor="middle">task row</text>
<rect class="chip" x="47" y="112" width="128" height="24" rx="6"/>
<text class="lbl s11" x="111" y="128" text-anchor="middle">photo + file</text>
<line class="arr" x1="182" y1="88" x2="248" y2="88" marker-end="url(#fa)"/>
<rect class="chipo" x="255" y="44" width="150" height="24" rx="6"/>
<text class="lbl s11" x="330" y="60" text-anchor="middle">Δ Zine</text>
<rect class="chipo" x="255" y="76" width="150" height="24" rx="6"/>
<text class="lbl s11" x="330" y="92" text-anchor="middle">Δ task</text>
<rect class="chipo" x="255" y="108" width="150" height="24" rx="6"/>
<text class="lbl s11" x="330" y="124" text-anchor="middle">Δ photo + blob</text>
<line class="arr" x1="428" y1="88" x2="466" y2="88" marker-end="url(#fa)"/>
<rect class="chip" x="485" y="44" width="150" height="26" rx="7"/>
<text class="lbl s11" x="560" y="61" text-anchor="middle">Zine</text>
<rect class="chip" x="485" y="80" width="150" height="24" rx="6"/>
<text class="lbl s11" x="560" y="96" text-anchor="middle">task row</text>
<rect class="chip" x="485" y="112" width="150" height="24" rx="6"/>
<text class="lbl s11" x="560" y="128" text-anchor="middle">photo + file</text>
<text class="sub" x="330" y="156" text-anchor="middle">later, shared flips off</text>
<line class="arrd" x1="20" y1="164" x2="640" y2="164"/>
<rect class="chip" x="25" y="176" width="150" height="26" rx="7"/>
<text class="lbl s11" x="100" y="193" text-anchor="middle">Zine — shared ✗</text>
<rect class="chip" x="47" y="210" width="128" height="24" rx="6"/>
<text class="lbl s11" x="111" y="226" text-anchor="middle">task row</text>
<rect class="chip" x="47" y="240" width="128" height="24" rx="6"/>
<text class="lbl s11" x="111" y="256" text-anchor="middle">photo + file</text>
<text class="sub" x="330" y="205" text-anchor="middle">deletes emitted</text>
<rect class="chipd ghost" x="485" y="176" width="150" height="26" rx="7"/>
<rect class="chipd ghost" x="485" y="206" width="150" height="24" rx="6"/>
<rect class="chipd ghost" x="485" y="234" width="150" height="24" rx="6"/>
<text class="sub" x="560" y="271" text-anchor="middle">retracted</text>
</svg>
</div>

<div class="code-exit">

[See complete example →](/docs/example)

</div>

<div class="end-cta">
<a href="/docs/">Read the docs</a>
</div>

</div>

<style>
:root {
    --vp-home-hero-name-color: transparent;
    --vp-home-hero-name-background: linear-gradient(120deg, #3f8f8b 30%, #345f5d);
    --vp-home-hero-image-background-image: linear-gradient(-45deg, #3f8f8b66 50%, #345f5d66 50%);
    --vp-home-hero-image-filter: blur(56px);
}

.dark {
    --vp-home-hero-name-background: linear-gradient(120deg, #8fd9d4 30%, #4a9a95);
}

/* Smaller eyebrow, bigger jumbo (VitePress sizes both at 32/48/56px
   across its breakpoints). */
.VPHero .name {
    font-size: 26px;
    line-height: 34px;
}

.VPHero .text {
    font-size: 40px;
    line-height: 48px;
}

.VPHero .tagline {
    font-size: 16px;
    line-height: 24px;
}

@media (min-width: 640px) {
    .VPHero .name {
        font-size: 30px;
        line-height: 38px;
    }

    .VPHero .text {
        font-size: 56px;
        line-height: 64px;
    }

    .VPHero .tagline {
        font-size: 18px;
        line-height: 26px;
    }
}

@media (min-width: 960px) {
    .VPHero .name {
        font-size: 34px;
        line-height: 42px;
    }

    .VPHero .text {
        font-size: 64px;
        line-height: 72px;
    }

    .VPHero .tagline {
        font-size: 20px;
        line-height: 28px;
    }
}

/* Wider gap between the feature cards (VitePress default is 16px:
   8px item padding against a -8px items margin). */
.VPFeatures .items {
    margin: -11px;
}

.VPFeatures .item {
    padding: 11px;
}

/* VPHomeContent provides the outer container and side padding; the
   hero/features .container is 1152px of content inside its own. */
.home-body {
    max-width: 1152px;
    margin: 0 auto;
    padding: 16px 0 96px;
}

/* The homepage lays diagrams out full-bleed inside flow cards. */
.home-body .flow {
    max-width: none;
    margin: 0;
}

.home-body .flowrow {
    display: flex;
    align-items: center;
    gap: 40px;
    margin: 22px 0;
    padding: 28px 32px;
    border: 1px solid var(--vp-c-divider);
    border-radius: 12px;
    background: linear-gradient(
        135deg,
        color-mix(in srgb, var(--coven-a) 3%, var(--vp-c-bg)),
        var(--vp-c-bg) 70%
    );
}

.home-body .byos {
    text-align: center;
    padding: 48px 0 8px;
    margin-top: 48px;
    border-top: 1px solid var(--vp-c-divider);
}

.home-body .byos .kicker {
    margin-bottom: 22px;
}

.home-body .story-head {
    margin: 64px 0 12px;
    text-align: center;
}

.home-body .story-title {
    margin: 0;
    font-size: 30px;
    font-weight: 600;
    letter-spacing: -0.02em;
    line-height: 1.25;
    color: var(--vp-c-text-1);
}

.home-body .flowtext {
    flex: 0 0 280px;
}

.home-body .flowtext h3 {
    margin: 0 0 10px;
    font-size: 18px;
}

.home-body .flowtext div[class*='language-'] {
    margin: 14px 0 0;
}

.home-body .flowtext div[class*='language-'] pre {
    padding: 12px 14px;
}

.home-body .flowtext div[class*='language-'] code {
    font-size: 11.5px;
}

.home-body .flowtext p {
    margin: 0;
    font-size: 14px;
    line-height: 1.65;
    color: var(--vp-c-text-2);
}

.home-body .flowrow .flow {
    flex: 1;
    min-width: 0;
}

@media (max-width: 767px) {
    .home-body .flowrow {
        flex-direction: column;
        align-items: stretch;
        gap: 8px;
        padding: 20px;
    }

    .home-body .flowtext {
        flex: none;
    }
}

.home-body .end-cta {
    margin: 56px 0 16px;
    text-align: center;
}

.home-body .end-cta a {
    display: inline-block;
    padding: 0 24px;
    line-height: 40px;
    border-radius: 22px;
    background-color: var(--vp-c-brand-3);
    color: #ffffff;
    font-size: 14px;
    font-weight: 600;
    text-decoration: none;
    transition: background-color 0.25s;
}

.home-body .end-cta a:hover {
    background-color: var(--vp-c-brand-2);
    color: #ffffff;
}

.home-body .code-exit {
    margin: 28px 0 0;
    text-align: center;
    font-size: 14px;
}

.home-body .code-exit p {
    margin: 0;
    font-size: 14px;
    color: var(--vp-c-text-2);
}

.home-body .code-exit a,
.home-body .byos a {
    color: var(--vp-c-text-2);
    font-weight: 400;
    text-decoration-color: var(--vp-c-divider);
}

.home-body .code-exit a:hover,
.home-body .byos a:hover {
    color: var(--vp-c-text-1);
}

.home-body .providers {
    display: flex;
    flex-wrap: wrap;
    justify-content: center;
    gap: 10px;
    margin: 0;
}

.home-body .providers span {
    padding: 8px 18px;
    border-radius: 24px;
    background-color: var(--vp-c-bg-soft);
    border: 1px solid var(--vp-c-divider);
    color: var(--vp-c-text-1);
    font-size: 14px;
    font-weight: 500;
}

.home-body .byos p:last-of-type {
    margin: 18px 0 0;
    font-size: 14px;
}
</style>
