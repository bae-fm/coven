import { defineConfig } from 'vitepress'

function rustdocHref(spec) {
    const separator = spec.indexOf(':')
    const kind = separator === -1 ? '' : spec.slice(0, separator)
    const path = separator === -1 ? '' : spec.slice(separator + 1)
    if (!kind || !path) {
        throw new Error(`Invalid rustdoc link: ${spec}`)
    }

    const segments = path.split('::')
    const crate = segments.shift()
    if (!crate) {
        throw new Error(`Invalid rustdoc path: ${path}`)
    }

    if (kind === 'mod') {
        const modulePath = segments.length > 0 ? `${segments.join('/')}/` : ''
        return `/rustdoc/${crate}/${modulePath}index.html`
    }

    if (kind === 'method') {
        const method = segments.pop()
        const typeName = segments.pop()
        if (!method || !typeName) {
            throw new Error(`Invalid rustdoc method path: ${path}`)
        }
        return `/rustdoc/${crate}/${segments.join('/')}/struct.${typeName}.html#method.${method}`
    }

    if (kind === 'variant') {
        const variant = segments.pop()
        const enumName = segments.pop()
        if (!variant || !enumName) {
            throw new Error(`Invalid rustdoc variant path: ${path}`)
        }
        const modulePath = segments.length > 0 ? `${segments.join('/')}/` : ''
        return `/rustdoc/${crate}/${modulePath}enum.${enumName}.html#variant.${variant}`
    }

    const item = segments.pop()
    if (!item) {
        throw new Error(`Invalid rustdoc item path: ${path}`)
    }

    const prefixes = {
        const: 'constant',
        enum: 'enum',
        fn: 'fn',
        struct: 'struct',
        trait: 'trait',
        type: 'type',
    }
    const prefix = prefixes[kind]
    if (!prefix) {
        throw new Error(`Unknown rustdoc link kind: ${kind}`)
    }

    const modulePath = segments.length > 0 ? `${segments.join('/')}/` : ''
    return `/rustdoc/${crate}/${modulePath}${prefix}.${item}.html`
}

function rustdocLinks(md) {
    const defaultRender =
        md.renderer.rules.link_open ??
        ((tokens, idx, options, _env, self) => self.renderToken(tokens, idx, options))

    md.renderer.rules.link_open = (tokens, idx, options, env, self) => {
        const token = tokens[idx]
        const hrefIndex = token.attrIndex('href')
        if (hrefIndex >= 0) {
            const href = token.attrs[hrefIndex][1]
            if (href.startsWith('rustdoc:')) {
                token.attrs[hrefIndex][1] = rustdocHref(href.slice('rustdoc:'.length))
                token.attrSet('target', '_self')
            }
        }
        return defaultRender(tokens, idx, options, env, self)
    }
}

export default defineConfig({
    title: 'coven',
    titleTemplate: ':title - coven',
    description: 'End-to-end encrypted, multi-writer SQLite sync over bring-your-own storage.',
    lang: 'en-US',
    cleanUrls: true,
    lastUpdated: false,

    head: [
        ['link', { rel: 'icon', type: 'image/svg+xml', href: '/favicon.svg' }],
        ['meta', { name: 'theme-color', content: '#345f5d' }],
        ['meta', { property: 'og:type', content: 'website' }],
        ['meta', { property: 'og:site_name', content: 'coven' }],
        ['meta', { name: 'twitter:card', content: 'summary' }],
    ],

    markdown: {
        config(md) {
            md.use(rustdocLinks)
        },
        theme: {
            light: 'github-light',
            dark: 'github-dark',
        },
        lineNumbers: false,
    },

    themeConfig: {
        logo: '/favicon.svg',
        siteTitle: 'coven',

        nav: [
            { text: 'Docs', link: '/docs/', activeMatch: '/docs/' },
        ],

        sidebar: {
            '/docs/': [
                {
                    text: 'Docs',
                    items: [
                        { text: 'Overview', link: '/docs/' },
                        { text: 'Comparison', link: '/docs/comparison' },
                        { text: 'Sync', link: '/docs/sync-model' },
                        { text: 'Local data', link: '/docs/local-data' },
                        { text: 'Bootstrap', link: '/docs/bootstrap' },
                        { text: 'Schema evolution', link: '/docs/schema-evolution' },
                        { text: 'Sharing', link: '/docs/sharing' },
                        { text: 'Encryption', link: '/docs/encryption' },
                        { text: 'Storage', link: '/docs/storage' },
                        { text: 'Blobs', link: '/docs/blobs' },
                        { text: 'Cache', link: '/docs/cache' },
                        { text: 'Web', link: '/docs/web' },
                    ],
                },
                {
                    text: 'Reference',
                    items: [
                        { text: 'Example', link: '/docs/example' },
                        { text: 'Rust API', link: '/rustdoc/coven/index.html', target: '_self' },
                        { text: 'Crate Index', link: '/rustdoc/coven/all.html', target: '_self' },
                    ],
                },
            ],
        },

        socialLinks: [
            { icon: 'github', link: 'https://github.com/bae-fm/coven' },
        ],

        footer: {
            message: 'Released under the Apache-2.0 License.',
            copyright: '© 2026 coven',
        },

        search: {
            provider: 'local',
        },

        outline: {
            level: [2, 3],
            label: 'On this page',
        },

        docFooter: {
            prev: 'Previous',
            next: 'Next',
        },
    },
})
