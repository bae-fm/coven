import { cp, rm } from 'node:fs/promises'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { spawn } from 'node:child_process'

const siteDir = dirname(dirname(fileURLToPath(import.meta.url)))
const repoRoot = resolve(siteDir, '..')
const rustdocSource = resolve(repoRoot, 'target/doc')
const rustdocPublic = resolve(siteDir, 'public/rustdoc')

await run('cargo', ['doc', '--no-deps', '--all-features'], repoRoot)
await rm(rustdocPublic, { recursive: true, force: true })
await cp(rustdocSource, rustdocPublic, { recursive: true })
await run('npx', ['vitepress', 'build'], siteDir)

function run(command, args, cwd) {
    return new Promise((resolveRun, rejectRun) => {
        const child = spawn(command, args, {
            cwd,
            stdio: 'inherit',
            shell: process.platform === 'win32',
        })

        child.on('error', rejectRun)
        child.on('exit', (code, signal) => {
            if (code === 0) {
                resolveRun()
            } else {
                rejectRun(new Error(`${command} exited with ${signal ?? code}`))
            }
        })
    })
}
