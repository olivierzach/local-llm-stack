# Source reconciliation, September 16, 2026

Both baseline checkouts had stayed at `500d050` while selected integration files
were copied from the feature branch. Both installed controllers already used
`696af2a`. The [audit](audits/2026-09-16/audit.md) matched every substantive local
source change to that branch's history.

Cleanup brings baseline source onto the complete history, includes its matching
tests and recipe validators, ignores runtime trees and environment backups, and
updates the serving status. Existing tracked sample datasets remain versioned.

Baseline services and versioned controller releases keep separate deployment
paths. Future baseline changes must use a committed revision. Controller upgrades
continue through the versioned installer; its `current` symlink does not update
baseline source or restart services automatically.

Before updating baseline source, retain a private source archive, original Git
revision and patch, protected runtime fingerprints, and container identities.
Rollback archives belong in ignored `logs/source-reconciliation/`. Preserve
`.env`, registries, runtime overlays/libraries, checkpoints, plans and reservations.
Avoid indiscriminate reset/clean operations in a service's mounted source directory.

Container environment and healthcheck changes require recreation, followed by
readiness, authentication, discovery and route verification. Name the CPU router
services explicitly and use `--no-deps` to avoid starting a default GPU model.

## Verification during cleanup

The reconciled local source passed all 450 CPU tests, all-profile Compose
validation, Git whitespace checks, both saved GLM plan validations, and ignore
checks for private/runtime paths. Protected `.env`, registry, receipt and overlay
files retained their content and permissions. The original local source archive
is `logs/source-reconciliation/20260916T040943Z/`.

The local LiteLLM container was recreated with the same installed image to apply
its current environment and healthcheck. Model workers were not restarted.

OS package installation is optional follow-up, separate from this source cleanup
and from operating the current GLM service. The attempted installs changed nothing
because sudo required interactive host authentication. The package-baseline audit
remains useful for future build, audio and diagnostic work.

Deployed source identities and final two-node verification are recorded after rollout.
The original audit remains a dated snapshot, including subsequently resolved
findings. Deferred hardware tests and research plans remain in the cluster checklist.
