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

## Two-node rollout result

Both baselines were reconciled to cleanup commit `2d957cc`, descended from the
complete feature revision `696af2a`. The final documentation commit and merge
retain the same operational source. GitHub PR [#1](https://github.com/olivierzach/local-llm-stack/pull/1)
records publication into main. Both immutable installed controllers remain at
`696af2a`; this cleanup changes no controller execution code relative to that release.

- 66f1: all 450 tests passed in 68.56 seconds.
- e8f1: all 450 tests passed in 69.55 seconds.
- GitHub's Static checks job passed for the cleanup revision.
- Both nodes passed Compose and individual shell syntax checks, authenticated
  LiteLLM/Guard model discovery, invalid-key rejection, managed-gateway discovery,
  and a small guarded GLM response identifying the exact accepted deployment.
- Both baseline checkouts were clean. Both GLM worker container identities and
  start times were preserved and their health checks remained healthy.
- The local LiteLLM environment and healthcheck now match rendered Compose.
- The peer's active environment symlink and protected runtime files were preserved.

The peer rollback archive is
`logs/source-reconciliation/20260916T042743Z/`. Neither source archive contains
private runtime data. `.env` and operational artifacts stayed in place.

Normal operation should use a clean baseline `main` synchronized with
`origin/main`, while controller upgrades continue to use explicit immutable
releases. Future maintenance must distinguish source synchronization, container
configuration rollout, and GPU qualification instead of mixing their status.
The original audit remains a dated snapshot, including subsequently resolved
findings. Deferred hardware tests and research plans remain in the cluster checklist.
