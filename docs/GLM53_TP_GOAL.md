# GLM-5.3-Flash on interchangeable Sparks

Focused objective requested September 13, 2026. **Completed and qualified.**
The thread goal tracker still contains the unfinished broader
interchangeable-node goal and refused creation of a second goal. This document
records the current focused objective without declaring that broader work complete.

## Objective

Prepare, qualify and serve GLM-5.3-Flash across the direct two-Spark fabric with a
pinned runtime, speculative decoding, measured practical context/concurrency,
interchangeable coordinators and a distinct `local-glm53-flash` Context Guard
alias. Verify text, tool continuation, reasoning and image understanding before
advertising those capabilities. Provide deterministic setup/start/stop/acceptance
commands and preserve existing DeepSeek and single-node recipes and client aliases.

Inspect active requests before a controlled GPU transition. Artifact preparation
may proceed while DeepSeek runs; never stop unrelated research or user workloads.
Model/image copies between Sparks must use the verified direct fabric. Do not
change network addressing, drivers, clocks or system services based on a third-party
installer. Review and adapt source rather than execute upstream launch/install scripts.

## Starting candidate and evidence limits

- Target: `canada-quant/glm-5.3-w4a16-mtp` at
  `4eeb77a3499fc2503290197118102eae2ed44553`; approximately 191 GB downloaded.
- Drafter: `incoai/GLM-5.3-Flash-DFlash2` at
  `bf582e4eacc1810f76656d1811693ff6c6737d2a`; approximately 2.34 GB downloaded.
- Public ARM64 runtime: `ghcr.io/tonyd2wild/vllm-glm53-flash` at digest
  `sha256:4def0ef644cb2e9814136dcffd5e385e21bc594f48f3b292234051904abe85a6`.
- Start with W4A16 Marlin, eager execution, FP8 KV and DFlash2 K=7. Begin with a
  bounded 256K context and measured memory headroom, then assess concurrent agents.
  Do not import the upstream 1M/9-GiB KV override as proven safe on our machines.

Primary recipe reviewed:
[rodman80 W4A16](https://github.com/rodman80/glm-5.3-flash-w4a16-2x-DGX-Sparks)
at `d5561311d178b3de7f7146e339629be1962ba8d1`. Its source and benchmarks include
concurrency and prefix-cache fixes but describe a work-in-progress deployment.
The alternative [alexellis NVFP4 recipe](https://github.com/alexellis/glm-5.3-flash-2x-dgx-spark-switchless)
at `b9f15dd033d88c2309bb7d3d1ab406153c22e568` reports cold-boot evidence with an
unpublished local image; the public digest is a reproduction candidate rather
than a byte-identical proven appliance. Neither repository's performance figures
are measurements on this cluster. Model identity/capabilities originate from
[Z.ai's model card](https://huggingface.co/zai-org/GLM-5.3-Flash).

## Completion gates

1. Pinned target/draft/image checksums verified on both nodes with direct-copy receipts.
2. Strict compatible recipe/schema support, cache/overlay validation, owned lifecycle
   and tests that preserve existing recipes; no unreviewed remote model code.
3. Both coordinator roles pass real text, SSE, tools, reasoning and image probes;
   repeatability and a bounded sequential/concurrent soak produce no stalls.
4. Record cold/warm prefill, decode, useful request throughput, prefix/draft
   acceptance, memory pressure and RoCE evidence. Choose an observed operating
   point, not a claim of a globally optimal configuration.
5. Publish the new alias through both guards, verify actual OMP tool use and
   image support, and preserve other client defaults unless explicitly requested.
6. Commit/push reproducible commands, recipes, docs and evidence references.
   Preserve exact-plan cleanup and DeepSeek restoration instructions.

## Progress record

All six completion gates are satisfied for this focused model setup:

- Pinned artifacts and strict runtime overlays are verified on both nodes, with
  peer transfers over the direct fabric.
- The selected 0.82-memory recipe passed the full profile with 66f1 coordinating
  (`66f1-02/qualification-01`) and the reversed-role profile initiated from e8f1
  (`e8f1-04/qualification-01`). e8f1 remains the live coordinator.
- Controller revision `eee6285` passed all 450 Linux tests and verifies memory,
  worker identity and observed RoCE traffic before publication.
- Both 4010/4110 gateways on both nodes publish the new alias while preserving
  other routes and defaults. Actual OMP tools/thinking, OMP image attachments,
  an llm image request and FamChat's configured provider passed.
- Commands, selected limits, source pins, measured results and exact-plan
  DeepSeek restoration are recorded in the [runbook](GLM53_TP.md) and
  [evidence](GLM53_TP_EVIDENCE.md), committed on the existing feature branch.

This does not declare the broader interchangeable-node goal complete, promise
four simultaneous full-context requests, or constitute a general model-quality
benchmark. The focused deployment is reproducible without an agent making
runtime configuration decisions.

## Preparation commands

`scripts/prepare-glm53-tp.py --peer NODE --output DIR` prints the staging plan
when run on the other inventoried Spark. Add `--apply` to download and verify
the pinned artifacts, then copy them over the direct cable. It never starts a
GPU worker. The same output directory supports resuming interrupted preparation.
Artifact staging alone does not satisfy the serving gates above.
