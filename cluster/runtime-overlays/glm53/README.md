# GLM runtime asset provenance

Vendored under the accompanying MIT license from
https://github.com/rodman80/glm-5.3-flash-w4a16-2x-DGX-Sparks
at d5561311d178b3de7f7146e339629be1962ba8d1.

The template is pinned by content hash in the recipe. `patches.json` pins the
image source and output hashes for the hybrid prefix-cache patch and the small
GB10 top-k guard derived from `docs/sparse_attn_indexer_kpool_sm121.py` in the
same upstream repository. The latter modifies an Apache-2.0 vLLM source file
in place, preserving its copyright and license header. The MIT license here
covers the recipe's contributed patch code, not all upstream vLLM source.

`prepare-glm53-runtime.py` checks both base and output hashes, then stages immutable
overlays; changed anchors fail closed. These are candidate fixes pending hardware
correctness acceptance. Upstream launcher, network and clock scripts are not
executed. Model weights retain their upstream licenses.

The local GLM5Next model override corrects its inherited GLM4V packed-module
mapping: the checkpoint stores `gate_proj` and `up_proj` separately. Without
this correction, compressed-tensors fails to honor the BF16 ignore entries for
dense/shared MLPs, creates quantized parameters, and loading fails with
`KeyError: layers.0.mlp.gate_up_proj.weight`. The override preserves other
inherited mappings and does not change or skip checkpoint weights.
`scripts/probe-glm53-quantization.py` checks the actual image's matcher and model
classes against the pinned checkpoint configuration without allocating a GPU.
