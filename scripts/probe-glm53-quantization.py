#!/usr/bin/env python3
"""CPU-only regression check, run inside the pinned GLM image with its overlay."""
import argparse
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    args = parser.parse_args()
    from vllm.models.glm5next.nvidia.model import Glm5NextForConditionalGeneration as Model
    from vllm.model_executor.models.glm4_1v import Glm4vForConditionalGeneration as Base
    from vllm.model_executor.layers.quantization.compressed_tensors.utils import should_ignore_layer

    config = json.loads(args.config.read_text())
    ignore = Model.hf_to_vllm_mapper.apply_list(config['quantization_config']['ignore'])
    mapping = Model.packed_modules_mapping
    checks = []
    # The checkpoint keeps the first three dense MLPs and all shared experts BF16.
    for layer in range(config['text_config']['num_hidden_layers']):
        mlp = f'language_model.model.layers.{layer}.mlp'
        dense = layer < config['text_config']['first_k_dense_replace']
        prefix = mlp if dense else mlp + '.shared_experts'
        for projection in ('gate_up_proj', 'down_proj'):
            name = prefix + '.' + projection
            assert should_ignore_layer(name, ignore, mapping), name
            checks.append(name)
        if not dense:
            name = mlp + '.experts.0.gate_up_proj'
            assert not should_ignore_layer(name, ignore, mapping), name
            checks.append(name)
    regression = 'language_model.model.layers.0.mlp.gate_up_proj'
    assert not should_ignore_layer(regression, ignore, Base.packed_modules_mapping)
    assert mapping['qkv_proj'] == Base.packed_modules_mapping['qkv_proj']
    print(json.dumps({'complete': True, 'gpu_used': False, 'checks': len(checks),
                      'unpatched_regression_reproduced': True}))


if __name__ == '__main__':
    main()
