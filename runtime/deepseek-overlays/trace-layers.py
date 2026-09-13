# Diagnostic instrumentation appended to the pinned vLLM model.py.
# It records only synthetic 36-token fixture tensors, at most eight forwards.
# No weight, token text, or credential is recorded. This is never a serving profile.
def _install_local_deepseek_trace(model):
    import hashlib
    import json
    from pathlib import Path
    state = {'active': False, 'request': 0}
    rank = get_tensor_model_parallel_rank()
    destination = Path(f'/tmp/deepseek-trace-{rank}.jsonl')

    def tensors(value):
        if isinstance(value, torch.Tensor):
            x = value.detach().contiguous().cpu()
            xf = x.float()
            return {'shape': list(x.shape), 'dtype': str(x.dtype),
                    'sha256': hashlib.sha256(x.view(torch.uint8).numpy().tobytes()).hexdigest(),
                    'mean': float(xf.mean()) if x.numel() else None,
                    'min': float(xf.min()) if x.numel() else None,
                    'max': float(xf.max()) if x.numel() else None}
        if isinstance(value, (tuple, list)):
            return [tensors(v) for v in value]
        if isinstance(value, dict):
            return {k: tensors(v) for k, v in value.items()}
        return None

    def emit(name, inputs, outputs):
        with destination.open('a') as f:
            f.write(json.dumps({'rank': rank, 'request': state['request'], 'module': name,
                                'inputs': tensors(inputs), 'outputs': tensors(outputs)}) + '\n')

    def begin(module, inputs):
        ids = inputs[0] if inputs else None
        state['active'] = isinstance(ids, torch.Tensor) and ids.numel() == 36 and state['request'] < 8
        if state['active']:
            state['request'] += 1
            emit('model.begin', inputs, None)

    def end(module, inputs, outputs):
        if state['active']:
            emit('model.end', inputs, outputs)
        state['active'] = False

    model.register_forward_pre_hook(begin)
    selected = ('attn', 'ffn', 'attn.fused_wqa_wkv', 'attn.wq_b', 'ffn.gate', 'ffn.experts', 'ffn.shared_experts')
    for name, module in model.named_modules():
        parts = name.split('.')
        wanted = name in ('embed_tokens', 'norm', 'hc_head') or (
            len(parts) >= 2 and parts[0] == 'layers' and parts[1].isdigit()
            and (len(parts) == 2 or '.'.join(parts[2:]) in selected))
        if wanted:
            def hook(module, inputs, outputs, name=name):
                if state['active']:
                    emit(name, inputs, outputs)
            module.register_forward_hook(hook)
    model.register_forward_hook(end)


_local_original_process = DeepseekV4ForCausalLM.process_weights_after_loading

def _local_traced_process(self):
    _local_original_process(self)
    if not getattr(self, '_local_trace_installed', False):
        _install_local_deepseek_trace(self.model)
        self._local_trace_installed = True

DeepseekV4ForCausalLM.process_weights_after_loading = _local_traced_process
