#!/usr/bin/env python3
"""Send actual image attachments through an isolated OMP, llm or AIChat profile."""
import argparse
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('vision_probe', ROOT/'scripts/probe-spark-vision.py')
vision = importlib.util.module_from_spec(spec)
spec.loader.exec_module(vision)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--node', required=True)
    parser.add_argument('--client', choices=('omp', 'llm', 'aichat'), required=True)
    parser.add_argument('--port', type=int, default=4110)
    parser.add_argument('--key-file', type=Path)
    parser.add_argument('--registry', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output = args.output.resolve()
    with tempfile.TemporaryDirectory(prefix='spark-vision-client-') as temporary:
        root = Path(temporary)
        images = root/'images'
        images.mkdir()
        paths = []
        for color in ('blue', 'red'):
            path = images/(color+'.png')
            # Filenames must not tell the model the colors it needs to recognize.
            path = path.with_name('image-'+str(len(paths))+'.png')
            path.write_bytes(vision.png(color))
            paths.append(path)
        prompt = 'Identify the solid fill color of each image. Reply only as a JSON array of lowercase English color names, in image order.'
        command = [sys.executable, str(ROOT/'scripts/spark-client'), 'run', '--node', args.node,
                   '--port', str(args.port), '--client', args.client, '--output', str(root/'profile')]
        for flag, path in [('--key-file', args.key_file), ('--registry', args.registry)]:
            if path: command += [flag, str(path.resolve())]
        if args.client == 'omp':
            command += ['--', '--model', 'spark-'+args.node+'/local-vision', '--no-tools', '--no-lsp',
                        '--no-extensions', '--no-session', '--max-time', '90', '--print', *['@'+str(p) for p in paths], prompt]
        elif args.client == 'llm':
            command += ['--', '-m', 'spark-'+args.node+'/local-vision', '--no-log', '-a', str(paths[0]), '-a', str(paths[1]), prompt]
        else:
            command += ['--attachment-dir', str(images), '--', '--model', 'spark:local-vision',
                        '--file', str(paths[0]), '--file', str(paths[1]), prompt]
        result = subprocess.run(command, cwd=root, capture_output=True, text=True, timeout=120)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.with_suffix('.stdout').write_text(result.stdout)
        args.output.with_suffix('.stderr').write_text(result.stderr)
        if result.returncode: raise RuntimeError('client failed; inspect the saved stderr')
        vision.answer(result.stdout, ['blue', 'red'])
        report = {'passed': True, 'client': args.client, 'gateway': args.node, 'answer': result.stdout.strip(),
                  'scope': 'Two synthetic image attachments with opaque filenames; no tools or coding-quality evaluation.'}
        args.output.write_text(json.dumps(report, indent=2)+'\n')
        print(json.dumps(report))


if __name__ == '__main__': main()
