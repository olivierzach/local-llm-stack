#!/usr/bin/env python3
"""Create deterministic synthetic audio and a minimal Vector Bucket job fixture."""
import argparse
import array
import json
import math
from pathlib import Path
import sys
import wave


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    root=args.output.resolve()
    if root.exists(): raise ValueError('choose a fresh fixture directory')
    (root/'audio').mkdir(parents=True)
    (root/'data/manifests').mkdir(parents=True)
    tracks=[]
    for index,frequency in enumerate((220,880)):
        identifier=f'smoke-tone-{index}'
        path=root/'audio'/f'{identifier}.wav'
        rate=48000
        samples=array.array('h',(int(8000*math.sin(2*math.pi*frequency*i/rate)) for i in range(5*rate)))
        if sys.byteorder!='little':samples.byteswap()
        with wave.open(str(path),'wb') as stream:
            stream.setnchannels(1);stream.setsampwidth(2);stream.setframerate(rate);stream.writeframes(samples.tobytes())
        tracks.append({'track_id':identifier,'album_id':'smoke-album','absolute_path':str(path),
            'relative_path':path.name,'duration_seconds':5.0,'title':f'Synthetic tone {frequency} Hz',
            'artist':'Generated fixture','album':'Spark acceptance'})
    (root/'data/manifests/tracks.jsonl').write_text(''.join(json.dumps(t)+'\n' for t in tracks))
    (root/'data/manifests/albums.jsonl').write_text(json.dumps({'album_id':'smoke-album','title':'Spark acceptance'})+'\n')
    (root/'job.json').write_text(json.dumps({'items':tracks,'count':len(tracks)},indent=2)+'\n')
    print(json.dumps({'fixture':str(root),'tracks':len(tracks)}))


if __name__=='__main__':main()
