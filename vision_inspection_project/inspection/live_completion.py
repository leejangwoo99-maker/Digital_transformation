"""Publish only complete verdict logs to the downstream consumer."""
import os
from time import monotonic


def publish_txt(path,lines,started_at,count):
    temporary = path.with_name(path.name+'.pending')
    try:
        with temporary.open('w',encoding='utf-8') as stream:
            stream.write('\n'.join(lines)+'\n')
            stream.flush()
            os.fsync(stream.fileno())
            elapsed = monotonic()-started_at
            stream.write(f'pipeline time(sec) : {elapsed:.7f}\ninspection_count: {count}\n')
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
        return monotonic()-started_at
    finally:
        if temporary.exists():
            temporary.unlink()
