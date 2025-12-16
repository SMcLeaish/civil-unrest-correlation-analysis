import lzma
import os
import shutil
from pathlib import Path

def check_file_compression(csv_path) -> str:
    compressed_path = f'{csv_path}.xz'
    if os.path.exists(csv_path):
        return csv_path
    try:
        if os.path.exists(compressed_path):
            decompress(compressed_path)
            return csv_path
    except Exception as e:
        raise FileNotFoundError from e
    return csv_path

def compress(path: str | Path) -> Path:
    path = Path(path)
    out_path = path.with_suffix(path.suffix + '.xz')

    with path.open('rb') as src, lzma.open(out_path, 'wb', preset=9) as dst:
        shutil.copyfileobj(src, dst)
        return out_path

def decompress(path: str | Path) -> Path:
    path = Path(path)

    if not path.suffix == '.xz':
        raise ValueError(f'File does not end with .xz {path}')

    out_path = path.with_suffix('')

    with lzma.open(path, 'rb') as src, out_path.open('wb') as dst:
        shutil.copyfileobj(src,dst)

    return out_path