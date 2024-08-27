import os
import json
from typing import List,Dict
import gzip
from pathlib import Path
import threading
import cchardet as chardet # pip install faust-cchardet
#import chardet
from smart_open import open as s_open

lock = threading.Lock()

# write json
def write_json(obj:object, path: str) -> None:
    #print(f'[write_json] {path}')
    directory = Path(path).parent
    try:
		# check if directory exists, if not, create it
        if not directory.exists():
            directory.mkdir(parents=True, exist_ok=True)
            directory.chmod(0o777)

        with open(path, 'w', encoding='utf-8') as f:
            f.write(json.dumps(obj))

        os.chmod(path, 0o777)

    except Exception as e:
	    print(f'[write_json] exception {e}')

# read json
def read_json(path: str) -> object:
    #print(f'[read_json] {path}')
    res = []
    try:
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                res = json.load(f)
    except Exception as e:
        print(f'[read_json] exception {e}')

    return res

# parse jsonlines
def parse_jsonlines(lines: List[str]) -> List[Dict]:
    # how many records
    #total_lines = len(lines)
    #print(f"[parse_jsonlines] lines = {total_lines}")

    # return parsed lines
    return [json.loads(line) for line in lines]

# write jsonl
def write_jsonl_0(list_dict:List[object], path: str) -> None:
    #print(f'[write_jsonl] {path}')
    directory = Path(path).parent
    try:
		# check if directory exists, if not, create it
        if not directory.exists():
            directory.mkdir(parents=True, exist_ok=True)
            directory.chmod(0o777)

        with lock:
            with open(path, 'a', encoding='utf-8') as f:
                for d in list_dict:
                    line = json.dumps(d, ensure_ascii=False) # build line
                    f.write(line + '\n') # write line

        os.chmod(path, 0o777)

    except Exception as e:
	    print(f'[write_jsonl] exception {e}')

# read jsonl
def read_jsonl_0(path: str) -> List[Dict]:
    #print(f'[read_jsonl] {path}')
    lines: List[Dict] = []
    try:
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
    except Exception as e:
        print(f'[read_jsonl] exception {e}')
    return parse_jsonlines(lines)

def write_jsonl(list_dict:List[object], path: str) -> None:
    """
    Write a string to a text file.
    If the file size exceeds 1GB, it will be split into multiple files.
    """
    directory = Path(path).parent
    try:
		# check if directory exists, if not, create it
        if not directory.exists():
            directory.mkdir(parents=True, exist_ok=True)
            directory.chmod(0o777)

        with lock:
            with s_open(path, 'a', encoding='utf-8', transport_params={'max_file_size': 1024 * 1024 * 1024}) as f:
                for d in list_dict:
                    line = json.dumps(d, ensure_ascii=False) # build line
                    f.write(line + '\n') # write line

        os.chmod(path, 0o777)

    except Exception as e:
	    print(f'[write_jsonl] exception {e}')

def read_jsonl(path: str) -> str:
    """
    Read a text file (potentially split into multiple files) and return the contents as a single string.
    """
    lines: List[Dict] = []
    try:
        if os.path.exists(path):
            with s_open(path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
    except Exception as e:
        print(f'[read_jsonl] exception {e}')

    return parse_jsonlines(lines)

# write object to file
def write_file(obj:object, path: str) -> None:
    #print(f'[write_file] {path}')
    directory = Path(path).parent
    try:
		# check if directory exists, if not, create it
        if not directory.exists():
            directory.mkdir(parents=True, exist_ok=True)
            directory.chmod(0o777)

        with open(path, 'w', encoding='utf-8') as f:
            if obj is None:
                f.write('')
            else:
                f.write(str(obj))

        os.chmod(path, 0o777)

    except Exception as e:
	    print(f'[write_file] exception {e}')
    
# read file
def read_file(path: str) -> List[str]:
    #print(f'[read_file] {path}')
    lines: List[str] = []
    try:
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
    except Exception as e:
        print(f'[read_file] exception {e}')

    return lines
    
# write gzip
def write_gzip(content:object, path: str) -> None:
    #print(f'[write_gzip] {path}')
    directory = Path(path).parent
    try:
		# check if directory exists, if not, create it
        if not directory.exists():
            directory.mkdir(parents=True, exist_ok=True)
            directory.chmod(0o777)

        with open(path, "wb") as f:
            f.write(content)

        os.chmod(path, 0o777)

    except Exception as e:
	    print(f'[write_gzip] exception {e}')
        
# read gzip
def read_gzip(path: str) -> str:
    #print(f'[read_gzip] {path}')
    raw_content: str = ''
    try:
        if os.path.exists(path):
            with gzip.open(path, 'rb') as f:
                content = f.read()

                # try common encodings first
                for encoding in ['utf-8','GB18030']: #  'gbk', 'big5',
                    try:
                        raw_content = content.decode(encoding)
                        break
                    except UnicodeDecodeError:
                        continue

                # if common encodings fail, using cchardet to detect encoding
                if not raw_content:
                    # Detect the encoding
                    encoding = chardet.detect(content)
                    if isinstance(encoding,dict): 
                        encoding = encoding.get('encoding')
                        print(f'[read_gzip][encoding] detected = {encoding}')

                    if encoding:
                        raw_content = content.decode(encoding) # decode
                    else:
                        raw_content = '' # If chardet couldn't detect the encoding, return nothing
    except Exception as e:
        print(f'[read_gzip] exception {e}')

    return raw_content