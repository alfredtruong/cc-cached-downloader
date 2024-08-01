import os
import json
from typing import List,Dict
import gzip

# write json
def write_json(obj:object, path: str) -> None:
    print(f'[write_json] {path}')
    try:
        # check if the directory exists, if not, create it
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(path, 'w', encoding='utf-8') as f:
            f.write(json.dumps(obj))
    except Exception as e:
	    print(f'[write_json] exception {e}')

# read json
def read_json(path: str) -> object:
    print(f'[read_json] {path}')
    res = []
    try:
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                res = json.load(f)
    except Exception as e:
        print(f'[read_json] exception {e}')
    finally:
        return res

# parse jsonlines
def parse_jsonlines(lines: List[str]) -> List[Dict]:
    # how many records
    total_lines = len(lines)
    print(f"[parse_jsonlines] lines = {total_lines}")

    # return parsed lines
    return [json.loads(line) for line in lines]

# write jsonl
def write_jsonl(objs:List[object], path: str) -> None:
    print(f'[write_jsonl] {path}')
    try:
		# check if the directory exists, if not, create it
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(path, 'w', encoding='utf-8') as f:
            for obj in objs:
                f.write(json.dumps(obj) + '\n')
    except Exception as e:
	    print(f'[write_jsonl] exception {e}')

# read jsonl
def read_jsonl(path: str) -> List[Dict]:
    print(f'[read_jsonl] {path}')
    lines: List[Dict] = []
    try:
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
    except Exception as e:
        print(f'[read_jsonl] exception {e}')
    finally:
        return parse_jsonlines(lines)

# write object to file
def write_file(obj:object, path: str) -> None:
    print(f'[write_file] {path}')
    try:
		# check if the directory exists, if not, create it
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(path, 'w', encoding='utf-8') as f:
            if obj is None:
                f.write('')
            else:
                f.write(str(obj))
    except Exception as e:
	    print(f'[write_file] exception {e}')
    
# read file
def read_file(path: str) -> List[str]:
    print(f'[read_file] {path}')
    lines: List[str] = []
    try:
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
    except Exception as e:
        print(f'[read_file] exception {e}')
    finally:
        return lines    
    
# write gzip
def write_gzip(content:object, path: str) -> None:
    print(f'[write_gzip] {path}')
    try:
        # check if the directory exists, if not, create it
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(path, "wb") as f:
            f.write(content)
    except Exception as e:
	    print(f'[write_gzip] exception {e}')
        
# read gzip
def read_gzip(path: str) -> List[str]:
    print(f'[read_gzip] {path}')
    raw_content: List[str] = []
    try:
        if os.path.exists(path):
            with gzip.open(path, 'rb') as f:
                content = f.read()
                raw_content: str = content.decode('utf-8')
    except Exception as e:
        print(f'[read_gzip] exception {e}')
    finally:
        return raw_content