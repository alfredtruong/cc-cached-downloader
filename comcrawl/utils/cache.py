import os
import json
from typing import List,Dict
import gzip

# write json
def write_json(obj:object, path: str) -> None:
	print(f'[write_json] {path}')

	# check if the directory exists, if not, create it
	directory = os.path.dirname(path)
	if not os.path.exists(directory):
		os.makedirs(directory)

	with open(path, 'w', encoding='utf-8') as f:
		f.write(json.dumps(obj))

# read json
def read_json(path: str) -> object:
    print(f'[read_json] {path}')
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            res = json.load(f)
        return res
    else:
        return []

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

    # check if the directory exists, if not, create it
    directory = os.path.dirname(path)
    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(path, 'w', encoding='utf-8') as f:
        for obj in objs:
            f.write(json.dumps(obj) + '\n')

# read jsonl
def read_jsonl(path: str) -> List[Dict]:
    print(f'[read_jsonl] {path}')
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            # parse and return each line
            lines = f.readlines()
            return parse_jsonlines(lines)
    else:
        return []

# write object to file
def write_file(obj:object, path: str) -> None:
    print(f'[write_file] {path}')

    # check if the directory exists, if not, create it
    directory = os.path.dirname(path)
    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(path, 'w', encoding='utf-8') as f:
        if obj is None:
            f.write('')
        else:
            f.write(str(obj))

# read file
def read_file(path: str) -> List[str]:
    print(f'[read_file] {path}')
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            # parse and return each line
            lines = f.readlines()
            return lines
    else:
        return []
    
# write gzip
def write_gzip(content:object, path: str) -> None:
    print(f'[write_gzip] {path}')

    # check if the directory exists, if not, create it
    directory = os.path.dirname(path)
    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(path, "wb") as f:
        f.write(content)

# read gzip
def read_gzip(path: str) -> List[str]:
    print(f'[read_gzip] {path}')
    with gzip.open(path, 'rb') as f:
        content = f.read()
        raw_content: str = content.decode('utf-8')

    return raw_content