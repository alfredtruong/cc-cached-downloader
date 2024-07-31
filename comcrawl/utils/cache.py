import os
import json
from typing import List,Dict
import gzip

# write json
def write_json(obj:object, path: str) -> None:
	print(f'[write_json] write to {path}')

	# check if the directory exists, if not, create it
	directory = os.path.dirname(path)
	if not os.path.exists(directory):
		os.makedirs(directory)

	with open(path, 'w', encoding='utf-8') as f:
		f.write(json.dumps(obj))

# read json
def read_json(path: str) -> object:
    print(f'[read_json] read {path}')
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
    print(f"[parse_jsonlines] total lines = {total_lines}")

    # return parsed lines
    return [json.loads(line) for line in lines]

# write jsonl
def write_jsonl(objs:List[object], path: str) -> None:
    print(f'[write_jsonl] write to {path}')

    # check if the directory exists, if not, create it
    directory = os.path.dirname(path)
    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(path, 'w', encoding='utf-8') as f:
        for obj in objs:
            f.write(json.dumps(obj) + '\n')

# read jsonl
def read_jsonl(path: str) -> List[Dict]:
    print(f'[read_jsonl] read {path}')
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            # parse and return each line
            lines = f.readlines()
            return parse_jsonlines(lines)
    else:
        return []

# write object to file
def write_file(obj:object, path: str) -> None:
    print(f'[write_file] write to {path}')

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
    print(f'[read_file] read {path}')
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            # parse and return each line
            lines = f.readlines()
            return lines
    else:
        return []
    
# read gz file
def read_gzip(path: str) -> List[str]:
    # Assuming the file is saved as 'compressed_file.gz'
    with gzip.open(path, 'rb') as f:
        content = f.read()
        raw_content: str = content.decode('utf-8')

    return raw_content