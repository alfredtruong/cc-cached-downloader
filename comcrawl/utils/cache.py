import os
import json
from typing import List,Dict

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
			f.write(json.dumps(obj))

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

'''
# write object to json
def write_file(obj:object, path: str) -> None:
    print(f'[write_file] write to {path}')

    # check if the directory exists, if not, create it
    directory = os.path.dirname(path)
    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(path, 'w', encoding='utf-8') as f:
        # write to file
        if obj is None:
            f.write('')
        else:
            f.write(str(obj))
'''