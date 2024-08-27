import os
import json
import gzip
from pathlib import Path
import threading
import cchardet as chardet # pip install faust-cchardet
#import chardet
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import glob
import time

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
def parse_jsonlines(lines: list[str]) -> list[dict]:
    # how many records
    #total_lines = len(lines)
    #print(f"[parse_jsonlines] lines = {total_lines}")

    # return parsed lines
    return [json.loads(line) for line in lines]

# write jsonl
def write_jsonl_0(list_dict:list[object], path: str) -> None:
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
def read_jsonl_0(path: str) -> list[dict]:
    #print(f'[read_jsonl] {path}')
    lines: list[dict] = []
    try:
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
    except Exception as e:
        print(f'[read_jsonl] exception {e}')
    return parse_jsonlines(lines)

def write_jsonl(list_dict: list[dict], base_path: str, max_json_lines: int = 100000) -> None:
    """
    Write a list of dictionaries to a JSONL file, and when the file size exceeds the specified maximum number of lines,
    write the data to a Parquet file and clear the JSONL file.
    """
    directory = Path(base_path).parent
    parquet_file_index = 0

    # Check for existing Parquet files and update the parquet_file_index
    existing_parquet_files = sorted(glob.glob(os.path.join(directory, f"{Path(base_path).stem}_*.parquet")))
    if existing_parquet_files:
        parquet_file_index = max([int(Path(f).stem.split("_")[1]) for f in existing_parquet_files]) + 1

    # Check for existing JSONL file and update the current_file_size and current_line_count
    current_file_size = 0
    current_line_count = 0
    if os.path.exists(base_path):
        current_file_size = os.path.getsize(base_path)
        with open(base_path, 'r', encoding='utf-8') as f:
            current_line_count = sum(1 for _ in f)

    try:
        # Check if directory exists, if not, create it
        if not directory.exists():
            directory.mkdir(parents=True, exist_ok=True)
            directory.chmod(0o777)

        with lock:
            with open(base_path, 'a', encoding='utf-8') as f:
                for d in list_dict:
                    line = json.dumps(d, ensure_ascii=False).encode('utf-8').decode('utf-8')
                    f.write(line + '\n')
                    current_file_size += len(line) + 1  # Account for newline character
                    current_line_count += 1

            # If the JSONL file line count exceeds the maximum, write the data to a Parquet file and clear the JSONL file
            if current_line_count >= max_json_lines:
                parquet_file_path = directory / f"{Path(base_path).stem}_{parquet_file_index}.parquet"
                with open(base_path, 'r', encoding='utf-8') as jsonl_file:
                    table = pa.Table.from_pandas(pd.read_json(jsonl_file, lines=True))
                pq.write_table(table, parquet_file_path, compression='snappy')
                os.chmod(parquet_file_path, 0o777)
                parquet_file_index += 1
                with open(base_path, 'w', encoding='utf-8') as jsonl_file:
                    jsonl_file.truncate(0)
                current_file_size = 0
                current_line_count = 0

        os.chmod(base_path, 0o777)

    except Exception as e:
        print(f'[write_jsonl] exception {e}')

def read_jsonl(base_path: str, column_name: str = None) -> list[dict]:
    """
    Read data from JSONL and Parquet files in the specified base path.
    If column_name is provided, only that column will be read from the Parquet files.
    """
    directory = Path(base_path).parent
    result = []

    # Read data from JSONL file
    start_time = time.time()
    if os.path.exists(base_path):
        with open(base_path, 'r', encoding='utf-8') as f:
            for line in f:
                row = json.loads(line.strip())
                if column_name:
                    result.append(row[column_name])
                else:
                    result.append(row)
    jsonl_read_time = time.time() - start_time
    print(f"JSONL file read time: {jsonl_read_time:.2f} seconds")

    # Read data from Parquet files
    existing_parquet_files = sorted(glob.glob(os.path.join(directory, f"{Path(base_path).stem}_*.parquet")))
    for parquet_file in existing_parquet_files:
        start_time = time.time()
        if column_name:
            table = pq.read_table(parquet_file, columns=[column_name])
            result.extend([row[column_name] for row in table.to_pandas().to_dict(orient='records')])
        else:
            table = pq.read_table(parquet_file)
            result.extend(table.to_pandas().to_dict(orient='records'))
        parquet_read_time = time.time() - start_time
        print(f"Parquet file read time: {parquet_read_time:.2f} seconds")

    return result

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
def read_file(path: str) -> list[str]:
    #print(f'[read_file] {path}')
    lines: list[str] = []
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