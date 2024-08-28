#%%
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

################################################################################
# json
################################################################################
# write json
def write_json(obj:object, path: str) -> None:
    #print(f'[write_json] {path}')
    try:
		# check if directory exists, if not, create it
        directory = Path(path).parent
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

################################################################################
# jsonl
################################################################################
# write jsonl
def write_jsonl(list_dict:list[object], path: str) -> None:
    #print(f'[write_jsonl] {path}')
    try:
		# check if directory exists, if not, create it
        directory = Path(path).parent
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
def read_jsonl(path: str) -> list[dict]:
    #print(f'[read_jsonl] {path}')
    lines: list[dict] = []
    try:
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
    except Exception as e:
        print(f'[read_jsonl] exception {e}')
    return [json.loads(line) for line in lines]

################################################################################
# jsonl cache (rolling files, combo of jsonl and parquets)
################################################################################
def write_to_jsonl_cache(list_dict: list[dict], base_path: str, max_json_lines: int = 100000) -> None:
    """
    Write a list of dictionaries to a JSONL file, and when the file size exceeds the specified maximum number of lines,
    write the data to a Parquet file and clear the JSONL file.
    """
    try:
        # Check if directory exists, if not, create it
        directory = Path(base_path).parent
        if not directory.exists():
            directory.mkdir(parents=True, exist_ok=True)
            directory.chmod(0o777)

        with lock:
            # count lines
            current_line_count = 0
            with open(base_path, 'r', encoding='utf-8') as f:
                current_line_count = sum(1 for _ in f)

            # write
            with open(base_path, 'a', encoding='utf-8') as f:
                for d in list_dict:
                    line = json.dumps(d, ensure_ascii=False).encode('utf-8').decode('utf-8')
                    f.write(line + '\n')
                    current_line_count += 1

            # if JSONL line count exceeds the maximum, write the data to a Parquet file and clear the JSONL file
            if current_line_count >= max_json_lines:
                # figure out correct suffix parquet_file_index
                parquet_file_index = 0
                existing_parquet_files = sorted(glob.glob(os.path.join(directory, f"{Path(base_path).stem}_*.parquet")))
                if existing_parquet_files:
                    parquet_file_index = max([int(Path(f).stem.split("_")[1]) for f in existing_parquet_files]) + 1

                # write compressed parquet
                parquet_file_path = directory / f"{Path(base_path).stem}_{parquet_file_index}.parquet"
                df = pd_read_jsonl(base_path)
                pd_save_parquet(df,parquet_file_path)
                os.chmod(parquet_file_path, 0o777)

                # truncate
                with open(base_path, 'w', encoding='utf-8') as jsonl_file: 
                    jsonl_file.truncate(0)

        os.chmod(base_path, 0o777)

    except Exception as e:
        print(f'[write_to_jsonl_cache] exception {e}')

def read_from_jsonl_cache(base_path: str, column_name: str = None) -> list[dict]:
    """
    Read data from JSONL and Parquet files in the specified base path.
    If column_name is provided, only that column will be read from the Parquet files.
    """
    result = []
    try:
        directory = Path(base_path).parent

        # Read data from JSONL file
        if os.path.exists(base_path):
            start_time = time.time()
            df = pd_read_jsonl(base_path)
            if column_name:
                result.extend([x[column_name] for x in df.to_dict(orient='records')])
            else:
                result.extend(df.to_dict(orient='records'))
            jsonl_read_time = time.time() - start_time
            print(f"[read jsonl] {base_path} {jsonl_read_time:.2f}, records = {len(df)}")

        # Read data from Parquet files
        existing_parquet_files = sorted(glob.glob(os.path.join(directory, f"{Path(base_path).stem}_*.parquet")))

        for parquet_file in existing_parquet_files:
            start_time = time.time()
            if column_name:
                df = pd_read_parquet(parquet_file,[column_name])
                result.extend([x[column_name] for x in df.to_dict(orient='records')])
            else:
                df = pd_read_parquet(parquet_file)
                result.extend(df.to_dict(orient='records'))
            parquet_read_time = time.time() - start_time
            print(f"[read parquet] {parquet_file} {parquet_read_time:.2f} seconds, records = {len(df)}")
    except Exception as e:
        print(f'[read_from_jsonl_cache] exception {e}')
    
    return result

################################################################################
# file
################################################################################
# write file
def write_file(obj:object, path: str) -> None:
    #print(f'[write_file] {path}')
    try:
		# check if directory exists, if not, create it
        directory = Path(path).parent
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
    
################################################################################
# gzip
################################################################################
# write gzip
def write_gzip(content:object, path: str) -> None:
    #print(f'[write_gzip] {path}')
    try:
		# check if directory exists, if not, create it
        directory = Path(path).parent
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
'''
path = '/home/alfred/nfs/cc_zho/records/2024-33/com,maishirts,uninked/TU5MWJMVY22O6T2CYADNFUFQCRZ6WLU2.gz'
read_gzip(path)
'''
#%%
################################################################################
# pandas
################################################################################
# read jsonl
def pd_read_jsonl(filepath: str) -> pd.DataFrame:
    return pd.read_json(filepath, lines=True)
'''
pd.read_json('~/nfs/cc_zho/extracts/2023-40/abc.jsonl', lines=True)
'''

# save parquet
def pd_save_parquet(df: pd.DataFrame, filepath: str) -> None:
    #df.to_parquet(filepath, compression='snappy') # snappy
    #df.to_parquet(filepath, compression='gzip')  # gzip
    #df.to_parquet(filepath, compression='lz4')   # lz4
    #df.to_parquet(filepath, compression='zstd')  # zstd
    df.to_parquet(filepath, compression='brotli') # brotli
'''
df.to_parquet('~/nfs/cc_zho/extracts/2023-40/abc_snappy.parquet', compression='snappy') # snappy
df.to_parquet('~/nfs/cc_zho/extracts/2023-40/abc_gzip.parquet', compression='gzip')  # gzip
df.to_parquet('~/nfs/cc_zho/extracts/2023-40/abc_lz4.parquet', compression='lz4')   # lz4
df.to_parquet('~/nfs/cc_zho/extracts/2023-40/abc_zstd.parquet', compression='zstd')  # zstd
df.to_parquet('~/nfs/cc_zho/extracts/2023-40/abc_brotli.parquet', compression='brotli') # brotli
'''

# read parquet
def pd_read_parquet(file_path: str, columns: list = None) -> pd.DataFrame:
    try:
        df = pd.read_parquet(file_path, columns=columns)
        return df
    except Exception as e:
        print(f"Error reading the Parquet file: {e}")
        return None
#%%
'''
pd_read_parquet('~/nfs/cc_zho/extracts/2023-40/2023-40_0.parquet')

#%%
%%time
print(len(pd_read_parquet('~/nfs/cc_zho/extracts/2023-40/abc_snappy.parquet',['filepath'])))

pd_read_parquet('~/nfs/cc_zho/extracts/2023-40/abc_snappy.parquet',['filepath']).to_dict(orient='records')
df.to_dict(orient='records')

#%%
%%time
print(len(pd_read_parquet('~/nfs/cc_zho/extracts/2023-40/abc_snappy.parquet')))
#%%
# print(len(pd_read_parquet('~/nfs/cc_zho/extracts/2023-40/abc_gzip.parquet')))
print(len(pd_read_parquet('~/nfs/cc_zho/extracts/2023-40/abc_lz4.parquet')))
print(len(pd_read_parquet('~/nfs/cc_zho/extracts/2023-40/abc_zstd.parquet')))
print(len(pd_read_parquet('~/nfs/cc_zho/extracts/2023-40/abc_brotli.parquet')))
#%%
'''