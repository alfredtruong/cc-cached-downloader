#%%
import os
import json
import gzip
from pathlib import Path
import threading
import cchardet as chardet # pip install faust-cchardet
#import chardet
import pandas as pd
import glob
import time

lock = threading.Lock()

################################################################################
# json
################################################################################
# write json
def write_json(obj:object, filepath: str) -> None:
    #print(f'[write_json] {filepath}')
    try:
		# check if directory exists, if not create it
        indexdir = Path(filepath).parent
        if not indexdir.exists():
            indexdir.mkdir(parents=True, exist_ok=True) # name subdir
            indexdir.chmod(0o777) # make accessible

        with lock:
            # write new file
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(json.dumps(obj))

            # make accessible
            os.chmod(filepath, 0o777)

    except Exception as e:
	    print(f'[write_json] exception {e}')

# read json
def read_json(filepath: str) -> list:
    #print(f'[read_json] {filepath}')
    res = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            res = json.load(f)

    except Exception as e:
        print(f'[read_json] exception {e}')

    return res

################################################################################
# jsonl
################################################################################
# truncate jsonl (use within a lock)
def truncate_jsonl(filepath: str) -> None:
    with open(filepath, 'w', encoding='utf-8') as f: 
        f.truncate(0)

# write jsonl
def write_jsonl(listdict:list[object], filepath: str, truncate: bool = False) -> None:
    #print(f'[write_jsonl] {filepath}')
    try:
		# check if directory exists, if not create it
        indexdir = Path(filepath).parent
        if not indexdir.exists():
            indexdir.mkdir(parents=True, exist_ok=True) # name subdir
            indexdir.chmod(0o777) # make accessible

        with lock:
            # should truncate first or not?
            if truncate:
                truncate_jsonl(filepath)

            # append
            with open(filepath, 'a', encoding='utf-8') as f:
                # write list dicts to jsonl
                for d in listdict:
                    line = json.dumps(d, ensure_ascii=False) # build line
                    f.write(line + '\n') # write line

            # make accessible
            os.chmod(filepath, 0o777)

    except Exception as e:
	    print(f'[write_jsonl] exception {e}')

# read jsonl
def read_jsonl(filepath: str) -> list[dict]:
    #print(f'[read_jsonl] {filepath}')
    lines: list[dict] = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()

    except Exception as e:
        print(f'[read_jsonl] exception {e}')
    return [json.loads(line) for line in lines]

################################################################################
# jsonl cache tools
################################################################################
def indexdir_to_jsonl(indexdir: Path, prefix: str) -> Path:
    return indexdir / f'{prefix}.jsonl'

def next_parquet_filepath(indexdir: Path, index: str) -> Path:
    # get correct index for next parquet
    parquet_file_index = 0
    parquet_files = sorted(glob.glob(str(indexdir / f"{index}_*.parquet")))
    if parquet_files:
        parquet_file_index = max([int(Path(f).stem.split("_")[1]) for f in parquet_files]) + 1

    # return new parquet filepath
    return indexdir / f"{index}_{parquet_file_index}.parquet"

def list_parquet_files(indexdir: Path) -> list[str]:
    parquet_files: list[str] = [] # default
    try:
        # Sort files based on the numeric part of the filename
        parquet_files = glob.glob(str(indexdir / f"{indexdir.name}_*.parquet"))
        parquet_files.sort(key=lambda x: int(Path(x).stem.split('_')[-1]))

    except Exception as e:
        print(f'[list_parquet_files] exception {e}')

    # return list of all parquets
    return parquet_files

def safe_div(a,b):
    return 0 if b == 0 else a/b

################################################################################
# jsonl cache (= combo of jsonl and parquets)
################################################################################
def write_cache(listdict: list[dict], indexdir: str, max_json_lines: int = 10000) -> None:
    """
    Write a list of dictionaries to a JSONL file, and when the file size exceeds the specified maximum number of lines,
    write the data to a Parquet file and clear the JSONL file.
    """
    try:
		# check if directory exists, if not create it
        with lock:
            indexdir = Path(indexdir)
            if not indexdir.exists():
                indexdir.mkdir(parents=True, exist_ok=True) # name subdir
                indexdir.chmod(0o777) # make accessible

            # make jsonl if doesnt exist
            filepath = indexdir_to_jsonl(indexdir, indexdir.name)
            if not filepath.exists():
                filepath.touch()
                os.chmod(filepath, 0o777)
                
            # count lines
            line_count = 0
            with open(filepath, 'r', encoding='utf-8') as f:
                line_count = sum(1 for _ in f)

            # write
            with open(filepath, 'a', encoding='utf-8') as f:
                for d in listdict:
                    line = json.dumps(d, ensure_ascii=False).encode('utf-8').decode('utf-8')
                    f.write(line + '\n')
                    line_count += 1

            # if JSONL line count exceeds the maximum, write the data to a Parquet file and clear the JSONL file
            if line_count >= max_json_lines:
                # get contents
                df = pd_read_jsonl(filepath)

                # write compressed parquet
                parquet_filepath = next_parquet_filepath(indexdir,indexdir.name)
                pd_save_parquet(parquet_filepath,df)

                # truncate jsonl
                truncate_jsonl(filepath)

    except Exception as e:
        print(f'[write_cache] exception {e}')

def read_cache(indexdir: str, column_name: str = None) -> list[dict]:
    """
    Read data from Parquet + JSONL files in the specified filepath.
    If column_name is provided, only that column will be read from the Parquet files.
    """
    result = []
    total_emptys = 0
    try:
        indexdir = Path(indexdir)
        # read parquets
        parquet_files = list_parquet_files(indexdir)
        for parquet_file in parquet_files:
            start_time = time.time()
            if column_name:
                df = pd_read_parquet(parquet_file,[column_name])
                result.extend([x[column_name] for x in df.to_dict(orient='records')])
            else:
                df = pd_read_parquet(parquet_file)
                result.extend(df.to_dict(orient='records'))
            parquet_read_time = time.time() - start_time

            # summarize parquet
            emptys = sum(df['content'].str.len() == 0)
            total_emptys += emptys
            print(f"[read_cache][read parquet] {parquet_file} {parquet_read_time:.2f}s, records = {len(df)}, emptys = {emptys}, empty_ratio = {safe_div(emptys,len(df)):.4f}")

        # read jsonl
        filepath = indexdir_to_jsonl(indexdir, indexdir.name)
        if os.path.exists(filepath):
            start_time = time.time()
            df = pd_read_jsonl(filepath)
            if column_name:
                result.extend([x[column_name] for x in df.to_dict(orient='records')])
            else:
                result.extend(df.to_dict(orient='records'))
            jsonl_read_time = time.time() - start_time

            # summarize jsonl
            emptys = sum(df['content'].str.len() == 0)
            total_emptys += emptys
            print(f"[read_cache][read jsonl] {filepath} {jsonl_read_time:.2f}s, records = {len(df)}, emptys = {emptys}, empty_ratio = {safe_div(emptys,len(df)):.4f}")

    except Exception as e:
        print(f'[read_cache] exception {e}')

    # summarize cache
    print(f"[read_cache] {indexdir} total records = {len(result)}, emptys = {total_emptys}, empty_ratio = {safe_div(total_emptys,len(result))}")

    return result
'''
# batch check old vs new data
for index in ALL_INDEXES:
    try:
        df1=read_cache(f'/home/alfred/nfs/cc_zho/extracts/{index}/') # old
        df2=read_cache(f'/home/alfred/nfs/cc_zho/extracts_redump/{index}/') # new
        print(f'[{index}] df1 = {len(df1)}, df2 = {len(df2)}, diff = {len(df1)-len(df2)}')
    except Exception as e:
        print(e)

'''
'''
# batch check all indexes
for index in ALL_INDEXES:
    read_cache(f'/home/alfred/nfs/cc_zho/extracts/{index}/') # actual
'''
'''
# check single index
c1=read_cache('/home/alfred/nfs/cc_zho/extracts/2019-22/')
c1=read_cache('/home/alfred/nfs/cc_zho/extracts/2019-18/')


c1=read_cache('/home/alfred/nfs/cc_zho/extracts/2019-43/') # done
c2=read_cache('/home/alfred/nfs/cc_zho/extracts_redump/2019-43/') # done

read_cache('/home/alfred/nfs/cc_zho/extracts/2023-40/'); # done
read_cache('/home/alfred/nfs/cc_zho/extracts_TRASH/2023-40/'); # done

# strip jsonl
strip_jsonl('/home/alfred/nfs/cc_zho/extracts_TRASH/2023-40/2023-40.jsonl')

# batch strip jsonl
for index in BATCH_3[1:]:
    #strip_jsonl(f'/home/alfred/nfs/cc_zho/extracts_TRASH/{index}/{index}.jsonl') # testing
    strip_jsonl(f'/home/alfred/nfs/cc_zho/extracts/{index}/{index}.jsonl') # actual

# batch check jsonl
for index in BATCH_3:
    #read_cache(f'/home/alfred/nfs/cc_zho/extracts_TRASH/{index}/') # testing
    read_cache(f'/home/alfred/nfs/cc_zho/extracts/{index}/') # actual

# batch verify nothing lost
for index in BATCH_2:
    print(index)
    listdict1 = read_cache(f'/home/alfred/nfs/cc_zho/extracts/{index}/')
    listdict2 = read_cache(f'/home/alfred/nfs/cc_zho/extracts/{index}/{index}/')
    print(f'!!!!!!!!!!!!!!!!!!!! ======= {len(listdict1)-len(listdict1)}')

# batch move old to extracts_TRASH
for index in BATCH_2[1:]:
    os.system(f'mv /home/alfred/nfs/cc_zho/extracts/{index} /home/alfred/nfs/cc_zho/extracts_TRASH/')
    os.system(f'mv /home/alfred/nfs/cc_zho/extracts_TRASH/{index}/{index} /home/alfred/nfs/cc_zho/extracts/')
'''

'''
# move parquet contents to jsonl
strip_parquet('/home/alfred/nfs/cc_zho/extracts/2023-50/2023-50_0.parquet')
df = pd_read_parquet('/home/alfred/nfs/cc_zho/extracts/2023-50/2023-50_0.parquet')
write_jsonl(df.to_dict('records'),'/home/alfred/nfs/cc_zho/extracts/2023-50/2023-50.jsonl')

# make sure nothing lost
df1 = pd_read_parquet('/home/alfred/nfs/cc_zho/extracts/2023-50/2023-50_0.parquet')
df2 = pd_read_jsonl('/home/alfred/nfs/cc_zho/extracts/2023-50/2023-50.jsonl')
len(df1)-len(df2)
'''

################################################################################
# file
################################################################################
# write file
def write_file(obj:object, filepath: str) -> None:
    #print(f'[write_file] {filepath}')
    try:
		# check if directory exists, if not create it
        indexdir = Path(filepath).parent
        if not indexdir.exists():
            indexdir.mkdir(parents=True, exist_ok=True) # name subdir
            indexdir.chmod(0o777) # make accessible

        with lock:
            with open(filepath, 'w', encoding='utf-8') as f:
                if obj is None:
                    f.write('')
                else:
                    f.write(str(obj))

                # make accessible
                os.chmod(filepath, 0o777)

    except Exception as e:
	    print(f'[write_file] exception {e}')
    
# read file
def read_file(filepath: str) -> list[str]:
    #print(f'[read_file] {filepath}')
    lines: list[str] = []
    try:
        if os.path.exists(filepath):
            with open(filepath, 'r', encoding='utf-8') as f:
                lines = f.readlines()

    except Exception as e:
        print(f'[read_file] exception {e}')

    return lines
    
################################################################################
# gzip
################################################################################
# write gzip
def write_gzip(content:object, filepath: str) -> None:
    #print(f'[write_gzip] {filepath}')
    try:
		# check if directory exists, if not create it
        indexdir = Path(filepath).parent
        if not indexdir.exists():
            indexdir.mkdir(parents=True, exist_ok=True) # name subdir
            indexdir.chmod(0o777) # make accessible

        with open(filepath, "wb") as f:
            f.write(content)

            # make accessible
            os.chmod(filepath, 0o777)

    except Exception as e:
	    print(f'[write_gzip] exception {e}')
        
# read gzip
def read_gzip(filepath: str, debug: bool = False) -> str:
    #print(f'[read_gzip] {filepath}')
    raw_content: str = ''
    try:
        if os.path.exists(filepath):
            # read contents
            with gzip.open(filepath, 'rb') as f:
                content = f.read()

            # for manual debugging
            if debug:
                print(content)

            # try common encodings first
            for encoding in ['UTF-8	','GB18030']: #  'gbk', 'big5',
                try:
                    raw_content = content.decode(encoding, errors='ignore')
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
read_gzip('/home/alfred/nfs/cc_zho_2/records/2024-10/com,naturallyhealthierways/F5FP46JBQ27F6OCZ62DNI3FRAAEPHPTG.gz')
'''

def strip_jsonl(filepath: str, target_location: str = None) -> None:
    '''
    remove lines with no content
    '''
    filepath = Path(filepath)
    df = pd_read_jsonl(filepath) # read jsonl
    if len(df) == 0: return
    df = df[df['content'].str.len() > 0] # strip rows with no content
    if target_location:
        # write to requested location
        try:
            print(f'[strip_jsonl][custom path] writing to {target_location}')
            write_jsonl(df.to_dict('records'),target_location,True) # overwrites / truncates existing file

        except Exception as e:
            print(f'[strip_jsonl][custom path] error')
    else:
        filepath_tmp = filepath.parent / f"{filepath.name}.tmp" # where to write stripped jsonl
        filepath_bck = filepath.parent / f"{filepath.name}.bck" # where to move existing file
        try:
            print(f'[strip_jsonl][overwrite] writing to {filepath_tmp}')
            write_jsonl(df.to_dict('records'),filepath_tmp,True) # write temp file
            os.system(f'mv {filepath} {filepath_bck}') # keep existing file
            os.system(f'mv {filepath_tmp} {filepath}') # move written file over

        except Exception as e:
            print(f'[strip_jsonl][overwrite] error with {filepath_tmp}')
'''
filepath='/home/alfred/nfs/cc_zho/extracts/2023-40/2023-40.jsonl'
strip_jsonl('/home/alfred/nfs/cc_zho/extracts/2024-33/2024-33.jsonl')


strip_jsonl('/home/alfred/nfs/cc_zho/extracts/2019-43/2019-43.jsonl_duplicated3')
strip_jsonl('/home/alfred/nfs/cc_zho/extracts/2019-43/2019-43.jsonl_duplicated3','/home/alfred/nfs/cc_zho/extracts/2019-43/2019-43.jsonl_duplicated4')

filepath='/home/alfred/nfs/cc_zho/extracts/2019-43/2019-43b.jsonl'
strip_jsonl('/home/alfred/nfs/cc_zho/extracts/2019-43/2019-43b.jsonl') # strip and keep backup
strip_jsonl('/home/alfred/nfs/cc_zho/extracts/2019-43/2019-43b.jsonl','/home/alfred/nfs/cc_zho/extracts/2019-43/2019-43c.jsonl') # strip and write somewhere else
'''

def strip_parquet(filepath: str, target_location: str = None) -> None:
    '''
    remove rows with no content
    '''
    filepath = Path(filepath)
    df = pd_read_parquet(filepath) # read parquet
    if len(df) == 0: return
    df = df[df['content'].str.len() > 0] # strip rows with no content
    if target_location:
        # write to requested location
        try:
            print(f'[strip_parquet][custom path] writing to {target_location}')
            pd_save_parquet(target_location,df)

        except Exception as e:
            print(f'[strip_parquet][custom path] error with {target_location}')
    else:
        filepath_tmp = filepath.parent / f"{filepath.name}.tmp" # where to write stripped jsonl
        filepath_bck = filepath.parent / f"{filepath.name}.bck" # where to move existing file
        try:
            print(f'[strip_parquet][overwrite] writing to {filepath_tmp}')
            pd_save_parquet(filepath_tmp,df) # write temp file
            os.system(f'mv {filepath} {filepath_bck}') # keep existing file
            os.system(f'mv {filepath_tmp} {filepath}') # move written file over

        except Exception as e:
            print(f'[strip_parquet][overwrite] error with {filepath_tmp}')
'''
strip_parquet('/home/alfred/nfs/cc_zho/extracts/2024-33/2024-33_0.parquet')
strip_parquet('/home/alfred/nfs/cc_zho/extracts/2024-33/2024-33_1.parquet')
strip_parquet('/home/alfred/nfs/cc_zho/extracts/2024-33/2024-33_2.parquet')
strip_parquet('/home/alfred/nfs/cc_zho/extracts/2024-33/2024-33_3.parquet')
strip_parquet('/home/alfred/nfs/cc_zho/extracts/2024-33/2024-33_4.parquet')
strip_parquet('/home/alfred/nfs/cc_zho/extracts/2024-33/2024-33_5.parquet')
strip_parquet('/home/alfred/nfs/cc_zho/extracts/2024-33/2024-33_6.parquet')
strip_parquet('/home/alfred/nfs/cc_zho/extracts/2024-33/2024-33_7.parquet')
strip_parquet('/home/alfred/nfs/cc_zho/extracts/2024-33/2024-33_8.parquet')
strip_parquet('/home/alfred/nfs/cc_zho/extracts/2024-33/2024-33_9.parquet')
strip_parquet('/home/alfred/nfs/cc_zho/extracts/2024-33/2024-33_10.parquet')
strip_parquet('/home/alfred/nfs/cc_zho/extracts/2024-33/2024-33_11.parquet')
strip_parquet('/home/alfred/nfs/cc_zho/extracts/2024-33/2024-33_12.parquet')
'''

def strip_cache(indexdir: str) -> None:
    '''
    remove entries from jsonl and parquets where there is no content
    '''
    try:
        # strip parquet files
        indexdir = Path(indexdir)
        parquet_files = list_parquet_files(indexdir)
        for parquet_file in parquet_files:
            strip_parquet(parquet_file)

        # strip JSONL file
        filepath = indexdir_to_jsonl(indexdir, indexdir.name)
        if os.path.exists(filepath):
            strip_jsonl(filepath)

    except Exception as e:
        print(f'[strip_cache] exception {e}')
'''
strip_cache('/home/alfred/nfs/cc_zho/extracts/2019-22/')
strip_cache('/home/alfred/nfs/cc_zho/extracts/2019-18/')
strip_cache('/home/alfred/nfs/cc_zho/extracts/2020-05/')

for index in BATCH_2:
    strip_cache(f'/home/alfred/nfs/cc_zho/extracts/{index}/')
'''

def redump_cache(indexdir: str, max_json_lines: int = 10000) -> None:
    '''
    read all jsonl and parquet files then redump new files that satisfy `max_json_lines`, i.e. apply after `strip_cache`
    '''
    try:
        # read cache
        #strip_cache(indexdir)
        listdict = read_cache(indexdir)

		# check if directory exists, if not create it
        indexdir = Path(str(indexdir).replace('/extracts/','/extracts_redump/')) # save to new directory
        if not indexdir.exists():
            indexdir.mkdir(parents=True, exist_ok=True) # name subdir
            indexdir.chmod(0o777) # make accessible

        # write new parquets
        current_line = 0
        while current_line < len(listdict):
            if (current_line + max_json_lines) < len(listdict):
                # write compressed parquet
                parquet_filepath = next_parquet_filepath(indexdir,indexdir.name)
                pd_save_parquet(parquet_filepath,pd.DataFrame.from_records(listdict[current_line:current_line + max_json_lines]))
                current_line += max_json_lines
                print(f'[redump_cache] current_line = {current_line},  fp = {parquet_filepath}')
                os.chmod(parquet_filepath, 0o777) # make accessible
            else:
                # write jsonl
                filepath = indexdir_to_jsonl(indexdir, indexdir.name)
                with open(filepath, 'w', encoding='utf-8') as f:
                    for d in listdict[current_line:]:
                        line = json.dumps(d, ensure_ascii=False).encode('utf-8').decode('utf-8')
                        f.write(line + '\n')
                        current_line += 1
                print(f'[redump_cache] current_line = {current_line},  fp = {filepath}')
                os.chmod(filepath, 0o777) # make accessible

    except Exception as e:
        print(f'[redump_cache] exception {e}')
'''
redump_cache('/home/alfred/nfs/cc_zho/extracts/2019-22/')
redump_cache('/home/alfred/nfs/cc_zho/extracts/2019-18/')

for index in ALL_INDEXES:
    redump_cache(f'/home/alfred/nfs/cc_zho/extracts/{index}/')
'''

################################################################################
# pandas
################################################################################
# read jsonl
def pd_read_jsonl(filepath: str) -> pd.DataFrame:
    return pd.read_json(filepath, lines=True)
'''
pd.read_json('/home/alfred/nfs/cc_zho/extracts/2023-40/abc.jsonl', lines=True)
'''

# save parquet
def pd_save_parquet(filepath: str, df: pd.DataFrame) -> None:
    try:
        df.to_parquet(filepath,compression='snappy') # snappy
        #df.to_parquet(filepath,compression='gzip')  # gzip
        #df.to_parquet(filepath,compression='lz4')   # lz4
        #df.to_parquet(filepath,compression='zstd')  # zstd
        #df.to_parquet(filepath,compression='brotli') # brotli
        os.chmod(filepath, 0o777) # make accessible

    except Exception as e:
        print(f'[pd_save_parquet] exception {e}')

# read parquet
def pd_read_parquet(filepath: str, columns: list = None) -> pd.DataFrame:
    try:
        df = pd.read_parquet(filepath, columns=columns)
        return df
    except Exception as e:
        print(f"Error reading the Parquet file: {e}")
'''
df = pd_read_parquet('/home/alfred/nfs/cc_zho/extracts/2019-43/2019-43_1.parquet')
'''

if __name__ == "__main__":
    ALL_INDEXES = [os.path.basename(index) for index in sorted(glob.glob('/home/alfred/nfs/cc_zho/extracts/*'))] # list of extracted indexes

    BATCH_1 = ['2024-33','2024-30','2024-26','2024-22','2024-18','2024-10','2023-50','2023-40']
    BATCH_2 = ['2018-51','2019-04','2019-18','2019-22','2019-35','2019-39','2019-43','2019-47','2022-21','2022-27','2022-33','2022-40']
    BATCH_3 = ['2018-51','2019-04','2019-18','2019-22','2019-35','2019-39','2019-43','2019-47','2022-21','2022-27','2022-33','2022-40','2023-40','2023-50','2024-10','2024-18','2024-22','2024-26','2024-30','2024-33']

    # check if redumped data is the same as original data
    if False:
        for index in ALL_INDEXES:
            if index not in [
                '2018-39','2018-43','2018-47','2018-51','2019-04','2019-09','2019-13','2019-18','2019-22','2019-26','2019-30',
                '2019-35','2019-39','2019-43','2019-47','2019-51','2020-05','2020-10','2020-16','2020-24'
            ]:
                print(index)
                try:
                    df1=read_cache(f'/home/alfred/nfs/cc_zho/extracts/{index}/') # old
                    df2=read_cache(f'/home/alfred/nfs/cc_zho/extracts_redump/{index}/') # new
                    print(f'[{index}] df1 = {len(df1)}, df2 = {len(df2)}, diff = {len(df1)-len(df2)}')
                except Exception as e:
                    print(e)

    # check contents of original directories that are missing in the redump
    if False:
        for index in ['2020-05','2020-16','2020-40','2023-23','2024-33']:
            df1=read_cache(f'/home/alfred/nfs/cc_zho/extracts/{index}/')

    # fix missing redumps
    if False:
        for index in ['2020-05','2020-16','2020-40','2024-33','2023-23']:
            redump_cache(f'/home/alfred/nfs/cc_zho/extracts/{index}/')

    # check if redumped data is the same as original data
    if False:
        for index in ['2020-05','2020-16','2020-40','2024-33','2023-23']:
            print(index)
            try:
                df1=read_cache(f'/home/alfred/nfs/cc_zho/extracts/{index}/') # old
                df2=read_cache(f'/home/alfred/nfs/cc_zho/extracts_redump/{index}/') # new
                print(f'[{index}] df1 = {len(df1)}, df2 = {len(df2)}, diff = {len(df1)-len(df2)}')
            except Exception as e:
                print(e)

#%%
