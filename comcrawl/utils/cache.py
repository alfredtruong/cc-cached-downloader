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
def write_json(obj:object, json_fp: str) -> None:
    #print(f'[write_json] {json_fp}')
    try:
		# check if directory exists, if not create it
        indexdir = Path(json_fp).parent
        if not indexdir.exists():
            indexdir.mkdir(parents=True, exist_ok=True) # create subdir
            indexdir.chmod(0o777) # make accessible

        with lock:
            # write new file
            with open(json_fp, 'w', encoding='utf-8') as f:
                f.write(json.dumps(obj))

            # make accessible
            os.chmod(json_fp, 0o777)

    except Exception as e:
	    print(f'[write_json] exception {e}')

# read json
def read_json(json_fp: str) -> list:
    #print(f'[read_json] {json_fp}')
    res = []
    try:
        with open(json_fp, 'r', encoding='utf-8') as f:
            res = json.load(f)

    except Exception as e:
        print(f'[read_json] exception {e}')

    return res

################################################################################
# jsonl
################################################################################
# truncate jsonl (use within a lock)
def truncate_jsonl(jsonl_fp: str) -> None:
    with open(jsonl_fp, 'w', encoding='utf-8') as f: 
        f.truncate(0)

# write jsonl
def write_jsonl(listdict:list[object], jsonl_fp: str, truncate: bool = False) -> None:
    #print(f'[write_jsonl] {jsonl_fp}')
    try:
		# check if directory exists, if not create it
        indexdir = Path(jsonl_fp).parent
        if not indexdir.exists():
            indexdir.mkdir(parents=True, exist_ok=True) # create subdir
            indexdir.chmod(0o777) # make accessible

        with lock:
            # should truncate first or not?
            if truncate:
                truncate_jsonl(jsonl_fp)

            # append
            with open(jsonl_fp, 'a', encoding='utf-8') as f:
                # write list dicts to jsonl
                for d in listdict:
                    line = json.dumps(d, ensure_ascii=False) # build line
                    f.write(line + '\n') # write line

            # make accessible
            os.chmod(jsonl_fp, 0o777)

    except Exception as e:
	    print(f'[write_jsonl] exception {e}')

# read jsonl
def read_jsonl(jsonl_fp: str) -> list[dict]:
    #print(f'[read_jsonl] {jsonl_fp}')
    lines: list[dict] = []
    try:
        with open(jsonl_fp, 'r', encoding='utf-8') as f:
            lines = f.readlines()

    except Exception as e:
        print(f'[read_jsonl] exception {e}')
    return [json.loads(line) for line in lines]

################################################################################
# jsonl cache tools
################################################################################
def indexdir_to_jsonl(indexdir: Path, prefix: str) -> Path:
    return indexdir / f'{prefix}.jsonl'

def indexdir_to_empties(indexdir: Path, prefix: str) -> Path:
    return indexdir / f'{prefix}.txt'

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
            # ensure directory exists
            indexdir = Path(indexdir)
            if not indexdir.exists():
                indexdir.mkdir(parents=True, exist_ok=True) # create subdir
                indexdir.chmod(0o777) # make accessible

            # ensure jsonl exist
            jsonl_fp = indexdir_to_jsonl(indexdir, indexdir.name)
            if not jsonl_fp.exists():
                jsonl_fp.touch()
                os.chmod(jsonl_fp, 0o777)

            # ensure txt for empty links exist
            empties_fp = indexdir_to_empties(indexdir, indexdir.name)
            if not empties_fp.exists():
                empties_fp.touch()
                os.chmod(empties_fp, 0o777)

            # count lines
            line_count = 0
            with open(jsonl_fp, 'r', encoding='utf-8') as f:
                line_count = sum(1 for _ in f)

            # write entries and filter for empties
            empty_entries = []
            with open(jsonl_fp, 'a', encoding='utf-8') as f:
                for d in listdict:
                    if len(d['content']) == 0:
                        empty_entries.append(d['filepath'])
                    else:
                        line = json.dumps(d, ensure_ascii=False).encode('utf-8').decode('utf-8')
                        f.write(line + '\n')
                        line_count += 1

            # write empty entries to empties_fp
            if empty_entries:
                with open(empties_fp, 'a', encoding='utf-8') as f:
                    for fp in empty_entries:
                        f.write(fp + '\n')

            # if JSONL line count exceeds the maximum, write the data to a Parquet file and clear the JSONL file
            if line_count >= max_json_lines:
                # get contents
                df = pd_read_jsonl(jsonl_fp)

                # write compressed parquet
                parquet_filepath = next_parquet_filepath(indexdir,indexdir.name)
                pd_save_parquet(parquet_filepath,df)

                # truncate jsonl
                truncate_jsonl(jsonl_fp)

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
        jsonl_fp = indexdir_to_jsonl(indexdir, indexdir.name)
        if os.path.exists(jsonl_fp):
            start_time = time.time()
            df = pd_read_jsonl(jsonl_fp)
            if column_name:
                result.extend([x[column_name] for x in df.to_dict(orient='records')])
            else:
                result.extend(df.to_dict(orient='records'))
            jsonl_read_time = time.time() - start_time

            # summarize jsonl
            emptys = sum(df['content'].str.len() == 0)
            total_emptys += emptys
            print(f"[read_cache][read jsonl] {jsonl_fp} {jsonl_read_time:.2f}s, records = {len(df)}, emptys = {emptys}, empty_ratio = {safe_div(emptys,len(df)):.4f}")

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

################################################################################
# file
################################################################################
# write file
def write_file(obj:object, fp: str) -> None:
    #print(f'[write_file] {fp}')
    try:
		# check if directory exists, if not create it
        indexdir = Path(fp).parent
        if not indexdir.exists():
            indexdir.mkdir(parents=True, exist_ok=True) # create subdir
            indexdir.chmod(0o777) # make accessible

        with lock:
            with open(fp, 'w', encoding='utf-8') as f:
                if obj is None:
                    f.write('')
                else:
                    f.write(str(obj))

                # make accessible
                os.chmod(fp, 0o777)

    except Exception as e:
	    print(f'[write_file] exception {e}')
    
# read file
def read_file(fp: str) -> list[str]:
    #print(f'[read_file] {fp}')
    lines: list[str] = []
    try:
        if os.path.exists(fp):
            with open(fp, 'r', encoding='utf-8') as f:
                lines = f.readlines()

    except Exception as e:
        print(f'[read_file] exception {e}')

    return lines
    
################################################################################
# gzip
################################################################################
# write gzip
def write_gzip(content:object, gzip_fp: str) -> None:
    #print(f'[write_gzip] {gzip_fp}')
    try:
		# check if directory exists, if not create it
        indexdir = Path(gzip_fp).parent
        if not indexdir.exists():
            indexdir.mkdir(parents=True, exist_ok=True) # create subdir
            indexdir.chmod(0o777) # make accessible

        with open(gzip_fp, "wb") as f:
            f.write(content)

            # make accessible
            os.chmod(gzip_fp, 0o777)

    except Exception as e:
	    print(f'[write_gzip] exception {e}')
        
# read gzip
def read_gzip(gzip_fp: str, debug: bool = False) -> str:
    #print(f'[read_gzip] {gzip_fp}')
    raw_content: str = ''
    try:
        if os.path.exists(gzip_fp):
            # read contents
            with gzip.open(gzip_fp, 'rb') as f:
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

def strip_jsonl(jsonl_fp: str, target_location: str = None) -> None:
    '''
    remove lines with no content
    '''
    jsonl_fp = Path(jsonl_fp)
    df = pd_read_jsonl(jsonl_fp) # read jsonl
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
        filepath_tmp = jsonl_fp.parent / f"{jsonl_fp.name}.tmp" # where to write stripped jsonl
        filepath_bck = jsonl_fp.parent / f"{jsonl_fp.name}.bck" # where to move existing file
        try:
            print(f'[strip_jsonl][overwrite] writing to {filepath_tmp}')
            write_jsonl(df.to_dict('records'),filepath_tmp,True) # write temp file
            os.system(f'mv {jsonl_fp} {filepath_bck}') # keep existing file
            os.system(f'mv {filepath_tmp} {jsonl_fp}') # move written file over

        except Exception as e:
            print(f'[strip_jsonl][overwrite] error with {filepath_tmp}')
'''
strip_jsonl('/home/alfred/nfs/cc_zho/extracts/2024-33/2024-33.jsonl')
'''

def strip_parquet(parquet_fp: str, target_location: str = None) -> None:
    '''
    remove rows with no content
    '''
    parquet_fp = Path(parquet_fp)
    df = pd_read_parquet(parquet_fp) # read parquet
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
        filepath_tmp = parquet_fp.parent / f"{parquet_fp.name}.tmp" # where to write stripped jsonl
        filepath_bck = parquet_fp.parent / f"{parquet_fp.name}.bck" # where to move existing file
        try:
            print(f'[strip_parquet][overwrite] writing to {filepath_tmp}')
            pd_save_parquet(filepath_tmp,df) # write temp file
            os.system(f'mv {parquet_fp} {filepath_bck}') # keep existing file
            os.system(f'mv {filepath_tmp} {parquet_fp}') # move written file over

        except Exception as e:
            print(f'[strip_parquet][overwrite] error with {filepath_tmp}')
'''
strip_parquet('/home/alfred/nfs/cc_zho/extracts/2024-33/2024-33_0.parquet')
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
        jsonl_fp = indexdir_to_jsonl(indexdir, indexdir.name)
        if os.path.exists(jsonl_fp):
            strip_jsonl(jsonl_fp)

    except Exception as e:
        print(f'[strip_cache] exception {e}')
'''
strip_cache('/home/alfred/nfs/cc_zho/extracts/2019-22/')

for index in ALL_INDEXES:
    strip_cache(f'/home/alfred/nfs/cc_zho/extracts/{index}/')
'''

def move_redumped_cache(indexdir: str, action: bool = False) -> None:
    try:
        #print(f'[move_redumped_cache] indexdir = {indexdir}')
        indexdir_redumped = Path(str(indexdir).replace('/extracts/','/extracts_redump/')) # save to new directory
        #print(indexdir_redumped)
        indexdir_bck = Path(indexdir_redumped).parent / f'{Path(indexdir_redumped).name}_bck' # where to backup old cache
        #print(indexdir_bck)

        if indexdir_bck.exists():
            # no need to move it if it's already been moved
            print(f'[move_redumped_cache][pass] {indexdir}')
        else:
            # move it if it hasn't been moved
            save_cmd = f'mv {indexdir} {indexdir_bck}'
            print(f'[move_redumped_cache][save] {indexdir}')
            if action: os.system(save_cmd) # save existing
            take_cmd = f'mv {indexdir_redumped} {indexdir}'
            #print(f'[move_redumped_cache][take] {indexdir_redumped}')
            if action: os.system(take_cmd) # move written file over

    except Exception as e:
        print(f'[move_redumped_cache][error] {indexdir}')
'''
for index in ALL_INDEXES:
    move_redumped_cache(f'/home/alfred/nfs/cc_zho/extracts/{index}/')
'''

def redump_cache(indexdir: str, max_json_lines: int = 10000) -> None:
    '''
    read all jsonl and parquet files then redump new files that satisfy `max_json_lines`, i.e. apply after `strip_cache`
    '''
    try:
        print(f'[redump_cache] indexdir = {indexdir}')
        # read cache
        strip_cache(indexdir)
        listdict = read_cache(indexdir)

		# check if directory exists, if not create it
        indexdir_redumped = Path(str(indexdir).replace('/extracts/','/extracts_redump/')) # save to new directory
        if not indexdir_redumped.exists():
            indexdir_redumped.mkdir(parents=True, exist_ok=True) # create subdir
            indexdir_redumped.chmod(0o777) # make accessible

        # write new cache
        current_line = 0
        while current_line < len(listdict):
            if (current_line + max_json_lines) < len(listdict):
                # write compressed parquet
                parquet_filepath = next_parquet_filepath(indexdir_redumped,indexdir_redumped.name)
                pd_save_parquet(parquet_filepath,pd.DataFrame.from_records(listdict[current_line:current_line + max_json_lines]))
                current_line += max_json_lines
                print(f'[redump_cache] current_line = {current_line},  fp = {parquet_filepath}')
                os.chmod(parquet_filepath, 0o777) # make accessible
            else:
                # write jsonl
                jsonl_fp = indexdir_to_jsonl(indexdir_redumped, indexdir_redumped.name)
                with open(jsonl_fp, 'w', encoding='utf-8') as f:
                    for d in listdict[current_line:]:
                        line = json.dumps(d, ensure_ascii=False).encode('utf-8').decode('utf-8')
                        f.write(line + '\n')
                        current_line += 1
                print(f'[redump_cache] current_line = {current_line},  fp = {jsonl_fp}')
                os.chmod(jsonl_fp, 0o777) # make accessible

        # move extracts_redump into original directory
        move_redumped_cache(indexdir,True)

    except Exception as e:
        print(f'[redump_cache] exception {e}')
'''
redump_cache('/home/alfred/nfs/cc_zho/extracts/2019-22/')

for index in ALL_INDEXES:
    redump_cache(f'/home/alfred/nfs/cc_zho/extracts/{index}/')
'''

################################################################################
# pandas
################################################################################
# read jsonl
def pd_read_jsonl(jsonl_fp: str) -> pd.DataFrame:
    return pd.read_json(jsonl_fp, lines=True)
'''
pd.read_json('/home/alfred/nfs/cc_zho/extracts/2023-40/abc.jsonl', lines=True)
'''

# save parquet
def pd_save_parquet(parquet_fp: str, df: pd.DataFrame) -> None:
    try:
        df.to_parquet(parquet_fp,compression='snappy') # snappy
        #df.to_parquet(parquet_fp,compression='gzip')  # gzip
        #df.to_parquet(parquet_fp,compression='lz4')   # lz4
        #df.to_parquet(parquet_fp,compression='zstd')  # zstd
        #df.to_parquet(parquet_fp,compression='brotli') # brotli
        os.chmod(parquet_fp, 0o777) # make accessible

    except Exception as e:
        print(f'[pd_save_parquet] exception {e}')

# read parquet
def pd_read_parquet(parquet_fp: str, columns: list = None) -> pd.DataFrame:
    try:
        df = pd.read_parquet(parquet_fp, columns=columns)
        return df
    except Exception as e:
        print(f"Error reading the Parquet file: {e}")
'''
df = pd_read_parquet('/home/alfred/nfs/cc_zho/extracts/2019-43/2019-43_1.parquet')
'''

#%%
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

    if False:
        for index in ALL_INDEXES:
            redump_cache(f'/home/alfred/nfs/cc_zho/extracts/{index}/')

#%%
