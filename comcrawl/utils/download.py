"""Download Helpers.

This module contains helper functions for downloading records from Common Crawl S3 Buckets.

"""

import io
import gzip
import requests
from pathlib import Path
from .types import ResultList,Result
from .cache import write_to_file,read_file
from .multithreading import make_multithreaded


URL_TEMPLATE = "https://data.commoncrawl.org/{filename}"
RECORDS_PATH = 'data/records_api/'
TESTING = False

'''
{
    "urlkey": "com,hk01)/", 
    "timestamp": "20240614083751", 
    "url": "https://www.hk01.com/", 
    "mime": "text/html", 
    "mime-detected": "text/html", 
    "status": "200", 
    "digest": "MJVIRWTZZ3XUMBZV76MYIOXGNXDLB5JP", 
    "length": "65556", 
    "offset": "720642372", 
    "filename": "crawl-data/CC-MAIN-2024-26/segments/1718198861545.42/warc/CC-MAIN-20240614075213-20240614105213-00661.warc.gz", 
    "languages": "zho", 
    "encoding": "UTF-8"
}
'''
def index_from_filename(result: Result) -> Path:
    '''
    given result, strips '2024-26' from filename

    e.g.
    this
        "filename": "crawl-data/CC-MAIN-2024-26/segments/1718198861545.42/warc/CC-MAIN-20240614075213-20240614105213-00661.warc.gz", 
    returns
        '2024-26'
    '''
    fn = result['filename']
    cc_main_index = fn.split('/')[1]
    index = cc_main_index.replace("CC-MAIN-", "")
    return index

# storage location for index / url query
def record_cache_path(result: Result, path: str) -> Path:
    index = index_from_filename(result)
    return Path(path) / f"{index}/{result['digest']}.jsonl" # index

# given search result, request record
def request_single_record(result: Result, path: str = RECORDS_PATH, parse_warc: bool = False, return_content: bool = False) -> Result:
    """Downloads content for single search result.

    Args:
        result: Common Crawl Index search result from the search function.
            result["offset"]
            result["length"]
            result["filename"]
            result["digest"]
    Returns:
        The provided result, extended by the corresponding record content string.
    """
    # testing
    print(f'[request_single_record] path = {path}, parse_warc = {parse_warc}, return_content = {return_content}')
    if TESTING: return

    # do request
    request_url = URL_TEMPLATE.format(filename=result["filename"]) # build url
    offset, length = int(result["offset"]), int(result["length"]) # record start # TODO need to ensure Athena query returns these names
    offset_end = offset + length - 1 # record end # TODO need to ensure Athena query returns these names
    print(f'[request_single_record] request_url = {request_url}')
    response = requests.get(request_url, headers={"Range": f"bytes={offset}-{offset_end}"}) # submit request

    # extract results
    zipped_file = io.BytesIO(response.content)
    unzipped_file = gzip.GzipFile(fileobj=zipped_file)
    raw_data: bytes = unzipped_file.read()
    try:
        data: str = raw_data.decode("utf-8")
    except UnicodeDecodeError:
        print(f"[request_single_record] could not extract data from {request_url}")
        data = ""

    # parse result
    content: str = None # default
    if len(data) == 0:
        print(f'[request_single_record][content] no content')
    else:
        if parse_warc:
            # strip warc content
            data_parts = data.strip().split("\r\n\r\n", 2) # strip data
            if len(data_parts) != 3:
                print(f'[request_single_record][content] unexpected content length = {len(data_parts)}')
            else:
                content = data_parts[2] # overwrite default with parsed result
        else:
            # save entire content
            content = data
    # cache content
    write_to_file(content, record_cache_path(result, path))

    # amend result with content
    if return_content:
        result['content'] = content
    else:
        result['content'] = content is not None # True if have content else False

    # return
    return result

#%%
# read local if it exists
def get_single_record(result: Result, path: str = RECORDS_PATH, force_update: bool = False, parse_warc: bool = False, return_content: bool = False) -> Result:
    # populate res
    cache_path = record_cache_path(result, path) # cache location for searched info
    if cache_path.exists() and not force_update:
        print(f'[get_single_record][cache] read {cache_path}')
        # amend result with content
        if return_content:
            result['content'] = read_file(cache_path)
        else:
            result['content'] = True

        res = result
    else:
        print(f'[get_single_record][download] write {cache_path}')
        res = request_single_record(result, path, parse_warc, return_content)

    # return
    return res

def get_multiple_records(results: ResultList, threads: int = None, path: str = RECORDS_PATH, force_update: bool = False, parse_warc: bool = False, return_content: bool = False) -> ResultList:
    """Downloads search results.

    The corresponding record for each Common Crawl results list is downloaded.

    Args:
        results: List of Common Crawl search results.
        threads: Number of threads to use for faster parallel downloads on multiple threads.
        path: Where to cache results
        return_content: Should return bool in results['content]' or actual content

    Returns:
        The provided list of results with corresponding contents in 'contents' key.

    """
    # populate results
    res: ResultList = [] # default
    if threads:
        # multi-thread
        multithreaded_download = make_multithreaded(get_single_record, threads)
        res = multithreaded_download(results, path, force_update, parse_warc, return_content)
    else:
        # single-thread
        for result in results:
            record:Result = get_single_record(result, path, force_update, parse_warc, return_content)
            res.append(record) # append

    # return
    return res