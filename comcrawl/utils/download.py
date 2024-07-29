"""Download Helpers.

This module contains helper functions for downloading records from Common Crawl S3 Buckets.

"""

import io
import gzip
from pathlib import Path
import requests
from requests.exceptions import ReadTimeout,RequestException
from urllib.parse import urlparse
from .types import ResultList,Result
from .cache import read_file,write_file
from .multithreading import make_multithreaded


TIMEOUT_DURATION = 60
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

# storage location for result
def record_cache_path(result: Result, path: str = RECORDS_PATH) -> Path:
    index = index_from_filename(result) # cc filename to index
    url_dir = urlparse(result['url']).netloc.replace(".", "_") # url to dirname
    return Path(path) / f"{index}/{url_dir}/{result['digest']}.txt" # e.g. path/2024-27/hk01/_hash_.txt

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

    # default
    content: str = "" # no content

    # build request
    request_url = URL_TEMPLATE.format(filename=result["filename"])
    offset, length = int(result["offset"]), int(result["length"])
    offset_end = offset + length - 1

    # do request
    try:
        # request
        print(f'[request_single_record] request_url = {request_url}')
        response = requests.get(request_url, timeout=TIMEOUT_DURATION, headers={"Range": f"bytes={offset}-{offset_end}"})
        response.raise_for_status()

        # parse
        zipped_file = io.BytesIO(response.content)
        unzipped_file = gzip.GzipFile(fileobj=zipped_file)
        raw_data: bytes = unzipped_file.read()
        content: str = raw_data.decode("utf-8") # overwrite default with parsed result
    except ReadTimeout as e:
        print(f'[request_single_record] request timed out: {e}')
    except RequestException as e:
        print(f'[request_single_record] an error occurred: {e}')
    except UnicodeDecodeError:
        print(f"[request_single_record] could not extract data from {request_url}")

    # finalize
    if len(content) == 0:
        print(f'[request_single_record][content] no content')
    else:
        if parse_warc:
            # strip warc content
            content_parts = content.strip().split("\r\n\r\n", 2)
            if len(content_parts) != 3:
                print(f'[request_single_record][content] unexpected length = {len(content_parts)}')
            else:
                content = content_parts[2] # overwrite default with parsed result

    # cache
    write_file(content, record_cache_path(result, path))

    # add 'content' key with required info
    if return_content:
        result['content'] = content
    else:
        result['content'] = len(content) != 0 # True if have content else False

    # return
    return result

# read local if it exists
def get_single_record(result: Result, path: str = RECORDS_PATH, force_update: bool = False, parse_warc: bool = False, return_content: bool = False) -> Result:
    # populate res
    cache_path = record_cache_path(result, path) # cache location for searched info
    if cache_path.exists() and not force_update:
        print(f'[get_single_record][cache] read {cache_path}')
        # add 'content' key with required info
        if return_content:
            result['content'] = read_file(cache_path)
        else:
            result['content'] = True
    else:
        print(f'[get_single_record][download] write {cache_path}')
        result = request_single_record(result, path, parse_warc, return_content)

    # return
    return result

def get_multiple_records(results: ResultList, threads: int = None, path: str = RECORDS_PATH, force_update: bool = False, parse_warc: bool = False, return_content: bool = False) -> ResultList:
    """Downloads search results.

    The corresponding record for each Common Crawl results list is downloaded.

    Args:
        results: list of Common Crawl search results
        threads: number of threads to use
        path: where to cache results
        force_update: if cache should be ignored / repopulated
        parse_warc: return raw or parsed warc record
        return_content: Should return bool in results['content]' or actual content

    Returns:
        The provided list of results with corresponding contents in 'contents' key.

    """
    # populate results
    results: ResultList = [] # default, i.e. no results
    if threads:
        # multi-thread
        multithreaded_download = make_multithreaded(get_single_record, threads)
        results = multithreaded_download(results, path, force_update, parse_warc, return_content)
    else:
        # single-thread
        for result in results:
            record:Result = get_single_record(result, path, force_update, parse_warc, return_content)
            results.append(record) # append

    # return
    return results