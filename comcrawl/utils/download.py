"""Download Helpers.

This module contains helper functions for downloading records from Common Crawl S3 Buckets.

"""

import io
import gzip
from pathlib import Path
import requests
from requests.exceptions import ReadTimeout,RequestException
from trafilatura import extract # strip content from html files
from urllib.parse import urlparse
from .types import ResultList,Result
from .cache import read_file,write_file,read_gzip,write_gzip
from .multithreading import make_multithreaded
from fake_useragent import UserAgent
ua = UserAgent()


TIMEOUT_DURATION = 60
URL_TEMPLATE = "https://data.commoncrawl.org/{filename}"
RECORDS_PATH = 'data/records/'
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
def index_from_result(result: Result) -> Path:
    '''
    from
        "filename": "crawl-data/CC-MAIN-2024-26/segments/1718198861545.42/warc/CC-MAIN-20240614075213-20240614105213-00661.warc.gz", 
    get
        '2024-26'
    '''
    fn = result['filename']
    cc_main_index = fn.split('/')[1]
    index = cc_main_index.replace("CC-MAIN-", "")
    return index

def url_from_result(result: Result) -> Path:
    '''
    from 
        'https://www.hk01.com/%E4%B8%96%E7%95%8C%E5%B0%88%E9%A1%8C/507167/%E8%8B%B1%E5%9C%8B%E5%88%A9%E7%89%A9%E6%B5%A6%E6%B5%B7%E6%80%AA%E7%9C%9F%E8%BA%AB%E6%98%AF-%E5%9B%9E%E7%9C%8B%E8%A2%AB%E6%B2%96%E4%B8%8A%E5%B2%B8%E8%AC%8E%E4%B9%8B%E7%94%9F%E7%89%A9-%E4%BA%BA%E9%AD%9A-%E6%9B%B4%E6%81%90%E6%80%96'
    get
        'www.hk01.com'
    '''
    url_dir = urlparse(result['url']).netloc.replace(".", "_") # url to dirname
    return url_dir

def domain_from_result(result: Result) -> Path:
    '''
    from
        'com,hk01)/%e4%b8%96%e7%95%8c%e5%b0%88%e9%a1%8c/507167/%e8%8b%b1%e5%9c%8b%e5%88%a9%e7%89%a9%e6%b5%a6%e6%b5%b7%e6%80%aa%e7%9c%9f%e8%ba%ab%e6%98%af-%e5%9b%9e%e7%9c%8b%e8%a2%ab%e6%b2%96%e4%b8%8a%e5%b2%b8%e8%ac%8e%e4%b9%8b%e7%94%9f%e7%89%a9-%e4%ba%ba%e9%ad%9a-%e6%9b%b4%e6%81%90%e6%80%96'
    get
        'com,hk01'
    '''
    domain = result['urlkey'].split(')/')[0] # com,eatthekiwi,store,hk)/blogs/blogthekiwi/cloudy-bay-storm-clams
    #domain = result['urlkey'].split(',')[1] # eatthekiwi
    return domain

# storage location for result
def record_cache_path(result: Result, path: str = RECORDS_PATH) -> Path:
    index = index_from_result(result) # get index
    domain = domain_from_result(result) # get domain
    return Path(path) / f"records/{index}/{domain}/{result['digest']}.txt" # e.g. path/2024-27/hk01/fashion_hk01_com/hash[suffix].txt

# storage location for result
def gzip_cache_path(result: Result, path: str = RECORDS_PATH) -> Path:
    index = index_from_result(result) # get index
    domain = domain_from_result(result) # get domain
    return Path(path) / f"records/{index}/{domain}/{result['digest']}.gz" # e.g. path/2024-27/hk01/fashion_hk01_com/hash[suffix].gz

# storage location for result
def extract_cache_path(result: Result, path: str = RECORDS_PATH) -> Path:
    index = index_from_result(result) # get index
    domain = domain_from_result(result) # get domain
    return Path(path) / f"extracts/{index}/{domain}/{result['digest']}.txt" # e.g. path/2024-27/hk01/fashion_hk01_com/hash[suffix].txt

# given search result, request record
def request_single_record(result: Result, path: str = RECORDS_PATH) -> str:
    """Downloads content for single search result.

    Args:
        result: Common Crawl Index search result from the search function.
            result["url"]
            result["urlkey"]
            result["digest"]
            result["filename"]
            result["offset"]
            result["length"]
    Returns:
        The provided result, extended by the corresponding record content string.
    """
    # testing
    #print(f'[request_single_record] path = {path}')
    if TESTING: return

    # default
    raw_content: str = "" # no content

    # build request
    request_url = URL_TEMPLATE.format(filename=result["filename"])
    offset, length = int(result["offset"]), int(result["length"])
    offset_end = offset + length - 1

    # do request
    try:
        # request
        print(f'[request_single_record] request_url = {request_url}')
        response = requests.get(
            request_url,
            timeout=TIMEOUT_DURATION,
            headers={
                "Range": f"bytes={offset}-{offset_end}",
                "User-Agent": ua.random, # user_agent
                #"Accept-Encoding": "*",
                #"Connection": "keep-alive"
            }
        )
        response.raise_for_status()
        # ('Connection aborted.', ConnectionResetError(10054, 'An existing connection was forcibly closed by the remote host', None, 10054, None))

        # parse
        zipped_file = io.BytesIO(response.content)
        unzipped_file = gzip.GzipFile(fileobj=zipped_file)
        raw_data: bytes = unzipped_file.read()
        raw_content: str = raw_data.decode("utf-8") # overwrite default with parsed result
    except ReadTimeout as e:
        print(f'[request_single_record] request timed out: {e}')
    except RequestException as e:
        print(f'[request_single_record] an error occurred: {e}')
    except UnicodeDecodeError:
        print(f"[request_single_record] could not extract data from {request_url}")

    # cache
    write_gzip(response.content, gzip_cache_path(result, path))
    #write_file(raw_content, record_cache_path(result, path)) # contents of warc including header

    # return
    return raw_content

# given search result, request record
def get_single_record(result: Result, path: str = RECORDS_PATH, force_update: bool = False, append_extract: bool = False) -> Result:
    # get raw warc
    cache_path = gzip_cache_path(result, path)
    if cache_path.exists() and not force_update:
        print(f'[get_single_record][cache] {cache_path}')
        raw_content = read_gzip(cache_path)
    else:
        print(f'[get_single_record][download] {cache_path}')
        raw_content = request_single_record(result, path)

    # finalize
    extracted_content = '' # default
    if len(raw_content) == 0:
        print(f'[get_single_record][content] no content')
    else:
        stripped_content = raw_content.strip().split("\r\n\r\n", 2) # strip content
        if len(stripped_content) != 3:
            print(f'[get_single_record][content] unexpected length = {len(stripped_content)}')
        else:
            stripped_content = stripped_content[2] # overwrite default with parsed result
            extracted_content = extract(stripped_content) # overwrite default with parsed result

    # cache
    if extracted_content is None: extracted_content = ''
    write_file(extracted_content, extract_cache_path(result, path)) # extract of contents

    # add 'content' key with required info
    if append_extract:
        result['content'] = extracted_content
    else:
        result['content'] = len(extracted_content) != 0 # True if have content else False

    # return
    return result

# read local if it exists
def get_single_extract(result: Result, path: str = RECORDS_PATH, force_update: bool = False, append_extract: bool = False) -> Result:
    # populate res
    cache_path = extract_cache_path(result, path)
    if cache_path.exists() and not force_update:
        print(f'[get_single_extract][cache] {cache_path}')
        result['content'] = read_file(cache_path) if append_extract else True # add 'content' key with required info
    else:
        print(f'[get_single_extract][download] {cache_path}')
        result = get_single_record(result, path, force_update, append_extract)

    # return
    return result

def get_multiple_extracts(results: ResultList, threads: int = None, path: str = RECORDS_PATH, force_update: bool = False, append_extract: bool = False) -> ResultList:
    """Downloads search results.

    The corresponding record for each Common Crawl results list is downloaded.

    Args:
        results: list of Common Crawl search results
        threads: number of threads to use
        path: where to cache results
        force_update: if cache should be ignored / repopulated
        append_extract: Should return bool in results['content]' or actual content

    Returns:
        The provided list of results with corresponding contents in 'contents' key.

    """
    # populate results
    out: ResultList = [] # default, i.e. no results
    if threads:
        # multi-thread
        multithreaded_download = make_multithreaded(get_single_extract, threads)
        out = multithreaded_download(results, path, force_update, append_extract)
    else:
        # single-thread
        for result in results:
            res:Result = get_single_extract(result, path, force_update, append_extract)
            out.append(res) # append

    # return
    return out