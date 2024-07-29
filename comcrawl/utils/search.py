"""Search Helpers.

This module contains helper functions for searching through Common Crawl Indexes.

"""

from urllib.parse import quote
import json
import requests
from pathlib import Path
from .types import ResultList,IndexList
from .cache import read_jsonl,write_jsonl
from .multithreading import make_multithreaded


URL_TEMPLATE = "https://index.commoncrawl.org/CC-MAIN-{index}-index?url={url}&output=json"
SEARCH_API_PATH = 'data/search_api/'
TESTING = False

# storage location for index / url query
def search_cache_path(index: str, url: str, path: str = SEARCH_API_PATH) -> Path:
    return Path(path) / f'index={index}_url={quote(url)}.jsonl'
'''
search_cache_path(index='2024-26', url='*.hk01.com').exists()
'''

# given index and url, request index api
def request_single_index(index: str, url: str, path: str = SEARCH_API_PATH) -> ResultList:
    """Searches specific Common Crawl Index for given URL pattern.

    Args:
        index: Common Crawl Index to search.
        url: URL Pattern to search.

    Returns:
        List of results dictionaries found in specified Index for the URL.

    """
    '''
    index = '2024-26'
    url = '*.hk01.com'
    '''
    '''
    cdx-api
    - uses pywb cc-index api, https://index.commoncrawl.org/CC-MAIN-2018-13-index
    - https://github.com/ikreymer/cc-index-server
    - https://github.com/ikreymer/cdx-index-client
    '''

    # testing
    print(f'[request_single_index] index = {index}, url = {url}, path = {path}')
    if TESTING: return

    # do request
    request_url = URL_TEMPLATE.format(index=index, url=url) # build url # https://index.commoncrawl.org/CC-MAIN-2018-13-index?url=*.hk01.com/&output=json
    print(f'[request_single_index] request_url = {request_url}')
    response = requests.get(request_url) # submit request # {"urlkey": "com,hk01,2016legcoelection)/candidate/tag/205/54", "timestamp": "20180321221633", "url": "http://2016legcoelection.hk01.com/candidate/tag/205/54", "mime": "text/html", "mime-detected": "text/html", "status": "200", "digest": "OMQ2TTHHOFVB3S7TPJUXKICPII6YNWJW", "length": "4535", "offset": "1183740", "filename": "crawl-data/CC-MAIN-2018-13/segments/1521257647706.69/warc/CC-MAIN-20180321215410-20180321235410-00385.warc.gz"}

    # parse result
    results: ResultList = [] # default
    if response.status_code == 200:
        results = [json.loads(result) for result in response.content.decode().splitlines()] # overwrite default with parsed result

    # cache result
    write_jsonl(results, search_cache_path(index, url, path))

    # return
    return results
'''
request_single_index('2024-26','*.hk01.com')
request_single_index('2024-26','*.hk.news.yahoo.com')
'''

# read local if it exists
def get_single_index(index: str, url: str, path: str = SEARCH_API_PATH, force_update: bool = False) -> ResultList:
    # populate res
    res: ResultList = [] # default
    cache_path = search_cache_path(index, url, path) # cache location for searched info
    if cache_path.exists() and not force_update:
        print(f'[get_single_index][cache] read {cache_path}')
        res = read_jsonl(cache_path)
    else:
        print(f'[get_single_index][download] index = {index} for url {url} and save to {cache_path}')
        res = request_single_index(index, url, path)

    # return
    return res

def get_multiple_indexes(url: str, indexes: IndexList, threads: int = None, path: str = SEARCH_API_PATH, force_update: bool = False) -> ResultList:
    """Searches multiple Common Crawl Indexes for URL pattern.

    Args:
        url: The URL pattern to search for.
        indexes: List of Common Crawl Indexes to search through.
        threads: Number of threads to use for faster parallel search on multiple threads.

    Returns:
        List of all results found throughout the specified Common Crawl indexes.

    """
    # populate results
    res: ResultList = [] # default
    if threads:
        # multi-thread
        multithreaded_search = make_multithreaded(get_single_index, threads)
        res = multithreaded_search(indexes, url, path, force_update)
    else:
        # single-thread
        for index in indexes:
            index_results: ResultList = get_single_index(index, url, path, force_update)
            res.extend(index_results) # extend

    # return
    return res