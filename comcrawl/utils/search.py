"""Search Helpers.

This module contains helper functions for searching through Common Crawl Indexes.

"""

import os
from urllib.parse import quote
import json
import requests
from ..types import ResultList, IndexList
from .cache import read_jsonl,write_jsonl
from .multithreading import make_multithreaded


URL_TEMPLATE = "https://index.commoncrawl.org/CC-MAIN-{index}-index?url={url}&output=json"


# storage location for index / url query
def build_search_cache_filepath(index: str, url: str, path: str) -> str:
    return os.path.join(path, f'index={index}_url={quote(url)}.jsonl')

# given index and url, do request against cdx api
def request_single_index(index: str, url: str, path: str) -> ResultList:
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

    # build index for url and return results in jsonl format
    url = URL_TEMPLATE.format(index=index, url=url) # https://index.commoncrawl.org/CC-MAIN-2018-13-index?url=*.hk01.com/&output=json
    
    # submit request
    response = requests.get(url)
    # {"urlkey": "com,hk01,2016legcoelection)/candidate/tag/205/54", "timestamp": "20180321221633", "url": "http://2016legcoelection.hk01.com/candidate/tag/205/54", "mime": "text/html", "mime-detected": "text/html", "status": "200", "digest": "OMQ2TTHHOFVB3S7TPJUXKICPII6YNWJW", "length": "4535", "offset": "1183740", "filename": "crawl-data/CC-MAIN-2018-13/segments/1521257647706.69/warc/CC-MAIN-20180321215410-20180321235410-00385.warc.gz"}

    # populate results
    results: ResultList = [] # default result
    if response.status_code == 200:
        results = [json.loads(result) for result in response.content.decode().splitlines()] # overwrite if successful
        write_jsonl(results, build_search_cache_filepath(index, url, path)) # cache

    # return
    return results
'''
request_single_index('2024-26','*.hk01.com')
request_single_index('2024-26','*.hk.news.yahoo.com')
'''

# read local if it exists
def search_single_index(index: str, url: str, path: str = 'data/request_cdx_api/', force_update: bool = False) -> ResultList:
    """Searches specific Common Crawl Index for given URL pattern.

    Args:
        index: Common Crawl Index to search.
        url: URL Pattern to search.

    Returns:
        List of results dictionaries found in specified Index for the URL.

    """
    # populate results
    results: ResultList = [] # default result
    search_cache_filepath = build_search_cache_filepath(index, url, path)
    if os.path.exists(search_cache_filepath) and not force_update:
        print(f'[search_single_index] read {search_cache_filepath}')
        results = read_jsonl(search_cache_filepath) # parse cached target
    else:
        print(f'[search_single_index] query {index} for {url} and save to {search_cache_filepath}')
        results = request_single_index(index, url, path) # do request and save

    # return
    return results

def search_multiple_indexes(url: str, indexes: IndexList, threads: int = None, path: str = 'data/search_api/', force_update: bool = False) -> ResultList:
    """Searches multiple Common Crawl Indexes for URL pattern.

    Args:
        url: The URL pattern to search for.
        indexes: List of Common Crawl Indexes to search through.
        threads: Number of threads to use for faster parallel search on multiple threads.

    Returns:
        List of all results found throughout the specified
        Common Crawl indexes.

    """
    # populate results
    results: ResultList = [] # default result
    if threads:
        # multi-thread
        multithreaded_search = make_multithreaded(search_single_index, threads)
        results = multithreaded_search(indexes, url, path, force_update)

    else:
        # single-thread
        for index in indexes:
            index_results = search_single_index(index, url, path, force_update)
            results.extend(index_results) # extend vs append?

    return results