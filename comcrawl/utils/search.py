"""Search Helpers.

This module contains helper functions for searching through Common Crawl Indexes.

"""

# https://skeptric.com/searching-100b-pages-cdx/

import json
from pathlib import Path
import requests
from requests.exceptions import ReadTimeout,RequestException
from urllib.parse import quote
from .types import ResultList,IndexList
from .cache import read_jsonl,write_jsonl
from .multithreading import make_multithreaded
from fake_useragent import UserAgent
ua = UserAgent()

TIMEOUT_DURATION = 60
URL_TEMPLATE = "https://index.commoncrawl.org/CC-MAIN-{index}-index?url={url}&output=json"

# storage location for index / url query
def search_cache_path(index: str, url: str, path: str) -> Path:
    return Path(path) / f'searches/index={index}_url={quote(url)}.jsonl'
'''
search_cache_path(index='2024-26', url='*.hk01.com').exists()
'''

# given index and url, do request with cc index api
def save_single_index(index: str, url: str, path: str) -> ResultList:
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
    #print(f'[save_single_index] path = {path}, index = {index}, url = {url}')

    # default
    results: ResultList = [] # no results

    # build request
    request_url = URL_TEMPLATE.format(index=index, url=url) # https://index.commoncrawl.org/CC-MAIN-2018-13-index?url=*.hk01.com/&output=json

    # do request
    try:
        # request
        print(f'[save_single_index] request_url = {request_url}')
        response = requests.get(
            request_url,
            timeout=TIMEOUT_DURATION,
            headers={
                "User-Agent": ua.random, # user_agent
                #"Accept-Encoding": "*",
                #"Connection": "keep-alive"
            }
        )
        # {"urlkey": "com,hk01,2016legcoelection)/candidate/tag/205/54", "timestamp": "20180321221633", "url": "http://2016legcoelection.hk01.com/candidate/tag/205/54", "mime": "text/html", "mime-detected": "text/html", "status": "200", "digest": "OMQ2TTHHOFVB3S7TPJUXKICPII6YNWJW", "length": "4535", "offset": "1183740", "filename": "crawl-data/CC-MAIN-2018-13/segments/1521257647706.69/warc/CC-MAIN-20180321215410-20180321235410-00385.warc.gz"}
        response.raise_for_status()

        # parse
        if response.status_code == 200:
            results = [json.loads(line) for line in response.content.decode().splitlines()] # overwrite default with parsed result
    except ReadTimeout as e:
        print(f'[save_single_index] request timed out: {e}')
    except RequestException as e:
        print(f'[save_single_index] an error occurred: {e}')

    # cache
    write_jsonl(results, search_cache_path(index, url, path))

    # return
    return results
'''
save_single_index('2024-26','*.hk01.com')
save_single_index('2024-26','*.hk.news.yahoo.com')
'''

# read local if it exists
def get_single_index(index: str, url: str, path: str) -> ResultList:
    # populate res
    cache_path = search_cache_path(index, url, path)
    if cache_path.exists():
        #print(f'[get_single_index][cache] {cache_path}')
        results = read_jsonl(cache_path)
    else:
        print(f'[get_single_index][download] {cache_path}')
        results = save_single_index(index, url, path)

    # return
    return results

def get_multiple_indexes(url: str, indexes: IndexList, path: str, threads: int = None) -> ResultList:
    """Searches multiple Common Crawl Indexes for URL pattern.

    Args:
        url: URL pattern to search for
        indexes: list of Common Crawl indexes to search
        threads: number of threads to use
        path: where to cache results

    Returns:
        List of all results found throughout the specified Common Crawl indexes.

    """
    # populate results
    out: ResultList = [] # default, i.e. no results
    if threads:
        # multi-thread
        multithreaded_search = make_multithreaded(get_single_index, threads)
        out = multithreaded_search(indexes, url, path)
    else:
        # single-thread
        for index in indexes:
            res:ResultList = get_single_index(index, url, path)
            out.extend(res) # extend

    # return
    return out