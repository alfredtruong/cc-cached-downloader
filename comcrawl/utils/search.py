"""Search Helpers.

This module contains helper functions for searching through Common Crawl Indexes.

"""

# https://skeptric.com/searching-100b-pages-cdx/

import json
from pathlib import Path
import requests
from requests.exceptions import ReadTimeout,RequestException
from urllib.parse import quote
from .custom_types import ResultList,IndexList
from .cache import read_jsonl,write_jsonl
from .multithreading import make_multithreaded

from fake_useragent import UserAgent
ua = UserAgent()


TIMEOUT_DURATION = 60
URL_TEMPLATE = "https://index.commoncrawl.org/CC-MAIN-{index}-index?url={url}&output=json"

# storage location for index / url query
def search_cache_path(url: str, index: str, basepath: str) -> Path:
    return Path(basepath) / f'searches/index={index}_url={quote(url)}.jsonl'
'''
search_cache_path(url='*.hk01.com', index='2024-26').exists()
'''

# given index and url, request with cc index api
def request_single_index(url: str, index: str) -> ResultList:
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

    cdx-api
    - uses pywb cc-index api, https://index.commoncrawl.org/CC-MAIN-2018-13-index
    - https://github.com/ikreymer/cc-index-server
    - https://github.com/ikreymer/cdx-index-client
    '''
    # do request
    results: ResultList = [] # default = no results
    try:
        # build
        request_url = URL_TEMPLATE.format(index=index, url=url) # https://index.commoncrawl.org/CC-MAIN-2018-13-index?url=*.hk01.com/&output=json

        # do
        #print(f'[request_single_index] request_url = {request_url}')
        response = requests.get(
            request_url,
            timeout=TIMEOUT_DURATION,
            headers={
                "User-Agent": ua.random, # user_agent
                #"Accept-Encoding": "*",
                #"Connection": "keep-alive"
            }
        )
        response.raise_for_status()
        # {"urlkey": "com,hk01,2016legcoelection)/candidate/tag/205/54", "timestamp": "20180321221633", "url": "http://2016legcoelection.hk01.com/candidate/tag/205/54", "mime": "text/html", "mime-detected": "text/html", "status": "200", "digest": "OMQ2TTHHOFVB3S7TPJUXKICPII6YNWJW", "length": "4535", "offset": "1183740", "filename": "crawl-data/CC-MAIN-2018-13/segments/1521257647706.69/warc/CC-MAIN-20180321215410-20180321235410-00385.warc.gz"}

        # parse
        if response.status_code == 200:
            results = [json.loads(line) for line in response.content.decode().splitlines()] # overwrite default with parsed result

    except ReadTimeout as e:
        print(f'[request_single_index] request timed out: {e}')
    except RequestException as e:
        print(f'[request_single_index] an error occurred: {e}')

    # return
    return results
'''
request_single_index('*.hk01.com','2024-26')
request_single_index('*.hk.news.yahoo.com','2024-26')
'''

# given index and url, get index
def get_single_index(url: str, index: str, basepath: str, should_save: bool = False) -> ResultList:
    cache_path = search_cache_path(url, index, basepath)
    if cache_path.exists():
        print(f'[get_single_index][cache] {cache_path}')
        results: ResultList = read_jsonl(cache_path)
    else:
        results: ResultList = request_single_index(url, index)
        if should_save:
            write_jsonl(results, cache_path)

    # return
    return results

def get_multiple_indexes(url: str, indexes: IndexList, basepath: str, should_save: bool = False, threads: int = None) -> ResultList:
    """Searches multiple Common Crawl Indexes for URL pattern.

    Args:
        url: URL pattern to search for
        indexes: list of Common Crawl indexes to search
        threads: number of threads to use
        basepath: where to cache results

    Returns:
        List of all results found throughout the specified Common Crawl indexes.

    """
    # populate results
    out: ResultList = [] # default = no results
    if threads:
        # multi-thread
        multithreaded_search = make_multithreaded(get_single_index, threads)
        out = multithreaded_search(indexes, url, basepath, should_save)
    else:
        # single-thread
        for index in indexes:
            res:ResultList = get_single_index(index, url, basepath, should_save)
            out.extend(res) # extend with multiple results

    # return
    return out