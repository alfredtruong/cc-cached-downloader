"""Download Helpers.

This module contains helper functions for downloading records from Common Crawl S3 Buckets.

"""

#import io
#import gzip
from pathlib import Path
import requests
from requests.exceptions import ReadTimeout,RequestException
from urllib.parse import urlparse
from .custom_types import Index,ResultList,Result
from .cache import write_file,read_gzip,write_gzip,write_cache
from .multithreading import make_multithreaded
from trafilatura import extract # strip content from html files
#from warcio.archiveiterator import ArchiveIterator

# https://www.alchemysoftware.com/livedocs/ezscript/Topics/Catalyst/Language.htm
'''
from fastlangid.langid import LID
langid = LID()
result = langid.predict('This is a test')
print(result)

Chinese (Simplified)								zh
Chinese (Simplified) (zh-Hans)						zh-Hans
Chinese (Simplified, People's Republic of China)	zh-CN
Chinese (Simplified, Singapore)						zh-SG
Chinese (Traditional) (zh-Hant)						zh-Hant
Chinese (Traditional, Hong Kong S.A.R.)				zh-HK
Chinese (Traditional, Macao S.A.R.)					zh-MO
Chinese (Traditional, Taiwan)						zh-TW
'''

from fake_useragent import UserAgent
ua = UserAgent()


TIMEOUT_DURATION = 5
URL_TEMPLATE = "https://data.commoncrawl.org/{filename}"

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

# storage location for raw result
def record_cache_path(result: Result, basepath: str) -> Path:
    index = index_from_result(result) # get index
    domain = domain_from_result(result) # get domain
    return Path(basepath) / f"records/{index}/{domain}/{result['digest']}.txt" # e.g. basepath/2024-27/hk01/fashion_hk01_com/hash[suffix].txt

# storage location for gzip of raw result
def gzip_cache_path(result: Result, basepath: str) -> Path:
    index = index_from_result(result) # get index
    domain = domain_from_result(result) # get domain
    return Path(basepath) / f"records/{index}/{domain}/{result['digest']}.gz" # e.g. basepath/2024-27/2024-27/hk01/fashion_hk01_com/hash[suffix].gz

# storage location for extract of result
def extract_cache_path(result: Result, basepath: str) -> Path:
    index = index_from_result(result) # get index
    domain = domain_from_result(result) # get domain
    return Path(basepath) / f"extracts/{index}/{domain}/{result['digest']}.txt" # e.g. basepath/2024-27/2024-27/hk01/fashion_hk01_com/hash[suffix].txt

# storage location for jsonl containing all results
def jsonl_cache_path(index: Index, basepath: str) -> Path:
    return Path(basepath) / f"extracts/{index}/{index}.jsonl" # e.g. basepath/2024-27/2024-27.jsonl

# storage location for index
def index_cache_path(index: Index, basepath: str) -> Path:
    return Path(basepath) / f"extracts/{index}/" # e.g. basepath/2024-27/

# given search result, request record
def request_single_record(result: Result) -> str:
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
    '''
    result = ic.results[101]
    basepath = ic.outdir
    '''
    # do request
    raw_content: str = "" # default = no content
    try:
        # build
        request_url = URL_TEMPLATE.format(filename=result["filename"])
        offset, length = int(result["offset"]), int(result["length"])
        offset_end = offset + length - 1

        # do
        #print(f'[request_single_record] request_url = {request_url}')
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
        #zipped_file = io.BytesIO(response.content)
        #unzipped_file = gzip.GzipFile(fileobj=zipped_file)
        #raw_data: bytes = unzipped_file.read()
        #raw_content: str = raw_data.decode("utf-8") # overwrite default with parsed result
        '''
        stream = ArchiveIterator(response.raw)
        nothing = None
        for warc_record in stream:
            print(type(warc_record))
            if warc_record.rec_type == 'response':
                nothing = warc_record.content_stream().read()
        '''

        # parse
        if response.status_code == 200:
            raw_content = response.content

    except ReadTimeout as e:
        print(f'[request_single_record] request timed out: {e}')
    except RequestException as e:
        print(f'[request_single_record] an error occurred: {e}')
    #except UnicodeDecodeError:
    #    print(f"[request_single_record] could not extract data from {request_url}")

    # return
    return raw_content

def get_single_record(result: Result, basepath: str, should_save: bool = False) -> str:
    cache_path = gzip_cache_path(result, basepath)
    if cache_path.exists():
        print(f'[get_single_extract][cache] {cache_path}')
        raw_content:str = read_gzip(cache_path)
    else:
        raw_content:str = request_single_record(result)
        if should_save:
            write_gzip(raw_content, cache_path)
            #write_file(raw_content, record_cache_path(result, basepath)) # contents of warc including header

    # return 
    return raw_content

# given search result, get record
def get_single_extract(result: Result, basepath: str, should_save: bool = False) -> Result:
    raw_content:str = get_single_record(result, basepath, should_save)

    # extract content from raw_content
    extracted_content: str = '' # default
    if len(raw_content) == 0:
        print(f'[get_single_extract] no content')
        print(raw_content)
    else:
        stripped_content = raw_content.strip().split("\r\n\r\n", 2) # strip content
        if len(stripped_content) != 3:
            print(f'[get_single_extract] unexpected length = {len(stripped_content)}')
            print(len(stripped_content))
        else:
            stripped_content = stripped_content[2] # overwrite default with parsed result
            extracted_content = extract(stripped_content) # overwrite default with parsed result

    # cache
    if extracted_content is None: extracted_content = ''
    fp = extract_cache_path(result, basepath)
    if fp.exists():
        print('.')
    else:
        print('x')
        if False: 
            write_file(extracted_content, fp) # write content extract into separate file
        else: 
            write_cache(
                [{'filepath':str(fp),'content':extracted_content}],
                index_cache_path(index_from_result(result),basepath)
            ) # write content extract to jsonl

    # return
    return result

def get_multiple_extracts(results: ResultList, basepath: str, should_save: bool = False, threads: int = None) -> ResultList:
    """Downloads search results.

    The corresponding record for each Common Crawl results list is downloaded.

    Args:
        results: list of Common Crawl search results
        threads: number of threads to use
        basepath: where to cache results

    Returns:
        List of all results with corresponding contents in 'contents' key and languages in 'languages' key if requested

    """
    # populate results
    out: ResultList = [] # default = no results
    if threads:
        # multi-thread
        multithreaded_download = make_multithreaded(get_single_extract, threads)
        out = multithreaded_download(results, basepath, should_save)
    else:
        # single-thread
        for result in results:
            res:Result = get_single_extract(result, basepath, should_save)
            out.append(res) # append single result

    # return
    return out