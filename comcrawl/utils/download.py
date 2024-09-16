"""Download Helpers.

This module contains helper functions for downloading records from Common Crawl S3 Buckets.

"""

#import io
#import gzip
from pathlib import Path
import requests
from requests.exceptions import ReadTimeout,RequestException
from trafilatura import extract # strip content from html files
from urllib.parse import urlparse
from .types import Index,ResultList,Result
from .cache import write_file,read_gzip,write_gzip,write_cache
from .multithreading import make_multithreaded
#from warcio.archiveiterator import ArchiveIterator

from fastlangid.langid import LID
langid = LID()
'''
result = langid.predict('This is a test')
print(result)
'''

# https://www.alchemysoftware.com/livedocs/ezscript/Topics/Catalyst/Language.htm
'''
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
def save_single_record(result: Result, basepath: str) -> None:
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
    # testing
    #print(f'[save_single_record] basepath = {basepath}')

    # default
    #raw_content: str = "" # no content

    # build request
    request_url = URL_TEMPLATE.format(filename=result["filename"])
    offset, length = int(result["offset"]), int(result["length"])
    offset_end = offset + length - 1

    # do request
    try:
        # request
        #print(f'[save_single_record] request_url = {request_url}')
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

        # cache
        write_gzip(response.content, gzip_cache_path(result, basepath))

    except ReadTimeout as e:
        print(f'[save_single_record] request timed out: {e}')
    except RequestException as e:
        print(f'[save_single_record] an error occurred: {e}')
    #except UnicodeDecodeError:
    #    print(f"[save_single_record] could not extract data from {request_url}")

    # cache
    #write_gzip(response.content, gzip_cache_path(result, basepath))
    #write_file(raw_content, record_cache_path(result, basepath)) # contents of warc including header

    # return
    #return raw_content

# given search result, request record
def save_single_extract(result: Result, basepath: str) -> str:
    # ensure we have warc gz
    cache_path = gzip_cache_path(result, basepath)
    if not cache_path.exists():
        #print(f'[save_single_extract][download] {cache_path}')
        save_single_record(result, basepath)

    # parse warc gz
    raw_content:str = read_gzip(cache_path)

    # extract contents
    extracted_content: str = '' # default
    if len(raw_content) == 0:
        print(f'[save_single_extract] no content')
        print(raw_content)
    else:
        stripped_content = raw_content.strip().split("\r\n\r\n", 2) # strip content
        if len(stripped_content) != 3:
            print(f'[save_single_extract] unexpected length = {len(stripped_content)}')
            print(len(stripped_content))
        else:
            stripped_content = stripped_content[2] # overwrite default with parsed result
            extracted_content = extract(stripped_content) # overwrite default with parsed result

    # cache
    if extracted_content is None: extracted_content = ''
    filepath = extract_cache_path(result, basepath) # id for record
    if False: write_file(extracted_content, filepath) # write content extract into separate file
    if True: 
        write_cache(
            [{'filepath':str(filepath),'content':extracted_content}],
            index_cache_path(index_from_result(result),basepath)
        ) # write content extract to jsonl

    # return
    return extracted_content

# read local if it exists
def get_single_extract(result: Result, basepath: str) -> Result:
    # bail if already cached, i.e. content already extracted
    filepath = extract_cache_path(result, basepath)
    if filepath.exists():
        #print(f'[get_single_extract][cache] {cache_path}')
        print('.')
        return result
    # otherwise get it

    # populate res
    #print(f'[get_single_extract][extract] {cache_path}')
    print('x')
    save_single_extract(result, basepath)

    # return
    return result

def get_multiple_extracts(results: ResultList, basepath: str, threads: int = None) -> ResultList:
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
    out: ResultList = [] # default, i.e. no results
    if threads:
        # multi-thread
        multithreaded_download = make_multithreaded(get_single_extract, threads)
        out = multithreaded_download(results, basepath)
    else:
        # single-thread
        for result in results:
            res:Result = get_single_extract(result, basepath)
            out.append(res) # append

    # return
    return out