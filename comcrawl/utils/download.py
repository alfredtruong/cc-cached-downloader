"""Download Helpers.

This module contains helper functions for downloading pages from the Common Crawl S3 Buckets.

"""

import io
import gzip
import requests
from ..types import Result, ResultList
from .multithreading import make_multithreaded


URL_TEMPLATE = "https://data.commoncrawl.org/{filename}"


def download_single_result(result: Result) -> Result:
    """Downloads HTML for single search result.

    Args:
        result: Common Crawl Index search result from the search function.
            result["offset"]
            result["length"]
            result["filename"]
            result["digest"]
    Returns:
        The provided result, extended by the corresponding HTML String.

    """
    offset, length = int(result["offset"]), int(result["length"]) # TODO need to ensure Athena query returns these names
    offset_end = offset + length - 1

    url = URL_TEMPLATE.format(filename=result["filename"])
    response = (requests
                .get(
                    url,
                    headers={"Range": f"bytes={offset}-{offset_end}"}
                ))

    zipped_file = io.BytesIO(response.content)
    unzipped_file = gzip.GzipFile(fileobj=zipped_file)

    raw_data: bytes = unzipped_file.read()
    try:
        data: str = raw_data.decode("utf-8")
    except UnicodeDecodeError:
        print(f"Warning: Could not extract file downloaded from {url}")
        data = ""

    # maybe just save it and dont bloat memory
    result["content"] = ""
    if len(data) > 0:
        data_parts = data.strip().split("\r\n\r\n", 2) # remove warc scrape info
        if len(data_parts) == 3:
            result["content"] = data_parts[2]

    return result

def download_multiple_results(results: ResultList, threads: int = None, path: str = 'data/contents/', force_update: bool = False) -> ResultList:
    """Downloads search results.

    For each Common Crawl search result in the given list the
    corresponding HTML page is downloaded.

    Args:
        results: List of Common Crawl search results.
        threads: Number of threads to use for faster parallel downloads on multiple threads.

    Returns:
        The provided results list, extended by the corresponding contents.

    """
    # populate results
    results_with_content: ResultList = [] # default result
    if threads:
        # multi-thread
        multithreaded_download = make_multithreaded(download_single_result, threads)
        results_with_content = multithreaded_download(results)

    else:
        # single-thread
        for result in results:
            result_with_content = download_single_result(result)
            results_with_content.append(result_with_content)

    return results_with_content