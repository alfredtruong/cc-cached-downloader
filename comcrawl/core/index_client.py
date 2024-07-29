"""Index Client.

This module contains the core object of the package.

"""

import logging
from pathlib import Path
from ..utils.types import Index, IndexList, ResultList
from ..utils import download_available_indexes,get_multiple_indexes,get_multiple_records,read_json,write_json


class IndexClient:
    """Common Crawl Index Client.

    Query Common Crawl indexes and download pages from the corresponding Common Crawl AWS S3 Buckets locations.

    Attributes:
        results: The list of results after calling the search method.

    """

    def __init__(self, index: Index = None, cache: str = 'data/', verbose: bool = False) -> None:
        """Initializes the class instance.

        Args:
            indexes: Index to focus on.
            verbose: Whether to print debug level logs to the console while making HTTP requests.

        """
        if verbose:
            logging.basicConfig(level=logging.DEBUG)

        self.indexes = [index] if isinstance(index,str) else index # ensure index saved as a list
        self.cache = Path(cache)
        self.results: ResultList = []

    def get_available_indexes(self, force_update: bool = False) -> IndexList:
        """Show all available indexes

        Searches the Common Crawl indexes this class was intialized with.

        Args:
            url: URL pattern to search
            threads: Number of threads to use. Enables multi-threading only if set.

        """
        # where we would cache the results
        cache_target = self.cache / 'index/collinfo.json'

        # download or read it
        if not cache_target.exists() or force_update:
            available_indexes = download_available_indexes() # download
            if not cache_target.parent.exists(): cache_target.mkdir(parents=True, exist_ok=True) # ensure output directory exists
            write_json(available_indexes, cache_target) # cache results
        else:
            available_indexes = read_json(cache_target) # read cache

        # return it
        return available_indexes

    def search_athena(self, query: str, force_update: bool = False) -> None:
        """Search.

        Searches the Common Crawl indexes this class was intialized with.

        Args:
            url: URL pattern to search
            threads: Number of threads to use. Enables multi-threading only if set.

        """
        self.results = None


    def search_api(self, url: str, threads: int = None, force_update: bool = False) -> None:
        """Search.

        Searches the Common Crawl indexes this class was intialized with.

        Args:
            url: URL pattern to search
            threads: Number of threads to use. Enables multi-threading only if set.

        """
        self.results = get_multiple_indexes(url, self.indexes, threads, self.cache / 'search_api', force_update)

    def download(self, threads: int = None, force_update: bool = False, parse_warc: bool = False, return_content: bool = False) -> None:
        """Download.

        Downloads warc extracts for every search result in the `results` attribute.

        Args:
            threads: Number of threads to use. Enables multi-threading only if set.

        """
        self.results = get_multiple_records(self.results, threads, self.cache / 'records_api' , force_update, parse_warc, return_content)
