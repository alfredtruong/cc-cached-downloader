"""Index Client.

This module contains the core object of the package.

"""

import logging
from pathlib import Path
from ..utils.types import Index, IndexList, ResultList
from ..utils import download_available_indexes,get_multiple_indexes,get_multiple_extracts,read_json,write_json
import pandas as pd

class IndexClient:
    """Common Crawl Index Client.

    Query Common Crawl indexes and download pages from the corresponding Common Crawl AWS S3 Buckets locations.

    Attributes:
        results: The list of results after calling the search method.

    """
    # Athena query csv files associated to above query, do this manually
    ATHENA_QUERY_EXECUTION_IDS = {
        #'2024-30':'5ef8e286-09bd-44f2-aca5-515cf7fe9a24', # no data
        '2024-26':'f19e4aad-c0cb-432d-9997-8cba1aaa21c8',
        '2024-22':'1fc85370-deb7-4822-8c6d-ee2b102517ea',
        '2024-18':'2baf656d-2c89-4926-847e-6cddccce5216',
        '2024-10':'b8e89e1b-b648-4d7b-bfd3-b7ad53cd4c37',
        '2023-50':'8cd3e911-d04a-4f02-8000-549d70628cdf',
        '2023-40':'1b7e488a-dca4-446f-856b-c5f8ec77d4f4',
        '2023-23':'84402f88-7174-448e-9511-53a1641abc5d',
        '2023-14':'8ceefda9-dbc2-4c3a-b6ef-bf5405c14124',
        '2023-06':'439db190-1c03-42a6-8c73-3a5f90965d50',
        '2022-49':'9ea5f768-cf0d-487e-a704-d55765dcfc7b',
        '2022-40':'51cb1586-53fc-4a53-b4e4-ae7ad86b4e70',
        '2022-33':'040d8a89-97d9-45a7-829d-69b049425d50',
        '2022-27':'9ef78122-aca5-4b37-8c58-d9905ab82121',
        '2022-21':'9633b152-b69a-45a8-ad7a-19661d5bfd16',
        '2022-05':'3856473f-a5da-44fa-a322-12d1235d0572',
        '2021-49':'75d5ac94-61d1-461b-b583-8b7826856f53',
        '2021-43':'3bd9dcc2-e8f8-4074-a54a-25c2358c9cf2',
        '2021-39':'8096e514-49db-4d3e-a485-6f30bdb3cf3d',
        '2021-31':'0714045a-2eb3-413c-8a6d-7f7a046cd5a7',
        '2021-25':'e5c811d9-d252-42e0-84bf-cca9979ddc1b',
        '2021-21':'7030f6fd-3648-47a1-9a26-58a69f57c3aa',
        '2021-17':'e8732d47-a592-4dca-ac57-9e79f3b00de4',
        '2021-10':'8814644a-bcfe-4f81-85cd-7c9e161da408',
        '2021-04':'2561c5dd-0f7a-4106-b321-d3659ecd01b6',
    }

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
        cache_path = self.cache / 'index/collinfo.json'

        # download or read it
        if cache_path.exists() and not force_update:
            print(f'[get_available_indexes][cache] {cache_path}')
            available_indexes = read_json(cache_path) # read cache
        else:
            print(f'[get_available_indexes][download] {cache_path}')
            available_indexes = download_available_indexes()
            write_json(available_indexes, cache_path) # cache results

        # return it
        return available_indexes

    def populate_results_with_athena_csvs(self, index: str, min_length: int = None, max_length: int = None) -> None:
        query = r'''
        SELECT
            url,                                              -- full url # https://www.example.com/path/index.html
            url_surtkey,                                      -- sortable url # com,example)/path/index.html
            url_host_name,                                    -- url_host_name # www.example.com
            url_host_registered_domain,                       -- short url # example.com
            content_digest,                                   -- SHA-1 hashing of content
            warc_filename,                                    -- which warc
            warc_record_offset,                               -- where to start
            warc_record_length                                -- how far to go
        FROM "ccindex"."ccindex"
        WHERE crawl = 'CC-MAIN-{index}'                     -- filter by crawl
            AND subset = 'warc'                               -- filter by subset
            AND url_host_tld = 'com'                          -- domain must end with .com
            AND fetch_status = 200                            -- must be successful request
            AND content_languages LIKE '%zho%'                -- must contain chinese
            AND content_mime_type = 'text/html'               -- only care about htmls
            AND url_host_name LIKE '%hk%'                     -- url_host_name must contain hk
        '''

        ########################################
        # read athena csvs
        ########################################

        df = pd.read_csv(self.cache / f'athena/{IndexClient.ATHENA_QUERY_EXECUTION_IDS[index]}.csv')
        df = df.drop_duplicates('content_digest') # unique digest
        df = df.sort_values('warc_record_length',ascending=False) # focus on large records
        pd.Series(df['warc_record_length'].values).plot() # ignore index
        if min_length: df = df[df['warc_record_length']>=min_length]
        if max_length: df = df[df['warc_record_length']<=max_length]

        df = df.rename(
            columns = {
                'url_surtkey': 'urlkey',
                'content_digest': 'digest',
                'warc_filename': 'filename',
                'warc_record_offset': 'offset',
                'warc_record_length': 'length',
            }
        )

        # return
        self.results = list(df.T.to_dict().values())
    
    def populate_results_with_url_filter(self, url: str, threads: int = None, force_update: bool = False) -> None:
        """Search.

        Searches the Common Crawl indexes this class was intialized with.

        Args:
            url: URL pattern to search
            threads: Number of threads to use. Enables multi-threading only if set.

        """
        self.results = get_multiple_indexes(url, self.indexes, threads, self.cache, force_update)

    def download(self, threads: int = None, force_update: bool = False, append_extract: bool = False) -> None:
        """Download.

        Downloads warc extracts for every search result in the `results` attribute.

        Args:
            threads: Number of threads to use. Enables multi-threading only if set.

        """
        self.results = get_multiple_extracts(self.results, threads, self.cache, force_update, append_extract)
