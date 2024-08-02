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
    # https://commoncrawl.org/errata/missing-language-classification
    # language classification added since 2018-39
    ATHENA_QUERY_EXECUTION_IDS = {
        # batch 0
        #'2024-30':'5ef8e286-09bd-44f2-aca5-515cf7fe9a24', # no data
        '2024-26':'f19e4aad-c0cb-432d-9997-8cba1aaa21c8',
        '2024-22':'1fc85370-deb7-4822-8c6d-ee2b102517ea',
        '2024-18':'2baf656d-2c89-4926-847e-6cddccce5216',
        '2024-10':'b8e89e1b-b648-4d7b-bfd3-b7ad53cd4c37',
        # batch 1
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
        # batch 2
        '2020-50':'0aa1ae0e-c9b8-4ea9-8e84-5907c6a92f02',
        '2020-45':'a7b94a67-3aee-48ff-8a3e-4b1770273f3f',
        '2020-40':'1bbd6ad5-21b3-4340-ba10-ed25444af869',
        '2020-34':'108169e9-a038-4e30-b282-4d829faaf80d',
        '2020-29':'2c136624-fc94-469a-a355-b64e80cdb160',
        '2020-24':'c6bfa4db-f9b3-4252-aea4-ec67ecc20b59',
        '2020-16':'088a46f4-8724-450b-87eb-4584f28e40de',
        '2020-10':'e4972c3b-c21e-4975-96a8-335256e62205',
        '2020-05':'b289e41e-91da-4eb6-97c5-0a0363938717',
        '2019-51':'5b8a8c7d-020d-4f3e-9d14-3daaccfb5ff2',
        '2019-47':'f5f23741-2062-4335-9f92-cc731388cacf',
        '2019-43':'8a4d0d55-365c-4c53-8b8e-f127e388b3d8',
        '2019-39':'e3054857-4bfb-49c3-a804-47be43f0a531',
        '2019-35':'db0e7418-9066-481f-81a4-a155cf008722',
        '2019-30':'0f4a68bb-2491-4433-af5a-00beb63eb028',
        '2019-26':'4f130d29-bf40-48af-b8ca-1d705ee399df',
        '2019-22':'9a637fa6-bab1-4807-8a18-ebfb2b73f8dc',
        '2019-18':'321a0ed1-a9bc-4f21-b85e-3ef8782077ce',
        '2019-13':'2489b2f6-fd95-4e8f-b697-aa41cd590d09',
        '2019-09':'94fbaa98-6418-40ab-918e-704ca027d1a1',
        # batch 3
        '2019-04':'22e60ea5-6973-49ef-8457-27d828815a26',
        '2018-51':'66f2c442-be99-4cf5-a1fd-a56d584e2378',
        '2018-47':'efc196b9-7134-4674-b6ee-e1543486c943',
        '2018-43':'d7003854-2816-4c83-b08c-66d456b9ce26', # cc added language annotation, https://commoncrawl.org/blog/august-2018-crawl-archive-now-available
        '2018-39':'b904f635-d706-4d19-8e4c-b93751a89caa',
        '2018-34':'82ef359f-ed2c-4de2-b98a-ae0a04a3d45b', # warc format error, ‍Please note that the WARC files of August 2018 (CC-MAIN-2018-34) are affected by a WARC format error and contain an extra \r\n between HTTP header and payload content. Also the given "Content-Length" is off by 2 bytes. For more information about this bug see this post on our user forum.
        '2018-30':'6a3d0035-385c-466c-95e5-49740c0432e6',
        '2018-26':'d0489b63-4af6-415d-bf62-ea0b0de605fc',
        '2018-22':'342a32b3-fc2a-4402-ba17-fbefc2bfbd89',
        '2018-17':'7ad9a04d-bb77-48a1-9148-55ea6123e01f',
        '2018-13':'76ccbb4f-b73d-43b9-a463-73f82982f6d7',
        '2018-09':'69a0c17e-2e25-4abc-81bf-9fa043b994a3',
        '2018-05':'cfb08e74-0eb1-4683-831a-930cd94d745d',
        '2017-51':'b904ec38-e5e3-442a-a100-3fa5a80535df',
        '2017-47':'2ba02255-d53a-494f-b3a4-2a51397cbd39',
        '2017-43':'5c77520b-a666-44fb-962e-9b0df8d98d4a',
        '2017-39':'f0dce51f-7f58-4315-aafe-c3acf54ac9bd',
        '2017-34':'f79bf0ae-1a9c-4c28-9d00-76b41ab9f974',
        '2017-30':'fc2c6220-80e4-4401-a3c1-967668ff3e8f',
        '2017-26':'f0323e2e-1cba-4256-a7e3-3f4c121ea244',
        '2017-22':'7bd7173d-ee8b-405e-a8b8-abd5e80d467f',
        '2017-17':'128dafbc-4db5-4504-ba73-d5f1c4fca1e6',
        '2017-13':'78e9fa9a-0cdc-4a67-b6b5-e55f19454351',
        '2017-09':'f7c32b52-e24a-4da6-b4d8-d13b50d5b2fd',
        '2017-04':'87ee952e-b934-4ada-bce7-3d9593c7af2d',
        '2016-50':'089f5f93-9e7f-48c2-9d8f-d45d2be380c9',
        '2016-44':'9fdacc88-644c-404d-87d6-8d3af21a34cb',
        '2016-40':'be097f0f-00ae-4fd6-8595-a881d39cf815',
        '2016-36':'fa27f0d5-c352-45ba-a3c6-a288c0173b28',
        '2016-30':'c9bb80b4-78ba-4d80-acc4-292696da4fe0',
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
        #pd.Series(df['warc_record_length'].values).plot() # ignore index
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
