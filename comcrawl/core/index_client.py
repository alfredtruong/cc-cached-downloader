"""Index Client.

This module contains the core object of the package.

"""

import logging
from pathlib import Path
from ..utils.types import Index, IndexList, ResultList
from ..utils import download_available_indexes,get_multiple_indexes,get_multiple_extracts,read_json,write_json,read_from_jsonl_cache,extract_cache_path,jsonl_cache_path
import pandas as pd

# with HK in domain filter
EXECUTION_IDS = {
    # batch 4
    '2024-30':'6a0f498f-6c48-4377-a953-d3aafb654abe',
    # batch 0
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
    '2018-43':'d7003854-2816-4c83-b08c-66d456b9ce26',
    '2018-39':'b904f635-d706-4d19-8e4c-b93751a89caa', # Starting with crawl CC-MAIN-2018-39 we added a language classification field (‘content-languages’) to the columnar indexes, WAT files, and WARC metadata for all subsequent crawls. The CLD2 classifier was used, and includes up to three languages per document. We use the ISO-639-3 (three-character) language codes.
    # batch 5 # content_languages added after 2018-34 # cc added language annotation, https://commoncrawl.org/blog/august-2018-crawl-archive-now-available
    '2018-34':'80dd144e-48b2-4290-9ef8-1ae7a40ebad0', # warc format error, ‍Please note that the WARC files of August 2018 (CC-MAIN-2018-34) are affected by a WARC format error and contain an extra \r\n between HTTP header and payload content. Also the given "Content-Length" is off by 2 bytes. For more information about this bug see this post on our user forum.
    '2018-30':'933d7a44-0279-401d-8c1a-c7bce5949cb0',
    '2018-26':'94cb662c-11a8-4fc7-9a28-112a3d374c9a',
    '2018-22':'adb0a017-acdd-48ee-8cb6-a4357ae2f4f6',
    '2018-17':'7f8ded0b-09ec-4315-b98c-5de7f69a4665',
    '2018-13':'56625e31-4d9a-4ae4-bacf-832936f0b2ba',
    '2018-09':'69d8600e-a51c-4f40-b79a-81687fa17b9b',
    '2018-05':'71f7c3d3-d7a9-41b9-abf2-af2bd60b2df4',
    '2017-51':'8991a6a2-df85-42fc-b27a-d51fd1383084',
    '2017-47':'a9091199-3467-4591-a570-24add079752a',
    '2017-43':'41719e8d-e455-4867-85bf-446c9b9347e2',
    '2017-39':'d3fc2c62-4ac3-42e1-bfb4-315b9784afd9',
    '2017-34':'fb38c29c-875e-4cd5-b20a-c769ffb8e30c',
    '2017-30':'4102e6f5-6276-47d5-98e2-fc5aabcc9b5a',
    '2017-26':'376f10cd-d7c4-42be-918d-2bbf76abe35c',
    '2017-22':'a23b931d-61b8-4173-923d-772fdad772ad',
    '2017-17':'cd9a27c5-36ca-473c-bcb5-1c43cc2f2d47',
    '2017-13':'bf246719-0b14-48ce-80c0-ad16bda91f22',
    '2017-09':'a96fb92f-1abb-49ce-8407-9bea76b829ae',
    '2017-04':'6dc387e6-97e9-445d-8e81-105dee45a16d',
    '2016-50':'fe96ea74-8857-4263-83e5-19c2a1b46672',
    '2016-44':'7735461d-901c-4f0b-9bdf-4dd5e58faa64',
    '2016-40':'2e7c4b5e-b3ee-4d04-8229-8bdafea98acd',
    '2016-36':'74465f97-c529-4ec9-83e6-479f307dd3e0',
    '2016-30':'e77ed407-0f9c-4d55-802c-7824f7a888df',
    '2016-26':'8b335ceb-73da-448b-946f-de972ac7d44b',
    '2016-22':'fab05947-d03b-4116-9069-eb8380911085',
    '2016-18':'decfc66e-5a56-4e42-a929-3c7e29c961f4',
    '2016-07':'4837bf45-1e23-46df-8e28-85783bcce21e',
    '2015-48':'a58f4a36-bbc5-4783-a6f2-cf544a59403e',
    '2015-40':'f581ec04-7288-40cc-925f-a353bf38d5ca',
    '2015-35':'20766d15-c470-4957-a722-8b8c3b57fea8',
    '2015-32':'48ccf194-414f-43d6-9593-ddc5b4ee6616',
    '2015-27':'a725feee-415c-4bb6-ba9e-99097c499de5',
    '2015-22':'abd351c0-0dea-4909-acfa-22acc05e8e58',
    '2015-18':'4e4ceda2-5f40-42ec-bfad-46b287fd9bcf',
    '2015-14':'40ceca4b-e888-492d-93ad-d621efc7733e',
    '2015-11':'ed6ec25b-f812-4e42-8cc9-1b055d9d79d3', # empty
    '2015-06':'2b4ea1b7-689e-4dfb-8c97-49d9aa7b9c05', # empty
    '2014-52':'7eec65c0-51a8-4e8e-8d2d-4ae047cd8808',
    '2014-49':'8d27f3dc-81df-4093-abb8-862bdf1339dc',
    '2014-42':'61fe46ca-bf43-404f-b364-62a23b7378a2',
    '2014-41':'a6c3b4e5-58f7-4264-af12-f032edfe4cf1',
    '2014-35':'fdef3c09-745d-4348-a60f-cae93b76a538',
    '2014-23':'43a7c373-8d32-48b9-b757-c8745bbfa468',
    '2014-15':'3eefe4ba-f1ec-4637-ba02-4adb83314163',
    '2014-10':'9464fda1-0f81-477b-819d-18c287b1e3f5',
    '2013-48':'43897e03-f14f-4e81-8b25-5b57ab3c20b2',
    '2013-20':'7e120e99-f55a-42fa-b5e4-b57db695b4f1',
    '2012':'ba4c0225-7038-4625-a29e-77a15ad846d5', # empty
    '2009-2010':'dcf275ac-c8d2-470a-b827-55fb728f83f8', # empty
    '2008-2009':'d6ca55f3-0abe-4673-b297-e418214ff94e', # empty
}

# no HK in domain filter
EXECUTION_IDS = {
    # batch 6
    '2024-33':'deefcbdc-fa09-4c3e-be8b-c779f83026d2',
    '2024-30':'cb36fb15-5e81-4f49-8140-740bd9679de7',
    '2024-26':'565360b1-ceb5-465b-af18-eaf2f6ac00b6',
    '2024-22':'46ca0de8-9cf4-4209-935c-114d073e383f',
    '2024-18':'544069b8-419e-40fe-8723-2cd4d8dfe91b',
    '2024-10':'69c7d697-931d-469c-baac-d4df5c891e24',
    '2023-50':'5aa0286d-0303-4e62-8381-9bae17dcbe23',
    '2023-40':'7b90b3ed-8032-40e1-b48c-9ffca563ac12',
    '2023-23':'7581324f-f4db-4839-ab68-924d7952134e',
    '2023-14':'f41161b2-e551-494b-b8f0-8be1e41bd847',
    '2023-06':'68ca8223-d2de-4cdb-a729-7fd6c9dff8da',
    '2022-49':'dadbe941-8894-49ae-abdb-588a468c45f4',
    '2022-40':'26e2331a-d570-45fb-bdb3-749fac5229b1',
    '2022-33':'c68263f6-c50e-4362-91f0-d4ca9bea9c04',
    '2022-27':'bb054706-54d1-4bbf-92f8-4f9bbc1add67',
    '2022-21':'145b3804-621a-48cf-b8f5-0cc8d178a639',
    '2022-05':'66eb2116-9e4a-4b6f-b70c-27c7b48c9815',
    '2021-49':'a794541e-3255-4aed-92c2-b0a6ae8f0c3c',
    '2021-43':'7e920324-d25f-43b7-9340-328ed1e3acf2',
    '2021-39':'220a787d-2560-4f1e-95e8-29f4be678973',
    '2021-31':'330d7819-8706-4f66-b676-f6920bec7c1a',
    '2021-25':'0714ec35-6ea1-4877-a2a3-7bd3b60fbafb',
    '2021-21':'b4e64006-7016-4b12-af65-a292bf83b83a',
    '2021-17':'7486eefa-56c8-4162-a1a5-cda1882ed9d5',
    '2021-10':'7987d7a5-6d1a-4ca0-8fe0-8c63a3a65e71',
    '2021-04':'0cbe5ac1-1076-4671-9b33-510898f78aca',
    '2020-50':'17e9f840-ca1b-4916-b4ba-01cd2f2432f5',
    '2020-45':'db8a7adc-26d0-4a65-9e7c-4a221d69f1a2',
    '2020-40':'63775e36-122e-4fdd-86ff-cf17b0f9237d',
    '2020-34':'32bc0bde-6f28-4f27-9bd3-39fcdbd43952',
    '2020-29':'a0c984ab-1342-42f1-9dab-e4a353d0e2b7',
    '2020-24':'cea84eec-391e-4e0b-9c28-3206bf2bbd25',
    '2020-16':'600328f6-38dc-42e4-ac8a-7e160d33c8b1',
    '2020-10':'f624adaa-cf5c-4604-8c1f-0d974da9230a',
    '2020-05':'6b373ad0-7f29-48f1-a563-c4d80128591f',
    '2019-51':'f36f861f-ab3c-49fb-a019-f88f2d5390c2',
    '2019-47':'aa33c540-9f3e-4bb8-b7da-d9a07750c803',
    '2019-43':'ee97c786-885f-4cba-8af2-044aa38343a3',
    '2019-39':'2317f096-2e16-41ae-a09b-69b50f702380',
    '2019-35':'a344bdab-4ad7-4eaa-b7cd-d7121d0279ea',
    '2019-30':'6848928b-4634-4a97-9944-4c9f7e3f03de',
    '2019-26':'c80dce22-a9c4-4253-83a0-4d89cb4c0afc',
    '2019-22':'a019e62c-461b-472c-b614-f38fa7681de7',
    '2019-18':'44b6e9cd-f586-456c-aeda-fbc66c6504df',
    '2019-13':'bb7bf540-67f7-4a5d-b88c-83d55e70d965',
    '2019-09':'a2b3bf98-e98f-40b1-881e-81ba8c720ce3',
    '2019-04':'6e3e0db5-6594-4caf-b8b6-823c560d1374',
    '2018-51':'3549d3eb-8911-4666-9511-98e947b96f24',
    '2018-47':'8f242c9a-6215-40dc-bb16-71e622edb5d0',
    '2018-43':'4e6ec6db-df2c-4b55-8854-9d0faf04c800',
    '2018-39':'034a5b1a-d03a-4096-a28c-3c23bf536434',
    '2018-34':'e303ff3e-183f-4392-9037-4538260448a9',
}

class IndexClient:
    """Common Crawl Index Client.

    Query Common Crawl indexes and download pages from the corresponding Common Crawl AWS S3 Buckets locations.

    Attributes:
        results: The list of results after calling the search method.

    """
    # Athena query csv files associated to above query, do this manually
    # https://commoncrawl.org/errata/missing-language-classification
    # language classification added since 2018-39

    def __init__(self, index: Index = None, outdir: str = 'data/', verbose: bool = False) -> None:
        """Initializes the class instance.

        Args:
            indexes: Index to focus on.
            verbose: Whether to print debug level logs to the console while making HTTP requests.

        """
        if verbose:
            logging.basicConfig(level=logging.DEBUG)

        self.indexes = [index] if isinstance(index,str) else index # ensure index saved as a list
        self.outdir = Path(outdir)
        self.results: ResultList = []

    def get_available_indexes(self) -> IndexList:
        """Show all available indexes

        Searches the Common Crawl indexes this class was intialized with.

        Args:
            url: URL pattern to search
            threads: Number of threads to use. Enables multi-threading only if set.

        """
        # where we would outdir the results
        cache_path = self.outdir / 'index/collinfo.json'

        # download or read it
        if cache_path.exists():
            print(f'[get_available_indexes][cache] {cache_path}')
            available_indexes = read_json(cache_path) # read cache
        else:
            print(f'[get_available_indexes][download] {cache_path}')
            available_indexes = download_available_indexes()
            write_json(available_indexes, cache_path) # cache results

        # return it
        return available_indexes

    def init_results_with_athena_query_csvs(self, index: str, min_length: int = None, max_length: int = None, do_plot: bool = False) -> None:
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

        # read athena csvs
        print('[init_results_with_athena_query_csvs] read_csv')
        df = pd.read_csv(self.outdir / f'athena/{EXECUTION_IDS[index]}.csv')
        df = df.drop_duplicates('content_digest') # only unique digests
        df = df.sort_values('warc_record_length',ascending=False) # large records first
        if do_plot: pd.Series(df['warc_record_length'].values).plot()
        if min_length: df = df[df['warc_record_length']>=min_length]
        if max_length: df = df[df['warc_record_length']<=max_length]

        # rename for downloader
        df = df.rename(
            columns = {
                'url_surtkey': 'urlkey',
                'content_digest': 'digest',
                'warc_filename': 'filename',
                'warc_record_offset': 'offset',
                'warc_record_length': 'length',
            }
        )

        # build results
        results = list(df.T.to_dict().values())

        # get cached results
        print('[init_results_with_athena_query_csvs] read_from_jsonl_cache')
        cached_filepaths = read_from_jsonl_cache(jsonl_cache_path(index,self.outdir),'filepath') # read info from all cached jsonls
        cached_filepaths = set(cached_filepaths) # extract filepath identifiers

        # add cache status to results
        print('[init_results_with_athena_query_csvs] mark cached')
        cache_counter = 0
        for result in results:
            cache_path = extract_cache_path(result,self.outdir)
            if str(cache_path) in cached_filepaths:
                result['cached'] = True
                cache_counter += 1
            else:
                result['cached'] = False
        print(f'[init_results_with_athena_query_csvs] {cache_counter} / {len(cached_filepaths)}')

        ########################################
        # save
        ########################################
        print('[init_results_with_athena_query_csvs] done')
        self.results = results
    
    def init_results_with_url_filter(self, url: str, threads: int = None) -> None:
        """Search.

        Searches the Common Crawl indexes this class was intialized with.

        Args:
            url: URL pattern to search
            threads: Number of threads to use. Enables multi-threading only if set.

        """
        self.results = get_multiple_indexes(url, self.indexes, self.outdir, threads)

    def populate_results(self, threads: int = None) -> None:
        """Download.

        Downloads warc extracts for every search result in the `results` attribute.

        Args:
            threads: Number of threads to use. Enables multi-threading only if set.

        """
        self.results = get_multiple_extracts(self.results, self.outdir, threads)
