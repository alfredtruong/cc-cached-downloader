'''
1) kicks of remote queries on AWS Athena to query Common Crawl columnar indexes
2) can download from AWS with code OR log into AWS Athena or S3 for direct download (faster)
'''
# https://julien.duponchelle.info/scrapping/ai/common-crawl
# https://github.com/brevityinmotion/straylight/blob/main/notebooks/corefunctions.ipynb
# https://github.com/brevityinmotion/straylight/blob/main/notebooks/tools-commoncrawl.ipynb
# https://commoncrawl.org/blog/index-to-warc-files-and-urls-in-columnar-format

#%%
import os
import boto3
import pandas as pd
from pathlib import Path

#%%
########################################
# SETTINGS
########################################
# WHERE TO SAVE DATA
OUTPUT_DIR = Path('/home/alfred/nfs/common_crawl/athena/')

# ATHENA CONFIG
AWS_KEY = os.getenv('AWS_KEY') # populate your own credentials, Athena queries cost money (not much)
AWS_SECRET = os.getenv('AWS_SECRET') # populate your own credentials, Athena queries cost money (not much)
AWS_REGION = "us-east-1" # cc data hosted here
S3_STAGING_DIR = 's3://omgbananarepublic/' # set your own S3 working area
SCHEMA_NAME = 'cc-index' # follow the tutorial

# common crawls
INDEXES = [
  '2024-30', # batch 4, needed to rebuild ccindex table for new crawls
  '2024-26', # batch 0
  '2024-22', # batch 0
  '2024-18', # batch 0
  '2024-10', # batch 0
  '2023-50', # batch 1
  '2023-40', # batch 1
  '2023-23', # batch 1
  '2023-14', # batch 1
  '2023-06', # batch 1
  '2022-49', # batch 1
  '2022-40', # batch 1
  '2022-33', # batch 1
  '2022-27', # batch 1
  '2022-21', # batch 1
  '2022-05', # batch 1
  '2021-49', # batch 1
  '2021-43', # batch 1
  '2021-39', # batch 1
  '2021-31', # batch 1
  '2021-25', # batch 1
  '2021-21', # batch 1
  '2021-17', # batch 1
  '2021-10', # batch 1
  '2021-04', # batch 1
  '2020-50', # batch 2
  '2020-45', # batch 2
  '2020-40', # batch 2
  '2020-34', # batch 2
  '2020-29', # batch 2
  '2020-24', # batch 2
  '2020-16', # batch 2
  '2020-10', # batch 2
  '2020-05', # batch 2
  '2019-51', # batch 2
  '2019-47', # batch 2
  '2019-43', # batch 2
  '2019-39', # batch 2
  '2019-35', # batch 2
  '2019-30', # batch 2
  '2019-26', # batch 2
  '2019-22', # batch 2
  '2019-18', # batch 2
  '2019-13', # batch 2
  '2019-09', # batch 2
  '2019-04', # batch 3
  '2018-51', # batch 3
  '2018-47', # batch 3
  '2018-43', # batch 3
  '2018-39', # batch 3
  '2018-34', # batch 5
  '2018-30', # batch 5
  '2018-26', # batch 5
  '2018-22', # batch 5
  '2018-17', # batch 5
  '2018-13', # batch 5
  '2018-09', # batch 5
  '2018-05', # batch 5
  '2017-51', # batch 5
  '2017-47', # batch 5
  '2017-43', # batch 5
  '2017-39', # batch 5
  '2017-34', # batch 5
  '2017-30', # batch 5
  '2017-26', # batch 5
  '2017-22', # batch 5
  '2017-17', # batch 5
  '2017-13', # batch 5
  '2017-09', # batch 5
  '2017-04', # batch 5
  '2016-50', # batch 5
  '2016-44', # batch 5
  '2016-40', # batch 5
  '2016-36', # batch 5
  '2016-30', # batch 5
  '2016-26', # batch 5
  '2016-22', # batch 5
  '2016-18', # batch 5
  '2016-07', # batch 5
  '2015-48', # batch 5
  '2015-40', # batch 5
  '2015-35', # batch 5
  '2015-32', # batch 5
  '2015-27', # batch 5
  '2015-22', # batch 5
  '2015-18', # batch 5
  '2015-14', # batch 5
  '2015-11', # batch 5 # empty
  '2015-06', # batch 5 # empty
  '2014-52', # batch 5
  '2014-49', # batch 5
  '2014-42', # batch 5
  '2014-41', # batch 5
  '2014-35', # batch 5
  '2014-23', # batch 5
  '2014-15', # batch 5
  '2014-10', # batch 5
  '2013-48', # batch 5
  '2013-20', # batch 5
  '2012', # batch 5 # empty
  '2009-2010', # batch 5 # empty
  '2008-2009', # batch 5 # empty
]

#%%
########################################
# INIT ATHENA CLIENT
########################################
athena_client = boto3.client(
    "athena",
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET,
    region_name=AWS_REGION,
)

#%%
########################################
# QUERY (after 2018-34)
########################################
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

#%%
########################################
# QUERY WITHOUT CONTENT_LANGUAGES (inc and before 2018-34)
########################################
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
   -- AND content_languages LIKE '%zho%'                -- must contain chinese
    AND content_mime_type = 'text/html'               -- only care about htmls
    AND url_host_name LIKE '%hk%'                     -- url_host_name must contain hk
'''

#%%
########################################
# SUBMIT QUERIES TO REMOTE SERVER
########################################
execution_ids = {}
for INDEX in INDEXES[50:]: # i.e. from '2018-34'
  print(INDEX,end=':')
  query_execution = athena_client.start_query_execution(
      QueryString=query.format(index=INDEX),
      QueryExecutionContext={"Database": SCHEMA_NAME},
      ResultConfiguration={
          "OutputLocation": S3_STAGING_DIR,
          #"EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
      },
  )
  execution_id = query_execution['QueryExecutionId']
  print(execution_id)
  execution_ids[INDEX] = execution_id

#%%
'''
execution_ids = {
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
    '2018-39':'b904f635-d706-4d19-8e4c-b93751a89caa',
    # batch 5
    '2018-34':'80dd144e-48b2-4290-9ef8-1ae7a40ebad0',
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
    '2015-11':'ed6ec25b-f812-4e42-8cc9-1b055d9d79d3',
    '2015-06':'2b4ea1b7-689e-4dfb-8c97-49d9aa7b9c05',
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
    '2012':'ba4c0225-7038-4625-a29e-77a15ad846d5',
    '2009-2010':'dcf275ac-c8d2-470a-b827-55fb728f83f8',
    '2008-2009':'d6ca55f3-0abe-4673-b297-e418214ff94e',
}
'''

# %%
########################################
# CHECK QUERY STATUS
########################################
for index,execution_id in execution_ids.items():
  query_details = athena_client.get_query_execution(QueryExecutionId=execution_id)
  state = query_details['QueryExecution']['Status']['State']
  print(state)
  
#SUCCEEDED
#FAILED
#CANCELLED

########################################
# [DOWNLOAD] THROUGH BOTO3 / CODE
########################################
# %%
# partial results
response_query_result = athena_client.get_query_results(QueryExecutionId=execution_id)
print(response_query_result['ResultSet']['Rows'])

#%%
# full results
for index,execution_id in execution_ids.items():
  print(index)
  if execution_id == 'b8e89e1b-b648-4d7b-bfd3-b7ad53cd4c37':
    continue

  # get all data
  paginator = athena_client.get_paginator('get_query_results')
  result_pages = paginator.paginate(QueryExecutionId=execution_id)
  all_results = []
  for page in result_pages:
      all_results.extend(page['ResultSet']['Rows'])

  # transcribe
  data = [
    {
      'url':                     row['Data'][0]['VarCharValue'],
      'urlkey':                  row['Data'][1]['VarCharValue'], # url_surtkey
      'urlhostname':             row['Data'][2]['VarCharValue'],
      'urlhostregistereddomain': row['Data'][3]['VarCharValue'],
      'digest':                  row['Data'][4]['VarCharValue'], # digest
      'filename':                row['Data'][5]['VarCharValue'], # warc_filename
      'offset':                  row['Data'][6]['VarCharValue'], # warc_record_offset
      'length':                  row['Data'][7]['VarCharValue'], # warc_record_length
    } for row in all_results
  ]

  # create df and pickle it
  df = pd.DataFrame(data[1:]) # 1st row is header
  df.to_pickle(OUTPUT_DIR / f'{index}.pkl')

# %%
# filter results
df = pd.read_pickle(OUTPUT_DIR / f'{index}.pkl')
df2 = df.drop_duplicates('digest') # unique digest
df3 = df2.sort_values('length',ascending=False) # focus on large records


########################################
# [DOWNLOAD] DIRECTLY FROM S3 (run in bash)
########################################
#%%
for index,execution_id in execution_ids.items():
  print(f'aws s3 cp s3://omgbananarepublic/{execution_id}.csv {OUTPUT_DIR}/')

#%%
'''
aws s3 cp s3://omgbananarepublic/22e60ea5-6973-49ef-8457-27d828815a26.csv /home/alfred/nfs/common_crawl/athena/
'''

#%%
########################################
# GENERATE BASH JOBS
########################################
for index,execution_id in execution_ids.items():
	print(f'nohup python /home/alfred/nfs/code/cc-cached-downloader/run.py --index {index} --threads 100 > /home/alfred/nfs/common_crawl/output/output_{index}.txt 2>&1 & # ')
#%%

########################################
# READ ATHENA CSV
########################################
#%%
df = pd.read_csv(OUTPUT_DIR / f'{execution_ids['2024-22']}.csv')
df2 = df.drop_duplicates('content_digest') # unique digest
df3 = df2.sort_values('warc_record_length',ascending=False) # focus on large records
pd.Series(df3['warc_record_length'].values).plot() # ignore index
df3[df3['warc_record_length']>50_000]