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
  '2024-30', # batch 0, no data
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
  '2018-34', # batch 3, doesnt work, no language identifier
  '2018-30', # batch 3, doesnt work, no language identifier
  '2018-26', # batch 3, doesnt work, no language identifier
  '2018-22', # batch 3, doesnt work, no language identifier
  '2018-17', # batch 3, doesnt work, no language identifier
  '2018-13', # batch 3
  '2018-09', # batch 3
  '2018-05', # batch 3
  '2017-51', # batch 3
  '2017-47', # batch 3
  '2017-43', # batch 3
  '2017-39', # batch 3
  '2017-34', # batch 3
  '2017-30', # batch 3
  '2017-26', # batch 3
  '2017-22', # batch 3
  '2017-17', # batch 3
  '2017-13', # batch 3
  '2017-09', # batch 3
  '2017-04', # batch 3
  '2016-50', # batch 3
  '2016-44', # batch 3
  '2016-40', # batch 3
  '2016-36', # batch 3
  '2016-30', # batch 3
  '2016-26', # batch 3
  '2016-22', # batch 3
  '2016-18', # batch 3
  '2016-07', # batch 3
  '2015-48', # batch 3
  '2015-40', # batch 3
  '2015-35', # batch 3
  '2015-32', # batch 3
  '2015-27', # batch 3
  '2015-22', # batch 3
  '2015-18',
  '2015-14',
  '2015-11',
  '2015-06',
  '2014-52',
  '2014-49',
  '2014-42',
  '2014-41',
  '2014-35',
  '2014-23',
  '2014-15',
  '2014-10',
  '2013-48',
  '2013-20',
  '2012',
  '2009-2010',
  '2008-2009'
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
# QUERY
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
# SUBMIT QUERIES TO REMOTE SERVER
########################################
execution_ids = {}
for INDEX in INDEXES[45:75]:
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
    '2018-43':'d7003854-2816-4c83-b08c-66d456b9ce26',
    '2018-39':'b904f635-d706-4d19-8e4c-b93751a89caa',
    '2018-34':'82ef359f-ed2c-4de2-b98a-ae0a04a3d45b',
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
# [DOWNLOAD] DIRECTLY FROM S3
########################################
#%%
for index,execution_id in execution_ids.items():
  print(f'aws s3 cp s3://omgbananarepublic/{execution_id}.csv {OUTPUT_DIR}/')

'''
aws s3 cp s3://omgbananarepublic/22e60ea5-6973-49ef-8457-27d828815a26.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/66f2c442-be99-4cf5-a1fd-a56d584e2378.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/efc196b9-7134-4674-b6ee-e1543486c943.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/d7003854-2816-4c83-b08c-66d456b9ce26.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/b904f635-d706-4d19-8e4c-b93751a89caa.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/82ef359f-ed2c-4de2-b98a-ae0a04a3d45b.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/6a3d0035-385c-466c-95e5-49740c0432e6.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/d0489b63-4af6-415d-bf62-ea0b0de605fc.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/342a32b3-fc2a-4402-ba17-fbefc2bfbd89.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/7ad9a04d-bb77-48a1-9148-55ea6123e01f.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/76ccbb4f-b73d-43b9-a463-73f82982f6d7.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/69a0c17e-2e25-4abc-81bf-9fa043b994a3.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/cfb08e74-0eb1-4683-831a-930cd94d745d.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/b904ec38-e5e3-442a-a100-3fa5a80535df.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/2ba02255-d53a-494f-b3a4-2a51397cbd39.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/5c77520b-a666-44fb-962e-9b0df8d98d4a.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/f0dce51f-7f58-4315-aafe-c3acf54ac9bd.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/f79bf0ae-1a9c-4c28-9d00-76b41ab9f974.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/fc2c6220-80e4-4401-a3c1-967668ff3e8f.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/f0323e2e-1cba-4256-a7e3-3f4c121ea244.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/7bd7173d-ee8b-405e-a8b8-abd5e80d467f.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/128dafbc-4db5-4504-ba73-d5f1c4fca1e6.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/78e9fa9a-0cdc-4a67-b6b5-e55f19454351.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/f7c32b52-e24a-4da6-b4d8-d13b50d5b2fd.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/87ee952e-b934-4ada-bce7-3d9593c7af2d.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/089f5f93-9e7f-48c2-9d8f-d45d2be380c9.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/9fdacc88-644c-404d-87d6-8d3af21a34cb.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/be097f0f-00ae-4fd6-8595-a881d39cf815.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/fa27f0d5-c352-45ba-a3c6-a288c0173b28.csv /home/alfred/nfs/common_crawl/athena/
aws s3 cp s3://omgbananarepublic/c9bb80b4-78ba-4d80-acc4-292696da4fe0.csv /home/alfred/nfs/common_crawl/athena/
'''

########################################
# READ ATHENA CSV
########################################
#%%
df = pd.read_csv(OUTPUT_DIR / f'{execution_ids['2024-22']}.csv')
df2 = df.drop_duplicates('content_digest') # unique digest
df3 = df2.sort_values('warc_record_length',ascending=False) # focus on large records
pd.Series(df3['warc_record_length'].values).plot() # ignore index
df3[df3['warc_record_length']>50_000]

########################################
# GENERATE BASH JOBS
########################################
#%%
for index,execution_id in execution_ids.items():
	print(f'nohup python /home/alfred/nfs/code/cc-cached-downloader/run.py --index {index} --threads 100 > /home/alfred/nfs/common_crawl/output/output_{index}.txt 2>&1 & # ')
#%%