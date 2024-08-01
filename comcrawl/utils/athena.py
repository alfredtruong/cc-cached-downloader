'''
1) kicks of remote queries on AWS Athena to query Common Crawl columnar indexes
2) can download from AWS with code OR log into AWS Athena or S3 for direct download (faster)
'''
# https://julien.duponchelle.info/scrapping/ai/common-crawl
# https://github.com/brevityinmotion/straylight/blob/main/notebooks/corefunctions.ipynb
# https://github.com/brevityinmotion/straylight/blob/main/notebooks/tools-commoncrawl.ipynb

#%%
import os
import boto3
import pandas as pd
from pathlib import Path

OUTPUT_DIR = '/home/alfred/nfs/common_crawl'

#%%
########################################
# configure your AWS Athena connnection
########################################
# https://commoncrawl.org/blog/index-to-warc-files-and-urls-in-columnar-format
AWS_KEY = os.getenv('AWS_KEY') # populate your own credentials, Athena queries cost money (not much)
AWS_SECRET = os.getenv('AWS_SECRET') # populate your own credentials, Athena queries cost money (not much)
AWS_REGION = "us-east-1" # cc data hosted here
S3_STAGING_DIR = 's3://omgbananarepublic/' # set your own S3 working area
SCHEMA_NAME = 'cc-index' # follow the tutorial

# common crawls
INDEXES = [
  '2024-30', # no data
  '2024-26', # done
  '2024-22', # done
  '2024-18', # done
  '2024-10', # done
  '2023-50', # done
  '2023-40', # done
  '2023-23', # done
  '2023-14', # done
  '2023-06', # done
  '2022-49', # done
  '2022-40', # done
  '2022-33', # done
  '2022-27', # done
  '2022-21', # done
  '2022-05', # done
  '2021-49', # done
  '2021-43', # done
  '2021-39', # done
  '2021-31', # done
  '2021-25', # done
  '2021-21', # done
  '2021-17', # done
  '2021-10', # done
  '2021-04', # done
  '2020-50',
  '2020-45',
  '2020-40',
  '2020-34',
  '2020-29',
  '2020-24',
  '2020-16',
  '2020-10',
  '2020-05',
  '2019-51',
  '2019-47',
  '2019-43',
  '2019-39',
  '2019-35',
  '2019-30',
  '2019-26',
  '2019-22',
  '2019-18',
  '2019-13',
  '2019-09',
  '2019-04',
  '2018-51',
  '2018-47',
  '2018-43',
  '2018-39',
  '2018-34',
  '2018-30',
  '2018-26',
  '2018-22',
  '2018-17',
  '2018-13',
  '2018-09',
  '2018-05',
  '2017-51',
  '2017-47',
  '2017-43',
  '2017-39',
  '2017-34',
  '2017-30',
  '2017-26',
  '2017-22',
  '2017-17',
  '2017-13',
  '2017-09',
  '2017-04',
  '2016-50',
  '2016-44',
  '2016-40',
  '2016-36',
  '2016-30',
  '2016-26',
  '2016-22',
  '2016-18',
  '2016-07',
  '2015-48',
  '2015-40',
  '2015-35',
  '2015-32',
  '2015-27',
  '2015-22',
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
# init athena_client
########################################
athena_client = boto3.client(
    "athena",
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET,
    region_name=AWS_REGION,
)

#%%
########################################
# query
########################################
#%%
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
# submit queries
########################################
execution_ids = {}
for INDEX in INDEXES[5:25]:
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
'''


# %%
########################################
# check statuses
########################################
for index,execution_id in execution_ids.items():
  query_details = athena_client.get_query_execution(QueryExecutionId=execution_id)
  state = query_details['QueryExecution']['Status']['State']
  print(state)
  
#SUCCEEDED
#FAILED
#CANCELLED

########################################
# [download results] with code
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
  df.to_pickle(Path(OUTPUT_DIR) / f'athena/{index}.pkl')

# %%

# filter results
df = pd.read_pickle(Path(OUTPUT_DIR) / f'athena/{index}.pkl')
df2 = df.drop_duplicates('digest') # unique digest
df3 = df2.sort_values('length',ascending=False) # focus on large records


########################################
# [download results] directly from S3
########################################
#%%
for index,execution_id in execution_ids.items():
  print(f'aws s3 cp s3://omgbananarepublic/{execution_id}.csv .')



########################################
# read athena csvs
########################################
#%%
df = pd.read_csv(Path(OUTPUT_DIR) / f'athena/{execution_ids['2024-22']}.csv')
df2 = df.drop_duplicates('content_digest') # unique digest
df3 = df2.sort_values('warc_record_length',ascending=False) # focus on large records
pd.Series(df3['warc_record_length'].values).plot() # ignore index
df3[df3['warc_record_length']>50_000]
