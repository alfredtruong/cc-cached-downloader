# introduction

## what is Common Crawl?
Common Crawl project is _"open repository of web crawl data that can be accessed and analyzed by anyone"_  
a [search index](https://index.commoncrawl.org) is provided that lets you search at domain level  
search results contains link and byte offset to a specific record in [AWS S3 buckets](https://commoncrawl.s3.amazonaws.com/cc-index/collections/index.html) for targeted downloaded  
you can also query for records with AWS Athena queries without needing to download each index

## what does repo do
helps identify records of interest from common crawl  
can loop over said records to  
   1. download and 
   2. extract its contents

has caching mechanism to allow reruns (ensure all records downloaded)

## how to identify records of interest
you can either gather these by 
1. searching at domain name level via the CDX index api (less flexible) or
2. use AWS Athena to query / dump a csv to specify all records of interest (very flexible)

# download usage
## with CDX index api
```python
from comcrawl.core import IndexClient

# crawl of interest + output location
ic = IndexClient('2024-26',cache = '/home/alfred/nfs/common_crawl')

# identify records to scrape (populate `ic.results` list of records to download)
ic.init_results_with_url_filter("reddit.com/r/MachineLearning/*")
#ic.init_results_with_url_filter('*.hk01.com') # read / save
#ic.init_results_with_url_filter('*.hk01.com', force_update=True) # overwrite

# downloads and extracts each record
ic.populate_results()
first_record = ic.results[0]["content"]
```

## with AWS Athena
```python
from comcrawl.core import IndexClient

# crawl of interest + output location
ic = IndexClient(cache = '/home/alfred/nfs/common_crawl') # use athena csvs

# identify records to scrape (populate `ic.results` list of records to download)
ic.init_results_with_athena_query_csvs(index=INDEX, min_length = MIN_LENGTH, max_length = MAX_LENGTH)
# you need to update IndexClient.ATHENA_QUERY_EXECUTION_IDS with the AWS Athena query csv hash

# download and extract each record
ic.populate_results()
first_record = ic.results[0]["content"]
```

# AWS Athena query usage
look at
`comcrawl/utils/athena.py`

### Multithreading
can multithread the search and/or download by specifying number of threads
(don't overdo this as could stress Common Crawl servers, [Code of Conduct](#code-of-conduct)).

```python
from comcrawl.core import IndexClient

client = IndexClient()

client.init_results_with_url_filter("reddit.com/r/MachineLearning/*", threads=4)
client.populate_results(threads=4)
```

### removing duplicates & saving
e.g. use [pandas](https://github.com/pandas-dev/pandas) say to filter out duplicate results and persist to disk:

```python
from comcrawl.core import IndexClient
import pandas as pd

client = IndexClient()
client.init_results_with_url_filter("reddit.com/r/MachineLearning/*")

client.results = (pd.DataFrame(client.results)
                  .sort_values(by="timestamp")
                  .drop_duplicates("urlkey", keep="last")
                  .to_dict("records"))
client.populate_results()

pd.DataFrame(client.results).to_csv("results.csv")
```

The urlkey alone might not be sufficient here, so you might want to write a function to compute a custom id from the results' properties for the removal of duplicates.

### Logging HTTP requests
can enable logging to debug HTTP requests

```python
from comcrawl.core import IndexClient

client = IndexClient(verbose=True)
client.init_results_with_url_filter("reddit.com/r/MachineLearning/*")
client.populate_results()
```

## Code of Conduct
please beware of [guidelines](https://groups.google.com/forum/#!msg/common-crawl/3QmQjFA_3y4/vTbhGqIBBQAJ) posted by Common Crawl maintainers