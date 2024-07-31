#%%
from comcrawl.core import IndexClient

OUTPUT_DIR = '/home/alfred/nfs/common_crawl'

#%%
################# USE ATHENA CSVS
ic = IndexClient(cache = OUTPUT_DIR) # use athena csvs
ic.populate_results_with_athena_csvs(index='2024-26') #,min_length = 100_000)
#len(ic.results)

#%%
################# SEARCH INDEX
# populate
ic = IndexClient('2024-26',cache = OUTPUT_DIR) # only consider single crawl
ic.populate_results_with_url_filter('*.hk01.com') # read / save
#ic.populate_results_with_url_filter('*.hk01.com', force_update=True) # overwrite

#%%
# do a few for testing
ic.results = ic.results[:5]
#len(ic.results)

#%%
################# inspect results
#ic.results
#ic.results[0]

#%%
#ic.download(force_update=True) # single thread

# %%
#ic.download(force_update=True, threads=5) # multi-thread

#%%
################# INITIATE DOWNLOAD
# multi-thread
ic.download(force_update=False, threads=250)

#%%
# single-thread
ic.download(force_update=False)

#%%
################# INSPECT RESULTS
[x['content'] for x in ic.results]
