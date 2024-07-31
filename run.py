#%%
from comcrawl.core import IndexClient

################# ATHENA CSVS
ic = IndexClient(cache = r'C:\Users\alfred\Desktop\D_DRIVE\data') # use athena csvs
ic.populate_results_with_athena_csvs(index='2024-26',min_length = 100_000)
len(ic.results)

################# SEARCH INDEX
#ic = IndexClient('2024-26',cache = r'C:\Users\alfred\Desktop\D_DRIVE\data') # only consider single crawl
#ic.populate_results_with_url_filter('*.hk01.com') # read / save
#ic.populate_results_with_url_filter('*.hk01.com', force_update=True) # overwrite

#ic.results = ic.results[:51]
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
ic.download(force_update=False, threads=50) # multi-thread

# %%
ic.download(force_update=False) # multi-thread

# %%
[x['content'] for x in ic.results]
# %%
