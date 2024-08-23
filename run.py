#%%
import argparse
from comcrawl.core import IndexClient

################# SETTINGS
if True:
	parser = argparse.ArgumentParser(description='cc-cached-downloader')
	parser.add_argument('--index', help='cc index identifier', type=str, default='2024-26')
	parser.add_argument('--min_length', help='min record size', type=int, default=None)
	parser.add_argument('--max_length', help='max record size', type=int, default=None)
	parser.add_argument('--threads', help='number of threads', type=int, default=None)
	parser.add_argument('--force_update', help='overwrite existing data', type=bool, default=False)
	parser.add_argument('--output_dir', help='where to save output', type=str, default='/home/alfred/nfs/common_crawl')
	args = parser.parse_args()

	INDEX = args.index
	MIN_LENGTH = args.min_length
	MAX_LENGTH = args.max_length
	THREADS = args.threads
	FORCE_UPDATE = args.force_update
	OUTPUT_DIR = args.output_dir
else:
	INDEX = '2018-39'
	MIN_LENGTH = None
	MAX_LENGTH = None
	THREADS = 50
	FORCE_UPDATE = False
	OUTPUT_DIR = '/home/alfred/nfs/common_crawl'

#%%
################# POPULATE RESULTS WITH ATHENA CSVS
ic = IndexClient(cache = OUTPUT_DIR)
ic.init_results_with_athena_query_csvs(index=INDEX, min_length = MIN_LENGTH, max_length = MAX_LENGTH)
#len(ic.results)

#%%
################# POPULATE RESULTS VIA INDEX API URL FILTER
'''
ic = IndexClient('2024-26',cache = OUTPUT_DIR) # only consider single crawl
ic.init_results_with_url_filter('*.hk01.com') # read / save
#ic.init_results_with_url_filter('*.hk01.com', force_update=True) # overwrite
'''

#%%
################# [TESTING] REDUCE RESULTS
#ic.results = ic.results[:5] # for testing
#len(ic.results)

#%%
################# [TESTING] INSPECT RESULTS
#ic.results # for testing
#ic.results[0]

#%%
################# DOWNLOAD
ic.populate_results(force_update=FORCE_UPDATE, threads=THREADS) # single or multithreaded
#ic.populate_results(force_update=FORCE_UPDATE, threads=THREADS, append_extract=True) # actual result

#%%
################# DETECT LANGUAGE
#ic.populate_languages(force_update=FORCE_UPDATE, threads=THREADS, append_extract=True) # bool for result

################# [TESTING] INSPECT RESULTS
#%%
#[x['content'] for x in ic.results]