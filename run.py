#%%
import argparse
from comcrawl.core import IndexClient
import sys

################# SETTINGS
if len(sys.argv) > 2:
	parser = argparse.ArgumentParser(description='cc-cached-downloader')
	parser.add_argument('--index', help='cc index identifier', type=str, default='2024-26')
	parser.add_argument('--min_length', help='min record size', type=int, default=None)
	parser.add_argument('--max_length', help='max record size', type=int, default=None)
	parser.add_argument('--threads', help='number of threads', type=int, default=None)
	parser.add_argument('--outdir', help='where to save output', type=str, default='/home/alfred/nfs/cc')
	args = parser.parse_args()

	INDEX = args.index
	MIN_LENGTH = args.min_length
	MAX_LENGTH = args.max_length
	THREADS = args.threads
	OUTPUT_DIR = args.outdir
else:
	INDEX = '2018-39'
	MIN_LENGTH = None
	MAX_LENGTH = None
	THREADS = 50
	OUTPUT_DIR = '/home/alfred/nfs/cc_zho_hk'
	OUTPUT_DIR = '/home/alfred/nfs/cc_zho'

#%%
################# POPULATE RESULTS WITH ATHENA CSVS
ic = IndexClient(outdir = OUTPUT_DIR)
ic.init_results_with_athena_query_csvs(index=INDEX, min_length=MIN_LENGTH, max_length=MAX_LENGTH)
#len(ic.results)

#%%
################# POPULATE RESULTS VIA INDEX API URL FILTER
'''
ic = IndexClient('2024-26',outdir = OUTPUT_DIR) # only consider single crawl
ic.init_results_with_url_filter('*.hk01.com') # read / save
'''

#%%
################# [TESTING] REDUCE RESULTS
'''
ic.results = ic.results[:5] # for testing
len(ic.results)
'''

#%%
################# [TESTING] INSPECT RESULTS
'''
ic.results # for testing
ic.results[0]
'''

#%%
################# DOWNLOAD
ic.populate_results(threads=THREADS) # single or multithreaded

################# [TESTING] INSPECT RESULTS
#%%
#[x['content'] for x in ic.results]