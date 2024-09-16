#%%
import argparse
from comcrawl.core import IndexClient
import sys

################# SETTINGS
# is jupyter?
is_jupyter = False
for arg in sys.argv:
	if 'ipykernel_launcher' in arg:
		is_jupyter = True
print(f'[run] is_jupyter = {is_jupyter}')
#is_jupyter = True

# load settings
if not is_jupyter:
	#print('here')
	parser = argparse.ArgumentParser(description='cc-cached-downloader')
	parser.add_argument('--outdir', help='where to save output', type=str, default='/home/alfred/nfs/cc')
	parser.add_argument('--index', help='cc index identifier', type=str, default='2018-43')
	parser.add_argument('--threads', help='number of threads', type=int, default=None)
	parser.add_argument('--min_length', help='min record size', type=int, default=None)
	parser.add_argument('--max_length', help='max record size', type=int, default=None)
	args = parser.parse_args()
	print(args)
	
	OUTPUT_DIR = args.outdir
	INDEX = args.index
	THREADS = args.threads
	MIN_LENGTH = args.min_length
	MAX_LENGTH = args.max_length
else:
	#print('there')
	OUTPUT_DIR = '/home/alfred/nfs/cc_zho_2' #OUTPUT_DIR = '/home/alfred/nfs/cc_zho_hk'
	INDEX = '2024-10' #INDEX = '2018-43'
	THREADS = 50
	MIN_LENGTH = None
	MAX_LENGTH = None

################# POPULATE RESULTS WITH ATHENA CSVS
print('[run] read athena csvs')
ic = IndexClient(outdir = OUTPUT_DIR)
ic.init_results_with_athena_query_csvs(index=INDEX, min_length=MIN_LENGTH, max_length=MAX_LENGTH)
#len(ic.results)

#%%
################# POPULATE RESULTS VIA INDEX API URL FILTER
'''
ic = IndexClient('2024-26',outdir = OUTPUT_DIR) # only consider single crawl
ic.init_results_with_url_filter('*.hk01.com') # read / save

# reduce results
ic.results = ic.results[:5] # for testing
len(ic.results)

# inspect results
ic.results # for testing
ic.results[0]

# find index of record with url == 'find_url'
find_url = 'https://www.naturallyhealthierways.com/uf20200409/20200409111419221922.html'
for i,x in enumerate(ic.results):
	if x['url'] == find_url:
		print(i)

ic._save_single_record(ic.results[101])

(base) alfred@net-g14:/nfs/alfred/cc_zho_2$ ll /nfs/alfred/cc_zho_2/records/2024-10/*/*.gz # raw record
(base) alfred@net-g14:/nfs/alfred/cc_zho_2$ less /nfs/alfred/cc_zho_2/extracts/2024-10/2024-10.jsonl # extract
'''
#%%
################# DOWNLOAD
print('[run] populate results')
ic.populate_results(threads=THREADS) # single or multithreaded
'''
# inspect results
[x['content'] for x in ic.results]
'''