"""This module contains utility functions used in the core class."""

from .download import download_multiple_results
from .initialization import download_available_indexes
from .search import search_multiple_indexes
from .cache import read_json,write_json,parse_jsonlines,read_jsonl,write_jsonl