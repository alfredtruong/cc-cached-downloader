"""This module contains utility functions used in the core class."""

from .initialization import download_available_indexes
from .download import get_multiple_extracts
from .search import get_multiple_indexes
from .cache import read_json,write_json,parse_jsonlines,read_jsonl,write_jsonl,read_file,write_file,read_gzip,write_gzip