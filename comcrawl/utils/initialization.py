"""Initialization Helpers.

This module contains helper functions for
initializing the Index Client.

"""

import requests
from .types import IndexList

INDEX_LIST = "https://index.commoncrawl.org/collinfo.json"


def download_available_indexes() -> IndexList:
    """Fetches the available Common Crawl Indexes to search.

    Returns:
        A list containing available indexes and information about them.

    """
    index_list = (requests
                  .get(INDEX_LIST)
                  .json())

    # other functions don't expect this prefix
    indexes = [index["id"].replace("CC-MAIN-", "") for index in index_list]

    return indexes
