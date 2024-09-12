import pandas as pd
from comcrawl.utils.search import get_single_index,get_multiple_indexes


def test_get_single_index(snapshot):
    results = get_single_index("https://index.commoncrawl.org/*", "2019-51")

    snapshot.assert_match(results)


def test_get_multiple_indexes_single_threaded(snapshot):
    results = get_multiple_indexes("https://index.commoncrawl.org", indexes=["2019-51", "2019-47"])

    snapshot.assert_match(results)


def test_get_multiple_indexes_multi_threaded(snapshot):
    results = get_multiple_indexes("https://index.commoncrawl.org", indexes=["2019-51", "2019-47"], threads=2)

    # sorting values to counteract the random results order, which is caused
    # through the randomness in which thread search finished first
    results_df = pd.DataFrame(results)
    sorted_results_df = results_df.sort_values(by="timestamp")

    snapshot.assert_match(sorted_results_df.to_dict("records"))
