from comcrawl.utils.initialization import download_available_indexes


def test_download_available_indexes(snapshot):
    indexes = download_available_indexes()

    assert "2008-2009" in indexes
