from typing import Dict
import io
import gzip
import requests


def _download_single_result(result: Dict) -> str:
    """Downloads HTML for single search result.

    Args:
        result: Common Crawl index search result from the search function.


    Returns:
        The HTML of the corresponding page as a string.

    """

    offset, length = int(result["offset"]), int(result["length"])

    offset_end = offset + length - 1

    prefix = "https://commoncrawl.s3.amazonaws.com"
    response = requests.get(
        f"{prefix}/{result['filename']}",
        headers={"Range": f"bytes={offset}-{offset_end}"}
    )

    zipped_file = io.BytesIO(response.content)
    unzipped_file = gzip.GzipFile(fileobj=zipped_file)

    raw_data: bytes = unzipped_file.read()
    data: str = raw_data.decode("utf-8")

    html = ""

    if len(data) > 0:
        _, _, html = data.strip().split("\r\n\r\n", 2)

    return html