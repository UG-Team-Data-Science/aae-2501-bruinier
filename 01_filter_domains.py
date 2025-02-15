# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "dask[distributed]",
#     "requests",
# ]
# ///
import gzip
import io
import json
import logging
from pathlib import Path

import dask.bag as db
import requests
from dask.distributed import Client
from urllib.parse import urlparse

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

OUTPUT_VOLUME = Path("/slow-data/p266261")
OUTPUT_FOLDER = OUTPUT_VOLUME.joinpath("aae-2501-bruinier/filtered_indices_domains")
OUTPUT_FOLDER.mkdir(exist_ok=True)

with open("domainslist.txt", "r", encoding="utf-8") as f:
    DOMAINS = [line.strip() for line in f if line.strip()]


def read_gzip_lines(url):
    response = requests.get(url, stream=True)

    if response.status_code == 200:
        with gzip.GzipFile(fileobj=io.BytesIO(response.content), mode="rb") as gz:
            text_stream = io.TextIOWrapper(gz, encoding="utf-8")
            for line in text_stream:
                yield line.rstrip()


def parse_data(line):
    surl, timestamp, raw_json = line.split(" ", 2)
    data = json.loads(raw_json)
    return data


def filter_domain(data):
    netloc = urlparse(data["url"]).netloc
    return any(
        (netloc == domain) or (netloc.endswith(f".{domain}")) for domain in DOMAINS
    )


if __name__ == "__main__":
    logger.info("Starting CC index filtering script")

    client = Client(n_workers=20, threads_per_worker=1)
    base_url = "https://data.commoncrawl.org/"
    with open("cc-index.paths", "r") as f:
        indices = [base_url + line.strip() for line in f]
        indices = [ind for ind in indices if ind.endswith(".gz")]

        result = (
            db.from_sequence(indices)
            .map(read_gzip_lines)
            .flatten()
            .map(parse_data)
            .filter(filter_domain)
            .map(lambda x: json.dumps(x, ensure_ascii=False))
            .to_textfiles(str(OUTPUT_FOLDER / "*.json.gz"))
        )

    logger.info("CC index filtering completed")
