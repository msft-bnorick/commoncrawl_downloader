import argparse
import hashlib
import shutil
import chardet
import json
import re
import sys
import traceback
import unicodedata
import zlib
from pathlib import Path
from pprint import pprint
from time import monotonic as monotonic_time
from typing import List, Union

import justext
import lxml
import pycld2 as cld2
import ray
import requests
import warcio
from tqdm import tqdm
from warcio.archiveiterator import ArchiveIterator


S3_URL_ROOT = 'https://commoncrawl.s3.amazonaws.com'
DEBUG = None  # set via parse_args with --debug argument


def get_all_snapshots():
    r = requests.get('http://index.commoncrawl.org/')
    return sorted(set(re.findall(r'CC-MAIN-(\d\d\d\d-\d\d)', r.text)), reverse=True)


@ray.remote
def _get_warc_urls(snapshot):
    url = f'{S3_URL_ROOT}/crawl-data/CC-MAIN-{snapshot}/warc.paths.gz'
    response = requests.get(url.strip(), stream=True)

    snapshot_warc_urls = []
    data = zlib.decompress(response.content, zlib.MAX_WBITS|32)
    for url in data.decode('utf-8').splitlines():
        if url:
            snapshot_warc_urls.append(f'{S3_URL_ROOT}/{url}')

    return snapshot_warc_urls


@ray.remote
def get_warc_urls(snapshots: Union[str, List[str]] = None):
    if not snapshots:
        snapshots = get_all_snapshots()
    elif isinstance(snapshots, str):
        snapshots = [snapshots]

    if len(snapshots) > 1:
        return ray.get([_get_warc_urls.remote(s) for s in snapshots])
    else:
        return ray.get(_get_warc_urls.remote(snapshots[0]))


def url_cache_path(url, snapshot_cache_path):
    if not snapshot_cache_path:
        return None
    url_hash = hashlib.md5(url.encode('utf8')).hexdigest()
    return snapshot_cache_path / url_hash / url.split('/')[-1]


def caching_read(stream, f_cache):
    def read(*args, **kwargs):
        chunk = stream.read(*args, **kwargs)
        f_cache.write(chunk)
        return chunk
    return read


def _warc_contents_from_stream(stream, url):
    try:
        for record in ArchiveIterator(stream, arc2warc=True):
            if record.rec_type == 'response':
                content = record.content_stream().read()
                meta = {
                    'warc': url.strip(),
                    'warc_headers': record.rec_headers.headers,
                    'http_response_headers': record.http_headers.headers,
                }

                yield content, meta
    except warcio.exceptions.ArchiveLoadFailed:
        print('WARNING: WARC load failed!')
        traceback.print_exc()


def warc_contents_from_stream(stream, url, cache_path):
    if cache_path:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        incomplete_cache_path = cache_path.parent / f'{cache_path.name}.incomplete'
        with incomplete_cache_path.open('wb') as f:
            stream.read = caching_read(stream, f)
            yield from _warc_contents_from_stream(stream, url)
        incomplete_cache_path.rename(cache_path)
    else:
        yield from _warc_contents_from_stream(stream, url)


def warc_url_to_contents(url, snapshot_cache_path, force_download):
    from loguru import logger
    cache_path = url_cache_path(url, snapshot_cache_path)
    if not force_download and cache_path and cache_path.exists():
        logger.info(f'Loading {url} from cache...')
        with cache_path.open('rb') as f:
            yield from warc_contents_from_stream(f, url, None)
    else:
        response = requests.get(url.strip(), stream=True)
        yield from warc_contents_from_stream(response.raw, url, cache_path)


def html_to_text(html, meta):
    try:
        html = html.decode('utf-8')
    except UnicodeDecodeError:
        # try to figure out encoding if not urf-8

        guess = chardet.detect(html)['encoding']

        if not guess or guess == 'UTF-8': return

        try:
            html = html.decode(guess)
        except (UnicodeDecodeError, LookupError):
            # still cant figure out encoding, give up
            return
    try:
        try:
            _,_,details = cld2.detect(html)
        except:
            # cld2 doesn't like control characters
            # https://github.com/mikemccand/chromium-compact-language-detector/issues/22#issuecomment-435904616
            html_no_ctrl_chars = ''.join([l for l in html if unicodedata.category(l)[0] not in ['C',]])
            _,_,details = cld2.detect(html_no_ctrl_chars)

        if details[0][1] == 'en':
            meta.update(
                primary_language='en',
                lang_detector='pycld2',
                lang_detector_extra_info=details,
                extractor='justext'
            )
            return '\n'.join([x.text for x in
                        justext.justext(html, justext.get_stoplist('English'))
                    if not x.is_boilerplate]), meta

    except lxml.etree.ParserError:
        return
    except:
        traceback.print_exc()


@ray.remote
def process_warc_file(url, output_path: Path, snapshot_cache_path, force_download, log_interval=1000):
    from loguru import logger
    processed = 0
    start = monotonic_time()
    last_log_time = start
    incomplete_path = output_path.parent / f'{output_path.name}.incomplete'
    with incomplete_path.open('w', encoding='utf8') as f:
        for args in warc_url_to_contents(url, snapshot_cache_path, force_download):
            result = html_to_text(*args)
            processed += 1
            if processed % log_interval == 0:
                cur_time = monotonic_time()
                duration = cur_time - start
                since_last_log = cur_time - last_log_time
                logger.info(f'{url}    {processed}    ({processed/duration:.3f} doc/s overall, {log_interval/since_last_log:.3f} doc/s since last log)')
                last_log_time = cur_time
            if not result:
                continue
            text, meta = result
            if text:
                serialized = json.dumps({'text': text, 'meta': meta})
                f.write(serialized)
                f.write('\n')
    incomplete_path.rename(output_path)


@ray.remote
def download_url(url, snapshot_cache_path, force):
    path = url_cache_path(url, snapshot_cache_path)
    if force or not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        incomplete_path = path.parent / f'{path.name}.incomplete'
        with requests.get(url, stream=True) as r:
            with incomplete_path.open('wb') as f:
                shutil.copyfileobj(r.raw, f)
            incomplete_path.rename(path)


@ray.remote
def process_snapshot(snapshot, output_path: Path, force_download, force_process, cache_path, download_only):
    from loguru import logger
    output_path.mkdir(parents=True, exist_ok=True)
    if cache_path:
        cache_path.mkdir(parents=True, exist_ok=True)

    urls = ray.get(get_warc_urls.remote(snapshot))

    if DEBUG:
        urls = urls[:10]

    result_ids = []
    for url in urls:
        if download_only:
            result_ids.append(download_url.remote(url, cache_path, force_download))
        else:
            name = url.split('/')[-1][:-len('.warc.gz')]
            url_output_path = output_path / f'{name}.jsonl'
            if force_process or not url_output_path.exists():
                result_ids.append(process_warc_file.remote(url, url_output_path, cache_path, force_download))
            else:
                logger.info(f'Skipping {url}, output exists.')

    logger.info(f'Processing {len(result_ids)} warc files for snapshot {snapshot}...')
    return result_ids


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--snapshots', action='append', default=None)
    parser.add_argument('-o', '--output', type=Path, default=Path('.'))
    parser.add_argument('-f', '--force-download', action='store_true')
    parser.add_argument('-p', '--force-process', action='store_true')
    parser.add_argument('-l', '--local', action='store_true')
    parser.add_argument('-d', '--download-only', action='store_true')
    parser.add_argument('-g', '--debug', action='store_true')
    parser.add_argument('-c', '--cache', type=Path, default=None)

    args = parser.parse_args()

    if args.download_only and not args.cache:
        raise ValueError(f'A cache path must be provided when using the --download-only flag.')

    if args.cache and args.force_download:
        from loguru import logger
        logger.warning(f'A cache path was specified alongside --force-download, previously cached downloads will be ignored.')

    global DEBUG
    DEBUG = args.debug

    return args


def main():
    args = parse_args()

    address = None if args.local else 'auto'
    ray.init(address=address)

    snapshots = args.snapshots
    output_path = args.output.resolve()
    force_download = args.force_download
    force_process = args.force_process
    cache_path = args.cache.resolve() if args.cache else None
    download_only = args.download_only

    if not snapshots:
        snapshots = get_all_snapshots()

    result_ids = []
    for snapshot in snapshots:
        snapshot_cache_path = cache_path / snapshot if cache_path else None
        result_ids.extend(ray.get(process_snapshot.remote(snapshot, output_path / snapshot, force_download, force_process, snapshot_cache_path, download_only)))

    progress = tqdm(total=len(result_ids))
    while result_ids:
        returns = min(100, len(result_ids))
        done_ids, result_ids = ray.wait(result_ids, num_returns=returns)
        progress.update(len(done_ids))
    progress.close()

if __name__ == '__main__':
    main()
