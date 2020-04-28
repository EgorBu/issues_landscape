"""
Download Github issues daily dumps from this page
"http://ghtorrent-downloads.ewi.tudelft.nl/mongo-daily/" and store them into target directory.
"""
import argparse
from datetime import datetime
from functools import partial
from multiprocessing import Pool
import os
import re
import tarfile
from typing import List

from bs4 import BeautifulSoup
import requests
from tqdm import tqdm


def extract_archive_links(url: str, start_date: str, end_date: str) -> List[str]:
    """
    Extract tar files links from GHTorrent html page to list
    :param url: Url of page with daily dumps tars
    :param start_date: Start date in ISO format
    :param end_date: End date in ISO format
    :return: List of tar files links
    """
    html_page = requests.get(url).text
    soup = BeautifulSoup(html_page, features="html.parser")
    tar_files_links = []
    for link in soup.findAll("a", href=True):
        filename = link.get("href")
        if filename.endswith("tar.gz") and is_between_dates(filename, start_date, end_date):
            tar_files_links.append(url + filename)
    return tar_files_links


def is_between_dates(tar_filename: str, start_date: str, end_date: str) -> bool:
    """
    Check that tar date is between start_date and end_date
    :param tar_filename: Tar filename which contains daily dump date
    :param start_date: Start date in ISO format
    :param end_date: End date in ISO format
    :return: True if tar file date is between start date and end date, else - False
    """
    iso_format = "%Y-%m-%d"
    start_date = datetime.strptime(start_date, iso_format)
    end_date = datetime.strptime(end_date, iso_format)
    tar_date = re.search("mongo-dump-(.*).tar.gz", tar_filename).group(1)
    tar_date = datetime.strptime(tar_date, iso_format)
    return start_date <= tar_date <= end_date


def process_archives(archive_links: List[str], target_dir: str) -> None:
    """
    Download tar files and untar them to target directory
    :param archive_links: List of tar files links
    :param target_dir: Target directory
    :return: None
    """
    os.makedirs(target_dir, exist_ok=True)
    pool = Pool(processes=8, initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),))
    process_archive_with_arg = partial(process_archive, target_dir)
    with tqdm(desc="process archives", total=len(archive_links), position=0) as p_bar:
        for i, _ in enumerate(pool.imap(process_archive_with_arg, enumerate(archive_links))):
            p_bar.update()


def process_archive(target_dir: str, archive_link: (int, str)) -> None:
    """
    Download tar file and untar it to target directory
    :param target_dir: Target directory
    :param archive_link: Tuple with archive link and it position in archive links list
    :return: None
    """
    index, archive_link = archive_link
    if not archive_link.endswith("tar.gz"):
        return
    tar_filename = os.path.basename(archive_link)
    target_loc = os.path.join(target_dir, tar_filename)

    download_file_from_url(archive_link, target_loc, index + 1)

    unique_dump_dir = os.path.join(target_dir, tar_filename.replace(".tar.gz", ""))
    is_successful_untar = untar(target_loc, unique_dump_dir)
    if is_successful_untar:
        remove_excess_files(os.path.join(unique_dump_dir, "dump/github"))
        tar_directory(unique_dump_dir, os.path.join(target_dir, tar_filename))


def download_file_from_url(file_url: str, target_loc: str, file_number: int) -> None:
    """
    Download file from given url to target location
    :param file_url: Url of file which will be downloaded
    :param target_loc: Target location where downloaded file will be saved
    :param file_number: Number of file
    :return: None
    """
    file_size = int(requests.head(file_url).headers["Content-Length"])
    if os.path.exists(target_loc):
        first_byte = os.path.getsize(target_loc)
    else:
        first_byte = 0
    header = {"Range": "bytes=%s-%s" % (first_byte, file_size)}
    p_bar = tqdm(position=file_number, total=file_size, initial=first_byte, unit="B",
                 unit_scale=True,
                 desc=file_url.split("/")[-1])
    req = requests.get(file_url, headers=header, stream=True)
    with(open(target_loc, "ab")) as f:
        for chunk in req.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
                p_bar.update(1024)
    p_bar.close()


def untar(tarfile_path: str, target_directory: str, remove_tarfile: bool = True) -> bool:
    """
    Untar tarfile to target directory and remove tarfile if it is necessary
    :param tarfile_path: Path of tarfile
    :param target_directory: Directory where files will be extracted
    :param remove_tarfile: True if necessary to remove tarfile, otherwise - False
    :return: True if untar process was successful, else - False
    """
    os.makedirs(target_directory, exist_ok=True)
    with tarfile.open(tarfile_path, "r:gz") as tar:
        try:
            for member in tqdm(desc="extracting %s" % tarfile_path, iterable=tar.getmembers(),
                               total=len(tar.getmembers())):
                tar.extract(path=target_directory, member=member)
        except EOFError:
            return False
    if remove_tarfile:
        os.remove(tarfile_path)
    return True


def tar_directory(dir_path: str, tarfile_path: str, remove_directory: bool = True) -> None:
    """
    Tar directory to specific path
    :param dir_path: Path to directory
    :param tarfile_path: Path to created tarfile
    :param remove_directory: True if necessary to remove tarred directory, otherwise - False
    :return: None
    """
    with tarfile.open(tarfile_path, "w:gz") as tar:
        for root, dirs, files in os.walk(dir_path):
            for file in files:
                tar.add(os.path.join(root, file))
    if remove_directory:
        os.system("rm -rf %s" % dir_path)


def remove_excess_files(directory: str) -> None:
    """
    Remove all files from directory, except for related to issues
    :param directory: Directory where excess files will be deleted
    :return: None
    """
    issue_related_files = {"issues.bson", "issue_comments.bson"}
    bson_files = os.listdir(directory)
    for file in bson_files:
        if file not in issue_related_files:
            os.remove(os.path.join(directory, file))


def main() -> None:
    """
    Download and untar Github daily dumps tar files to target_dir
    :return: None
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--target-dir", required=True,
                        help="Directory to store downloaded tar files")
    parser.add_argument("--start-date", default="2015-12-01",
                        help="Starting date(YYYY-MM-DD) from which dumps will be downloaded")
    parser.add_argument("--end-date", default=str(datetime.today().date()),
                        help="Ending date(YYYY-MM-DD) from which dumps will be downloaded")
    args = parser.parse_args()
    url = "http://ghtorrent-downloads.ewi.tudelft.nl/mongo-daily/"
    tar_files_links = extract_archive_links(url, args.start_date, args.end_date)

    process_archives(tar_files_links, args.target_dir)
