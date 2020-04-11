"""
Download Github issues daily dumps from this page
"http://ghtorrent-downloads.ewi.tudelft.nl/mongo-daily/" and store them into target directory.
"""
import argparse
import os
import tarfile
from typing import Optional

from bs4 import BeautifulSoup
from tqdm import tqdm
import urllib.request


def download_progress_hook(p_bar: tqdm) -> (int, int, Optional[int]):
    """
    Wraps tqdm instance
    :param p_bar: tqdm instance responible for showing download progress
    :return: function update_to which updates tqdm state
    """
    last_block = [0]

    def update_to(block_num: int = 1, block_size: int = 1,
                  total_size: Optional[int] = None) -> None:
        """
        Update tqdm state
        :param block_num: Number of blocks transferred so far
        :param block_size: Size of each block
        :param total_size: Total size (in tqdm units)
        :return: None
        """
        if total_size not in (None, -1):
            p_bar.total = total_size
        p_bar.update((block_num - last_block[0]) * block_size)
        last_block[0] = block_num

    return update_to


def extract_archive_links(url: str) -> [str]:
    """
    Extract tar files links from GHTorrent html page to list
    :param url: url of page with daily dumps tars
    :return: list of tar files links
    """
    html_page = urllib.request.urlopen(url)
    soup = BeautifulSoup(html_page, features="html.parser")
    tar_files_links = []
    for link in soup.findAll("a", href=True):
        filename = link.get("href")
        if filename.endswith("tar.gz"):
            tar_files_links.append(url + filename)
    return tar_files_links


def process_archives(archive_links: [str], target_dir: str) -> None:
    """
    Download tar files and untar them to target directory
    :param archive_links: list of tar files links
    :param target_dir: target directory
    :return: None
    """
    os.makedirs(target_dir, exist_ok=True)
    for archive_link in tqdm(desc="process archives", iterable=archive_links,
                             total=len(archive_links)):
        if not archive_link.endswith("tar.gz"):
            continue
        tar_filename = os.path.basename(archive_link)
        target_loc = os.path.join(target_dir, tar_filename)

        with tqdm(desc="downloading %s" % tar_filename) as p_bar:
            urllib.request.urlretrieve(archive_link, filename=target_loc,
                                       reporthook=download_progress_hook(p_bar))

        unique_dump_dir = os.path.join(target_dir, tar_filename.replace(".tar.gz", ""))
        is_successful_untar = untar(target_loc, unique_dump_dir)
        if is_successful_untar:
            remove_excess_files(os.path.join(unique_dump_dir, "dump/github"))


def untar(tarfile_path: str, target_directory: str, remove_tarfile: bool = True) -> bool:
    """
    Untar tarfile to target directory and remove tarfile if it is necessary
    :param tarfile_path: path of tarfile
    :param target_directory: directory where files will be extracted
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


def remove_excess_files(directory: str) -> None:
    """
    Remove all files from directory, except for related to issues
    :param directory: directory where excess files will be deleted
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
    args = parser.parse_args()
    url = "http://ghtorrent-downloads.ewi.tudelft.nl/mongo-daily/"
    tar_files_links = extract_archive_links(url)

    process_archives(tar_files_links, args.target_dir)


if __name__ == "__main__":
    main()
