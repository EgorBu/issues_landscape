import tarfile
import os
from typing import Optional

import urllib.request
from bs4 import BeautifulSoup
from tqdm import tqdm


def download_progress_hook(p_bar: tqdm) -> (int, int, Optional[int]):
    last_block = [0]

    def update_to(block_num: int = 1, block_size: int = 1, total_size: Optional[int] = None) -> None:
        if total_size not in (None, -1):
            p_bar.total = total_size
        p_bar.update((block_num - last_block[0]) * block_size)
        last_block[0] = block_num

    return update_to


def get_tar_files_links_from_html(url: str) -> [str]:
    html_page = urllib.request.urlopen(url)
    soup = BeautifulSoup(html_page, features='html.parser')
    tar_files_links = []
    for link in soup.findAll('a', href=True):
        filename = link.get('href')
        if filename.endswith('tar.gz'):
            tar_files_links.append(url + filename)
    return tar_files_links


def extract_tar_files_to_directory(tar_files_links: [str], target_dir: str) -> None:
    os.makedirs(target_dir, exist_ok=True)
    for tar_file_link in tar_files_links:
        tar_filename = os.path.basename(tar_file_link)
        full_tar_filename = target_dir + '/' + tar_filename

        print("downloading %s file" % tar_filename)
        with tqdm() as p_bar:
            urllib.request.urlretrieve(tar_file_link, filename=full_tar_filename,
                                       reporthook=download_progress_hook(p_bar))
        unique_dump_dir = target_dir + '/' + tar_filename.replace('.tar.gz', '')
        extract_tar_to_directory(full_tar_filename, unique_dump_dir, True)
        remove_excess_files(unique_dump_dir + '/dump/github')


def extract_tar_to_directory(tarfile_path: str, target_directory: str, need_remove_tarfile: bool) -> None:
    if tarfile_path.endswith('tar.gz'):
        print("extracting %s file" % tarfile_path)
        tar = tarfile.open(tarfile_path, 'r:gz')
        os.makedirs(target_directory, exist_ok=True)
        tar.extractall(target_directory)
        tar.close()
        if need_remove_tarfile:
            os.remove(tarfile_path)


def remove_excess_files(directory: str) -> None:
    issue_connected_files = ['issues.bson', 'issue_comments.bson']
    bson_files = os.listdir(directory)
    for cur_bson_file in bson_files:
        if cur_bson_file not in issue_connected_files:
            os.remove(directory + "/" + cur_bson_file)


def main():
    url = "http://ghtorrent-downloads.ewi.tudelft.nl/mongo-daily/"
    tar_files_links = get_tar_files_links_from_html(url)

    download_target_dir = os.path.abspath(os.getcwd()) + '/data'
    extract_tar_files_to_directory(tar_files_links, download_target_dir)


if __name__ == '__main__':
    main()
