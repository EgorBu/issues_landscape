import tarfile
import sys
import os

import urllib.request
from bs4 import BeautifulSoup


def show_file_progress(block_num, block_size, total_size):
    downloaded = block_num * block_size
    if total_size > 0:
        percent = downloaded * 1e2 / total_size
        s = "\r%5.1f%% %*d / %d bytes" % (percent, len(str(total_size)), downloaded, total_size)
        sys.stderr.write(s)
        if downloaded >= total_size:
            sys.stderr.write("\n")


def get_tar_files_links_from_html(url):
    html_page = urllib.request.urlopen(url)
    soup = BeautifulSoup(html_page, features='html.parser')
    tar_files_links = []
    for link in soup.findAll('a', href=True):
        filename = link.get('href')
        if filename.endswith('tar.gz'):
            tar_files_links.append(url + filename)
    return tar_files_links


def extract_tar_files_to_directory(tar_files_links, target_dir):
    os.makedirs(target_dir, exist_ok=True)
    for tar_file_link in tar_files_links:
        tar_filename = os.path.basename(tar_file_link)
        full_tar_filename = target_dir + '/' + tar_filename

        print("downloading %s file" % tar_filename)
        urllib.request.urlretrieve(tar_file_link, full_tar_filename, show_file_progress)

        unique_dump_dir = target_dir + '/' + tar_filename.replace('.tar.gz', '')
        extract_tar_to_directory(full_tar_filename, unique_dump_dir, True)
        remove_excess_files(unique_dump_dir + '/dump/github')


def extract_tar_to_directory(tarfile_path, target_directory, need_remove_tarfile):
    if tarfile_path.endswith('tar.gz'):
        print("extracting %s file" % tarfile_path)
        tar = tarfile.open(tarfile_path, 'r:gz')
        os.makedirs(target_directory, exist_ok=True)
        tar.extractall(target_directory)
        tar.close()
        if need_remove_tarfile:
            os.remove(tarfile_path)


def remove_excess_files(directory):
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
