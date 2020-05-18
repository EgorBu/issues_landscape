import argparse
from datetime import datetime
import os
import subprocess

from tqdm import tqdm

from landscape.utils import is_between_dates


def process_restore(start_date: str, end_date: str, remove_untar_files: bool) -> None:
    """
    Restore tar dumps to mongo db from start_date to end_date
    :param start_date: Start date in ISO format
    :param end_date: End date in ISO format
    :param remove_untar_files: True if necessary to remove untar files, else - False
    :return: None
    """
    tar_dir_path = "./daily_tar_dumps"
    tar_dumps = os.listdir(tar_dir_path)
    tar_dumps = list(filter(lambda tar_dump: is_between_dates(tar_dump, start_date, end_date),
                            tar_dumps))

    for tar_dump_name in tqdm(tar_dumps, total=len(tar_dumps)):
        untar_file(os.path.join(tar_dir_path, tar_dump_name))
        untar_dump_name = tar_dump_name.replace(".tar.gz", "")
        is_successful_restore = restore_issue_bson_to_mongo(untar_dump_name)
        if is_successful_restore and remove_untar_files:
            os.system("rm -rf %s" % os.path.join(tar_dir_path, untar_dump_name))


def untar_file(file_path: str) -> None:
    """
    Untar given file
    :param file_path: Path to file which will be untarred
    :return: None
    """
    cmd = "tar -zxvf %s" % file_path
    try:
        subprocess.run(cmd, check=True, shell=True)
    except subprocess.SubprocessError as exc:
        print(exc)


def restore_issue_bson_to_mongo(tar_file_name: str) -> bool:
    """
    Restore tar dump to mongo db
    :param tar_file_name: Tar filename
    :return: None
    """
    db_name = "issues_landscape"
    collection_name = tar_file_name + "_issues"
    bson_file_path = os.path.join("./daily_tar_dumps/%s/dump/github" % tar_file_name,
                                  "issues.bson")
    cmd = "mongorestore -d %s -c %s %s" % (db_name, collection_name, bson_file_path)
    try:
        subprocess.run(cmd, check=True, shell=True)
    except subprocess.SubprocessError as exc:
        print(exc)
        return False
    return True


def main() -> None:
    """
    Restore tar dumps which located in daily_tar_dumps directory to mongodb
    :return: None
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", default="2015-12-01",
                        help="Starting date(YYYY-MM-DD) from which dumps will be restored to mongo")
    parser.add_argument("--end-date", default=str(datetime.today().date()),
                        help="Ending date(YYYY-MM-DD) to which dumps will be restored to mongo")
    args = parser.parse_args()
    process_restore(args.start_date, args.end_date, True)
