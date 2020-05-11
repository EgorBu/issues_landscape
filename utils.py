from datetime import datetime
import re


def is_between_dates(tar_filename: str, start_date: str, end_date: str,
                     filename_pattern: str = "mongo-dump-(.*).tar.gz") -> bool:
    """
    Check that current dump date is between start_date and end_date
    :param tar_filename: Tar filename which contains daily dump date
    :param start_date: Start date in ISO format
    :param end_date: End date in ISO format
    :param filename_pattern: Filename pattern, which contains date in ISO format
    :return: True if tar file date is between start date and end date, else - False
    """
    iso_format = "%Y-%m-%d"
    start_date = datetime.strptime(start_date, iso_format)
    end_date = datetime.strptime(end_date, iso_format)
    tar_date = re.search(filename_pattern, tar_filename).group(1)
    tar_date = datetime.strptime(tar_date, iso_format)
    return start_date <= tar_date <= end_date
