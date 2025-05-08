import pandas as pd
import urllib.parse

def is_arabic(query) :
    return any('\u0600' <= char <= '\u06FF' for char in query) or any('\u0750' <= char <= '\u077F' for char in query)

def generate_date_ranges(start_date, end_date, days_jump=30):
    """
    Generate date ranges as tuples spanning from the start date till the end date with a specified jump in days.

    Args:
        start_date (str or pd.Timestamp): The start date in 'YYYY-MM-DD' format or as a pandas Timestamp.
        end_date (str or pd.Timestamp): The end date in 'YYYY-MM-DD' format or as a pandas Timestamp.
        days_jump (int): The number of days to jump for each range.

    Returns:
        list: A list of tuples representing the date ranges.
    """
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    jump_offset = pd.Timedelta(days=days_jump)

    date_ranges = []
    current_date = start_date
    while current_date <= end_date:
        range_start = current_date
        range_end = min(current_date + jump_offset - pd.Timedelta(days=1), end_date)
        date_ranges.append((range_start.strftime("%Y-%m-%d"), range_end.strftime("%Y-%m-%d")))
        current_date += jump_offset

    return date_ranges

def create_folder_if_not_exists(folder_path):
    """
    Create a folder if it does not exist.

    Args:
        folder_path (str): The path of the folder to create.
    """
    import os
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)


def url_encode_query(query):
    """
    Encodes a string for use in a URL query.  Replaces spaces with '%20'.

    Args:
        query: The string to encode.

    Returns:
        The URL-encoded string.
    """
    return urllib.parse.quote(query)