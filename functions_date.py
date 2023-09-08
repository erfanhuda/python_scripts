from datetime import datetime, timedelta
import calendar
from dateutil.relativedelta import relativedelta

"""CONSTANT DATE"""
YEAR_ = [1800, 2200]
TAHUN_ = [x for x in range(YEAR_[0], YEAR_[1])]
LEAP_YEAR = [1 if (x % 4 == 0) else 0 for x in TAHUN_]
YEAR = [(TAHUN_[x],LEAP_YEAR[y]) for x in range(len(TAHUN_)) for y in range(len(LEAP_YEAR)) if x == y]
BULAN = ["JANUARI", "FEBRUARI", "MARET", "APRIL", "MEI", "JUNI", "JULI", "AGUSTUS", "SEPTEMBER", "OKTOBER", "NOVEMBER", "DESEMBER"]
HARI = ["MINGGU", "SENIN", "SELASA", "RABU", "KAMIS", "JUMAT", "SABTU"]
DAYS_IN_MONTH = [28 if (x == 1) else 31 if (((x < 7) and (x % 2 == 0)) or ((x > 6) and (x % 2 == 1))) else 30 for x in range(len(BULAN))]
DAYS_IN_YEAR = [(x,y,k) for x in DAYS_IN_MONTH for y in BULAN for (k,v) in YEAR]

def determine_datetime_format(datetime_string):
    # List the formats of date string
    formats = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%m-%d-%Y",
        "%m/%d/%Y",
        "%d-%m-%Y",
        "%d/%m/%Y",
    ]

    # Iterate over the formats and matches to the datetime_string given
    for fmt in formats:
        try:
            datetime.strptime(datetime_string, fmt)
            return fmt

        except ValueError:
            pass

    return None


def get_beginning_of_month(date):
    # Convert input date string to datetime object
    date_obj = datetime.strptime(date, determine_datetime_format(date))

    # Extract the date obj to year and month
    year = date_obj.year
    month = date_obj.month

    # Create new datetime object for the beginning of the month
    beginning_of_month = datetime(year, month, 1)

    # Format the beginning of the month to string
    beginning_of_month_str = beginning_of_month.strftime("%Y-%m-%d")

    return beginning_of_month_str


def get_end_of_month(date):
    # Convert input date string to datetime object
    date_obj = datetime.strptime(date, determine_datetime_format(date))

    # Extract the date obj to year and month
    year = date_obj.year
    month = date_obj.month

    # Find the number of days in the month
    _, num_days = calendar.monthrange(year, month)

    # Create new datetime object for the end of the month
    end_of_month = datetime(year, month, num_days)

    # Format the beginning of the month to string
    end_of_month_str = end_of_month.strftime("%Y-%m-%d")

    return end_of_month_str


def get_day_date_range(start_date, end_date):
    # Convert input date string to datetime object
    start_date_obj = datetime.strptime(
        start_date, determine_datetime_format(start_date))
    end_date_obj = datetime.strptime(
        end_date, determine_datetime_format(end_date))

    # Create an empty list to store the range date
    date_range = []

    # Iterate over the range of dates
    current_date = start_date_obj
    while current_date < end_date_obj:
        # Append the current date to the list
        date_range.append(current_date.strftime("%Y-%m-%d"))

        # Move to the next date
        current_date += timedelta(days=1)

    return date_range


def get_monthly_date_range(start_date, end_date):
    # Convert input date string to datetime object
    start_date_obj = datetime.strptime(
        start_date, determine_datetime_format(start_date))
    end_date_obj = datetime.strptime(
        end_date, determine_datetime_format(end_date))

    # Create an empty list to store the range date
    date_range = []

    # Iterate over the range of dates
    current_date = start_date_obj
    while current_date < end_date_obj:
        # Append the current date to the list
        date_range.append(current_date.strftime("%Y-%m-%d"))

        # Move to the next date
        current_date += relativedelta(month=1)
        current_date = current_date.replace(day=1)

    return date_range