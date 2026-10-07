from func import is_trading_day, get_market_data_of_sp500, generate_sp500_treemap
import sys
import os
import gc
from datetime import datetime
from zoneinfo import ZoneInfo

from prefect import flow, task, get_run_logger

BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # folder of this file

# When Prefect runs this flow, it clones the GitHub repo into a temporary folder
# that is deleted after the run. Set STATUS_LOG_DIR (for example in the worker's
# .env / systemd EnvironmentFile) so status files land in a folder that persists.
STATUS_LOG_DIR = os.getenv("STATUS_LOG_DIR", os.path.join(BASE_DIR, "status_logs"))

MARKET_TZ = ZoneInfo("America/New_York")


def today_market_str():
    """
    Returns today's date in New York (market) time.

    Output:
        str: date in 'yyyy-mm-dd' format
    """
    return datetime.now(MARKET_TZ).strftime('%Y-%m-%d')


def write_status_log(suffix, status_str):
    """
    Writes a small status text file to STATUS_LOG_DIR.

    Inputs:
        suffix (str): text added to the end of the file name, ex: 'success'
        status_str (str): message written into the file
    """
    os.makedirs(STATUS_LOG_DIR, exist_ok=True)
    current_date_str = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    with open(os.path.join(STATUS_LOG_DIR, f"{current_date_str}_{suffix}.txt"), "w") as f:
        f.write(status_str)


@task(name="check-date")
def check_date(date_arg):
    """
    Validates the date and checks whether it is a trading day.

    Inputs:
        date_arg (str): date in 'yyyy-mm-dd' format

    Output:
        bool: True if the treemap should be generated, False if it is not a trading day

    Raises:
        ValueError: if the date format is invalid or the date is in the future
    """
    logger = get_run_logger()
    try:
        datetime.strptime(date_arg, '%Y-%m-%d')
    except ValueError:
        status_str = f"Invalid date format provided to script: {date_arg}. Use yyyy-mm-dd format."
        write_status_log("value_error", status_str)
        raise ValueError(status_str)

    if date_arg > today_market_str():
        status_str = f"Invalid date provided to script: {date_arg} is in the future."
        write_status_log("future_not_exist", status_str)
        raise ValueError(status_str)

    if not is_trading_day(date_arg):
        status_str = f"No issue with script: {date_arg} not a trading day."
        logger.info(status_str)
        write_status_log("valid_nontrading_day", status_str)
        return False
    return True


# retries help with temporary Yahoo / Wikipedia / S3 network errors
@task(name="fetch-market-data", retries=2, retry_delay_seconds=300)
def fetch_market_data(date_arg, use_S3):
    """
    Downloads S&P 500 constituent data for the date and saves the CSV (S3 or local).

    Inputs:
        date_arg (str): date in 'yyyy-mm-dd' format
        use_S3 (bool): True to save to S3, False to save to the local data folder
    """
    get_market_data_of_sp500(current_date=date_arg, use_S3=use_S3)
    gc.collect()  # free the yfinance objects before the plotly step


@task(name="build-treemap", retries=1, retry_delay_seconds=60)
def build_treemap(date_arg, use_S3):
    """
    Builds the treemap HTML from the saved CSV and stores it (S3 or local).

    Inputs:
        date_arg (str): date in 'yyyy-mm-dd' format
        use_S3 (bool): True to read/write S3, False to use local folders
    """
    generate_sp500_treemap(date_arg, use_S3=use_S3)


@flow(name="daily-sp500-treemap", log_prints=True)
def daily_treemap_flow(date=None, storage="s3"):
    """
    Prefect flow that replaces the crontab job for the daily S&P 500 treemap.

    Inputs:
        date (str or None): date in 'yyyy-mm-dd' format; None means today (New York time)
        storage (str): 's3' or 'local'

    Output:
        str: status message for the run
    """
    if storage.lower() not in ['s3', 'local']:
        raise ValueError("storage must be 's3' or 'local'.")
    use_S3 = storage.lower() == 's3'
    date_arg = date if date else today_market_str()
    print(f"Running treemap for {date_arg} (Using S3: {use_S3})")

    if not check_date(date_arg):
        return f"{date_arg} is not a trading day. Nothing to do."

    fetch_market_data(date_arg, use_S3)
    build_treemap(date_arg, use_S3)

    status_str = f"Successfully generated treemap for {date_arg}."
    if date_arg == today_market_str():
        write_status_log("success_current_date", status_str)
    else:
        write_status_log("success", status_str)
    return status_str


if __name__ == "__main__":
    # Manual runs still work the same way as before:
    #   python gen_daily_treemap.py [yyyy-mm-dd] [s3|local]
    date_cli = sys.argv[1] if len(sys.argv) > 1 else None
    storage_cli = sys.argv[2] if len(sys.argv) > 2 else "s3"
    try:
        daily_treemap_flow(date=date_cli, storage=storage_cli)
    except Exception as e:
        print(f"Run failed: {e}")
        sys.exit(1)
    sys.exit(0)
