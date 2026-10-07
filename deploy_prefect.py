"""
Registers the daily treemap flow as a Prefect Cloud deployment on a
Prefect Managed work pool (the only pool type on the free Hobby plan).

Prefect runs the flow on its own servers: it pulls the code from GitHub,
installs PIP_PACKAGES, and runs gen_daily_treemap.py:daily_treemap_flow.
No worker is needed on the droplet.

Run once (and again only if you change the schedule, packages, entrypoint, or pool):
    python deploy_prefect.py
"""
from prefect import flow
from prefect.runner.storage import GitRepository
from prefect.schedules import Cron

REPO_URL = "https://github.com/dustint121/Market-Return-Calc-View-UI.git"
WORK_POOL = "managed-pool"

# Only what the treemap job needs (Flask etc. are not needed here).
# prefect itself is already in the managed image, so it is not listed.
PIP_PACKAGES = [
    "beautifulsoup4==4.14.3",
    "boto3==1.42.30",
    "html5lib==1.1",
    "lxml==6.0.2",
    "numpy==2.4.1",
    "pandas==2.3.3",
    "pandas_market_calendars==5.2.4",
    "plotly==6.5.2",
    "python-dotenv==1.2.1",
    "requests==2.32.5",
    "yfinance==1.0",
]

if __name__ == "__main__":
    flow.from_source(
        source=GitRepository(url=REPO_URL, branch="main"),
        entrypoint="gen_daily_treemap.py:daily_treemap_flow",
    ).deploy(
        name="weekday-treemap",
        work_pool_name=WORK_POOL,
        job_variables={"pip_packages": PIP_PACKAGES},
        # 6:00 PM New York time, Monday to Friday
        schedule=Cron("0 18 * * 1-5", timezone="America/New_York"),
        parameters={"date": None, "storage": "s3"},
        concurrency_limit=1,
        tags=["market", "treemap"],
        description="Daily S&P 500 treemap snapshot (replaces crontab).",
    )
