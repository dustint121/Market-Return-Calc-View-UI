"""
Registers the daily treemap flow as a Prefect Cloud deployment.

Prefect reads the code from GitHub at run time, so the worker on the droplet
always runs the latest commit on the main branch (no redeploy needed for code changes).

Run once (and again only if you change the schedule, entrypoint, or pool):
    python deploy_prefect.py
"""
from prefect import flow
from prefect.runner.storage import GitRepository
from prefect.schedules import Cron

REPO_URL = "https://github.com/dustint121/Market-Return-Calc-View-UI.git"
WORK_POOL = "droplet-process-pool"

if __name__ == "__main__":
    flow.from_source(
        source=GitRepository(url=REPO_URL, branch="main"),
        entrypoint="gen_daily_treemap.py:daily_treemap_flow",
    ).deploy(
        name="weekday-treemap",
        work_pool_name=WORK_POOL,
        # 6:00 PM New York time, Monday to Friday (same as the old 22:00 UTC cron during EDT)
        schedule=Cron("0 18 * * 1-5", timezone="America/New_York"),
        parameters={"date": None, "storage": "s3"},
        concurrency_limit=1,  # never run two copies at once (memory)
        tags=["market", "treemap"],
        description="Daily S&P 500 treemap snapshot (replaces crontab).",
    )
