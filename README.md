# About
My project repo for creating an application to simplifying the process of calculating potential returns in the market (S&P 500) using the python yfinance API.


**Page 1**: Has daily treemaps for the composition of the entire S&P 500. Components are size-based on market caps and colored (green/red) based on daily return compared to previous close.  [Example here](https://market-return-calc-project1.s3.us-west-1.amazonaws.com/treemaps/2026-01-20_treemap.html) 
Treemaps can be stored locally in directory or with AWS S3.

Charts are inspired by the visualizations in the daily StockTwits newsletter made by FinViz found [here](https://finviz.com/map.ashx?t=sec&utm_source=dailyrip&utm_medium=newsletter&utm_campaign=email&_bhlid=cbb28bc82581f0f05f301a711c8c9b20a670e957)


**Page 2**: Has live candle-stick chart of S&P 500 that updates every minute while the market is open. Shows opening, low, high, and closing price per minute.


**Page 3**: Interface to calculate returns from the market in any period between 1975-Present. Has options for:
* Contribution per interval
* Contribution per interval
* Investing strategy: Dollar-cost averaging or 'Buying the Dip' 
* Interval Section : weekly, monthly, biannual, etc.


# Instructions for Running Code Repo on Local Machine

## In Project File after git cloning

1. [Optional] Add **.env** file for functionality to allow an AWS S3 bucket to store treemaps. 

> AWS_ACCESS_KEY_ID=

> AWS_SECRET_ACCESS_KEY =

> AWS_S3_BUCKET_NAME=

> AWS_REGION_NAME=

2. Run
>`pip install -r requirements.txt`

3. Run
>`python app.py `


## Updated treemaps and candlestick charts
The treemap charts on Page2 and candlestick chart on Page 3 are meant to be updated externally.

### Updating manually
Run this to get new treemaps
> python gen_daily_treemap.py [yyyy-mm-dd] [local]

* The second argument is if you want to make a treemap that is not the current date.
* The third argument is to decide if you want to store the file locally rather than with AWS S3. It is stored on AWS S3 by default otherwise.

Run this to update candlestick chart
> `gen_candlestick_chart.py`

### Updating with crontab (Linux OS only)
with crontab, you can set the OS to run scripts automatically at specified times and/or intervals

Use this to access crontab to edit
> `crontab -e`

Add these lines or the equivalent for your setup in the cron file.
>`CRON_TZ=America/New_York`

>`1 16 * * * /usr/bin/python path/gen_daily_treemap.py`

> `* * * * * /usr/bin/python path/gen_candlestick_chart.py`

This will have the OS run the treemap script at 4:01 p.m. New York time everyday and the candlestick script every minute.


### Python Prefect
Prefect Cloud can replace crontab for the daily treemap job. The flow is defined in `gen_daily_treemap.py` (`daily_treemap_flow`) and the deployment is registered by `deploy_prefect.py`.

This setup uses a **Prefect Managed** work pool, which is the pool type available on the free Hobby plan. Prefect runs the flow on its own servers, so no worker is needed on your machine or server.

#### How Prefect Cloud gets the code
Prefect Cloud does not upload your local files. `deploy_prefect.py` only saves the GitHub repo URL, branch (`main`), and entrypoint (`gen_daily_treemap.py:daily_treemap_flow`) to Prefect Cloud. At every scheduled run, Prefect:
1. Clones the latest `main` branch of the GitHub repo into a temporary folder.
2. Installs the packages listed in `PIP_PACKAGES` in `deploy_prefect.py`.
3. Runs `daily_treemap_flow`, then deletes the temporary folder.

Because of this, code changes only take effect after they are pushed to GitHub. After that, no redeploy is needed. Rerun `deploy_prefect.py` only if you change the schedule, packages, entrypoint, or work pool.

#### 1. Push the code to GitHub
Commit and push the Prefect-related files so Prefect Cloud can read them from the repo.
> `git add func.py gen_daily_treemap.py deploy_prefect.py requirements.txt`

> `git commit -m "Add Prefect flow and deployment"`

> `git push origin main`

#### 2. Log in to Prefect Cloud
1. Create a free account and workspace at [https://app.prefect.cloud/](https://app.prefect.cloud/).
2. Install the requirements (includes `prefect`).
> `pip install -r requirements.txt`

3. Log in from the command line.
> `prefect cloud login`

* Choose to log in with a web browser, or paste an API key. To use an API key, create one in the Prefect Cloud UI under your account settings > **API Keys**, then run `prefect cloud login -k <YOUR_API_KEY>`.
* Select your workspace when asked.
* Check the login with `prefect cloud workspace ls`.

#### 3. Create the managed work pool
> `prefect work-pool create managed-pool --type prefect:managed`

* The name must match `WORK_POOL` in `deploy_prefect.py` (`managed-pool`).
* UI alternative: **Work Pools** > **+** > **Prefect Managed** > name it `managed-pool` > **Create**.
* Check it with `prefect work-pool ls`.

#### 4. Add AWS credentials as Secret blocks (Prefect Cloud UI)
The `.env` file is not in GitHub, so Prefect Managed runs cannot read it. Instead, `func.py` (`get_setting()`) reads `.env` first and falls back to Prefect Secret blocks when a value is missing.

In [https://app.prefect.cloud/](https://app.prefect.cloud/):
1. Open **Blocks** in the left sidebar and click **+**.
2. Search for and select **Secret**, then click **Create**.
3. Enter the **Block Name** and the **Value**, then click **Create**.
4. Repeat for each of the three blocks below. Block names must match exactly.

| Block Name | Value (same as in `.env`) |
|---|---|
| `aws-access-key-id` | `AWS_ACCESS_KEY_ID` |
| `aws-secret-access-key` | `AWS_SECRET_ACCESS_KEY` |
| `aws-s3-bucket-name` | `AWS_S3_BUCKET_NAME` |

#### 5. Create the deployment
From the project folder (while logged in to Prefect Cloud), run:
> `python deploy_prefect.py`

This registers the deployment `daily-sp500-treemap/weekday-treemap` with:
* Code source: this GitHub repo, `main` branch
* Schedule: 6:00 p.m. New York time, Monday to Friday (`0 18 * * 1-5`, `America/New_York`)
* Parameters: `date=None` (today in New York time) and `storage="s3"`
* At most one run at a time

#### Verify the deployment
1. Open [https://app.prefect.cloud/](https://app.prefect.cloud/) and go to the **Deployments** tab.
2. Confirm `weekday-treemap` (flow `daily-sp500-treemap`) is listed with the `managed-pool` work pool and the weekday 6:00 p.m. schedule.
3. Click the deployment to see its upcoming scheduled runs, parameters, and settings.

You can also check from the command line:
> `prefect deployment ls`

> `prefect deployment inspect 'daily-sp500-treemap/weekday-treemap'`


### Apache Airflow
