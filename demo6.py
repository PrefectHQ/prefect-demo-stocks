import random
from datetime import date
import pandas as pd
import yfinance as yf
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect.blocks.system import Secret

# adds Secret block

today = date.today().isoformat()


@task(retries=5, retry_delay_seconds=2, cache_key_fn=task_input_hash)
def fetch_data(ticker="AAPL"):
    """Fetch stock data for past month from buggy api"""
    stock_df = yf.download(f"{ticker}", period="1mo")
    buggy_api_result = random.choice([True, False])
    if buggy_api_result:
        raise Exception("API Failure. 😢")
    return stock_df


@task
def transform_data(stock_df):
    """Add five-day moving average to the DataFrame"""
    moving_avg = stock_df["Adj Close"].rolling(5).mean()
    smaller_df = stock_df.copy().tail(len(moving_avg))
    smaller_df["moving_avg"] = moving_avg
    return smaller_df


@task
def save_data(stock_df, ticker):
    """Save the transformed data and return success message"""
    stock_df.to_csv(f"{ticker}_moving_average_{today}.csv")
    return "success"


@flow
def pipeline(ticker: str):
    # using a secret
    secret_block = Secret.load("secretapikey")

    """Main pipeline"""
    stock_data = fetch_data(ticker)
    transformed_data = transform_data(stock_data)
    save_data_result = save_data(stock_df=transformed_data, ticker=ticker)
    print(save_data_result)
    # Access the stored secret
    print(secret_block.get())


if __name__ == "__main__":
    pipeline(ticker="AAPL")

# prefect deployment apply (until one step)
# run manually from gui
# observe in gui
