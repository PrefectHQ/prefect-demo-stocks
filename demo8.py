import random
import httpx
from datetime import date
import pandas as pd
import yfinance as yf
from prefect import flow, task

# adds subflows, removes Slack webhook
# sublows good if want to use
# different task runners, group flows, param input from GUI

today = date.today().isoformat()


@task(retries=5, retry_delay_seconds=1)
def fetch_data(ticker="AAPL"):
    """Fetch stock data for past month from buggy api"""
    stock_df = yf.download(f"{ticker}", period="1mo")
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
    stock_df.to_csv(f"{ticker}_moving_avg_{today}.csv")
    return "success"


@flow
def pipeline(ticker: str):
    """Main pipeline"""
    stock_data = fetch_data(ticker)
    transformed_data = transform_data(stock_data)
    save_data(stock_df=transformed_data, ticker=ticker)
    return transform_data


@flow()
def fetch_weather(lat: float, lon: float):
    """get weather data"""
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="temperature_2m"),
    )
    most_recent_temp = weather.json()["hourly"]["temperature_2m"][0]
    return most_recent_temp


@task
def combine(stock_df: pd.DataFrame, temp: float):
    combined_df = stock_df.copy()
    combined_df = stock_df["temp"] = temp
    return combined_df


@flow
def combine_pipes(ticker: str, lat: float, lon: float):
    """combine the data from both sources"""
    stock_df = pipeline(ticker)
    temp = fetch_weather(lat, lon, stock_df)
    combo_df = combine(stock_df, temp)
    # save_data(combo_df, ticker)


if __name__ == "__main__":
    combine_pipes(ticker="AAPL", lat=38.9, lon=77.1)


# can run with python demo8.py
# observe in gui radar chart
# or
# prefect deployment build demo8.py:pipeline -n subflows -o "subflow-deployment.yaml"
# prefect deployment apply subflow-deployment.yaml
# run manually from gui
