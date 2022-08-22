import random
from prefect import flow, task
from prefect.blocks.notifications import SlackWebhook
import yfinance as yf



@task
def get_data(ticker="AAPL"):
    stock = yf.download(f"{ticker}", start="2022-08-01", end="2022-08-20")
    print(stock)
    return stock


@task(retries=3)
def call_unreliable_api():
    choices = [{"data": 42}, "failure"]
    res = random.choice(choices)
    if res == "failure":
        raise Exception("Our unreliable service failed")
    else:
        return res


@task
def augment_data(data: dict, msg: str):
    data["message"] = msg
    return data


@task
def write_results_to_database(data: dict):
    print(f"Wrote {data['message']} to database successfully!")
    slack_webhook_block = SlackWebhook.load("message-jeff")
    slack_webhook_block.notify(
        "Hello from Prefect! Wrote {data} to database successfully!"
    )
    return "Success!"


@flow
def pipeline1(msg: str):
    stock_data = get_data()
    api_result = call_unreliable_api()
    augmented_data = augment_data(data=api_result, msg=msg)
    write_results_to_database(augmented_data)

    if random.randint(1, 2) == 2:
        augmented_data = augment_data(data=api_result, msg="hi")
    else:
        augmented_data = augment_data(data=api_result, msg="hello")


if __name__ == "__main__":
    pipeline1("Doing it")
