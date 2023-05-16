import logging
import sqlite3
from datetime import datetime

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup
from highcharts import Highchart
from IPython.display import HTML, display


def gold_crawl() -> None:
    """
    Fetch the historical gold price
    from bank of Taiwan
    """

    # parameter config
    url = "https://rate.bot.com.tw/gold/chart/year/TWD"
    conn = init_database()
    cur = conn.cursor()
    gold_info_list = []

    # start fetch
    response = requests.get(url)
    try:
        if response.ok:
            soup = BeautifulSoup(response.text, "html5lib")
            rows = soup.find_all("tr")
            for row in rows:
                individual_gold_info = []
                # To append information
                cells = row.find_all("td")
                for cell in cells:
                    cell_content = cell.getText()
                    individual_gold_info.append(cell_content)
                gold_info_list.append(individual_gold_info)
    except Exception as err:
        logging.error(
            f"Something went wrong during fetch gold price,error msg : {str(err)}"
        )

    # Remove nan list using list comprehension
    filter_list = [gold_info for gold_info in gold_info_list if gold_info]

    for gold_info in filter_list:
        date = gold_info[0]
        money_type = gold_info[1]
        weight = gold_info[2]
        buy = gold_info[3]
        sell = gold_info[4]

        # insert information into database
        cur.execute(
            f"INSERT INTO gold (type, buy, sell, date) VALUES ('{money_type}', '{int(buy)}', '{int(sell)}', '{date}')"
        )
        conn.commit()
        conn.close


def init_database():
    try:
        conn = sqlite3.connect("gold.db")
        logging.info("Database connect successfully.")
        cur = conn.cursor()
        # delete previous record prevent duplicate value
        cur.execute("DROP TABLE IF EXISTS gold")
        cur.execute(
            """CREATE TABLE IF NOT EXISTS gold
             (type TEXT,
             buy INT,
             sell INT,
             date TIMESTAMP);"""
        )
        logging.info("Gold table successful create.")

        return conn
    except Exception as err:
        logging.error(f"Create database filed, error msg : {str(err)}")
        raise


def selectAll() -> None:
    """
    fetch information from db
    and send into chart make function
    """

    # connect to db
    conn = sqlite3.connect("gold.db")
    cursor = conn.execute("SELECT * FROM gold")
    rows = cursor.fetchall()

    # create list to store gold information
    time_list = []
    buy_list = []
    sell_list = []

    for row in rows:
        correct_format_date = datetime.strptime(row[3], "%Y/%m/%d")
        time_list.append(correct_format_date)
        buy_list.append(row[1])
        sell_list.append(row[2])

    # reverse list to make date from old to new
    time_list.reverse()
    buy_list.reverse()
    sell_list.reverse()

    conn.close()

    # draw chart
    drawHighchart(time_list, buy_list, sell_list)


def drawHighchart(time_list: list, buy_list: list, sell_list: list) -> None:
    """
    Draw visual chart
    """

    # parameter config
    color = "#4285f4"
    linewidth = 2
    title = "Gold price from Taiwan bank"
    width = 800
    height = 500

    # start drawing
    H = Highchart(width=width, height=height)

    buy_line = [[index, s] for index, s in zip(time_list, buy_list)]
    sell_line = [[index, s] for index, s in zip(time_list, sell_list)]

    H.add_data_set(buy_line, "line", "buy price", color="blue")
    H.add_data_set(sell_line, "line", "sell price", color="red")

    H.set_options(
        "xAxis", {"type": "datetime", "labels": {"format": "{value:%Y/%m/%d}"}}
    )
    H.set_options("title", {"text": title, "style": {"color": "black"}})
    H.set_options(
        "plotOptions",
        {"line": {"lineWidth": linewidth, "dataLabels": {"enabled": False}}},
    )
    H.set_options("tooltip", {"shared": True, "crosshairs": True})

    # show chart
    H.save_file("chart")
    display(HTML("chart.html"))


if __name__ == "__main__":
    gold_crawl()
    selectAll()
