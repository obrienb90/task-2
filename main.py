# imports 
from flask import Flask, redirect, url_for, render_template, request, session

from google.cloud import bigquery

client = bigquery.Client()

# establish flask settings
app = Flask(__name__)
app.secret_key = "zaq12wsx"

# function to format the month / year string
def formatDate(date_str):
    
    # split the data
    date = str(date_str)
    month = date[-2:]
    year = date[0:4]

    if month == "01":
        month = "Jan"
    elif month == "02":
        month = "Feb"
    elif month == "03":
        month = "Mar"
    elif month == "04":
        month = "Apr"
    elif month == "05":
        month = "May"
    elif month == "06":
        month = "Jun"
    elif month == "07":
        month = "Jul"
    elif month == "08":
        month = "Aug"
    elif month == "09":
        month = "Sep"
    elif month == "10":
        month = "Oct"
    elif month == "11":
        month = "Nov"
    elif month == "12":
        month = "Dec"

    return month + " " + year

# function to run query 1
def queryOne():

    # define the query
    QUERY = (
            'SELECT time_ref, SUM(value) AS trade_value ' 
            'FROM `s3298931-a1-2.gsquarterlySeptember20.gsquarterlySeptember20` '
            'GROUP BY time_ref '
            'ORDER BY trade_value DESC '
            'LIMIT 10'
        )
    # execute the query
    query_job = client.query(QUERY)
    rows = query_job.result()

    # create an empty list to store the results
    results_list = []
    dates = []
    values = []

    # create a collection with the results

    for row in rows:
        # date
        date = formatDate(row.time_ref)
        dates.append(date)
        # trade value
        values.append(row.trade_value)

    results_list.append(dates)
    results_list.append(values)

    return results_list

# home page
@app.route("/")
def home():

    query_one = []
    query_one = queryOne()
    
    return render_template("index.html", q_one=query_one)
    
if __name__ == "__main__":
    app.run(debug=True)
