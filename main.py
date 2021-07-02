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

# function to run query 2
def queryTwo():

    # define the query
    QUERY = (
        'select names.string_field_1 as Country, trade_deficit_value '
        'from ('
        'select exports.country_code, (imports.imports - exports.exports) as trade_deficit_value '
        'from ('
        'select country_code, SUM(value) as exports '
        'from `s3298931-a1-2.gsquarterlySeptember20.gsquarterlySeptember20` '
        'where status = "F" and account = "Exports" and product_type = "Goods" and '
        'time_ref >= 201400 and time_ref <= 201612 '
        'group by country_code) exports '

        'inner join '

        '(select country_code, SUM(value) as imports '
        'from `s3298931-a1-2.gsquarterlySeptember20.gsquarterlySeptember20` '
        'where status = "F" and account = "Imports" and product_type = "Goods" and '
        'time_ref >= 201400 and time_ref <= 201612 '
        'group by country_code '
        ') as imports '
        'on exports.country_code = imports.country_code '
        'limit 50 '
        ') as primary '

        'inner join '

        '`s3298931-a1-2.gsquarterlySeptember20.country_classification` as names '
        'on primary.country_code = names.string_field_0 '
        'order by trade_deficit_value DESC'
    )

    # execute the query
    query_job = client.query(QUERY)
    rows = query_job.result()

    # create an empty list to store the results
    results_list = []

    for e in rows:
        results_list.append(e)

    return results_list

# home page
@app.route("/")
def home():

    query_one = []
    query_one = queryOne()

    query_two = queryTwo()

    return render_template("index.html", q_one=query_one, q_two=query_two)
    
if __name__ == "__main__":
    app.run(debug=True)
