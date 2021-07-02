# imports 
from flask import Flask, redirect, url_for, render_template, request, session

# establish flask settings
app = Flask(__name__)
app.secret_key = "zaq12wsx"

# home page
@app.route("/")
def home():

    return render_template("index.html")
    
if __name__ == "__main__":
    app.run(debug=True)
