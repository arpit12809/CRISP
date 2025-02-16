import flask 
from flask import Flask, render_template
import requests
import sqlite3


app = Flask(__name__)

PORT = 8001

@app.route('/', methods=['GET'])
def home():
    database = sqlite3.connect('./Captured_requests.db')
    cursor = database.cursor()
    entries = cursor.execute('select * from all_requests order by Request_Number desc')
    
    return render_template("home.html", entries=entries)


if __name__ == "__main__":
    app.run(port=PORT)