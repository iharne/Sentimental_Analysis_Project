from flask import Flask, jsonify
# Import the data processing function from your sentiment analysis script
from sentimentalAnalysis_REDDIT_MASTER import fetch_and_analyze_data

app = Flask(__name__)

@app.route('/data')
def get_data():
    # Call the data processing function from your script
    data = fetch_and_analyze_data()
    # Convert the JSON string to a Flask JSON response
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True)
