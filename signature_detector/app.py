import logging

import score
from flask import Flask, jsonify, request

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

score.init()


@app.route("/score", methods=["POST"])
def predict():
    result = score.run(request.get_data(as_text=True))
    return app.response_class(response=result, status=200, mimetype="application/json")


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy"})
