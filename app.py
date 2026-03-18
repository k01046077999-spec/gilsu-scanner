from flask import Flask, jsonify, request
from scanner import run_gilsu_scan

app = Flask(__name__)


@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "status": "ok",
        "message": "Gilsu scanner API is running"
    }), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok"
    }), 200


@app.route("/scan", methods=["GET"])
def scan():
    try:
        limit = request.args.get("limit", default=10, type=int)
        timeframe = request.args.get("timeframe", default="1h", type=str)

        if limit < 1:
            limit = 1
        if limit > 30:
            limit = 30

        if timeframe not in {"15m", "30m", "1h", "4h", "1d"}:
            return jsonify({
                "status": "error",
                "message": "Unsupported timeframe. Use one of: 15m, 30m, 1h, 4h, 1d"
            }), 400

        result = run_gilsu_scan(limit=limit, timeframe=timeframe)
        return jsonify(result), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
