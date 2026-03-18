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
    """
    Query params:
    - limit: int (default 15, max 50)
    - timeframe: str (default 1h)
    - market: str (default spot)
    """
    try:
        limit = request.args.get("limit", default=15, type=int)
        timeframe = request.args.get("timeframe", default="1h", type=str)
        market = request.args.get("market", default="spot", type=str)

        limit = max(1, min(limit, 50))
        if timeframe not in {"15m", "30m", "1h", "4h", "1d"}:
            return jsonify({
                "status": "error",
                "message": "Unsupported timeframe. Use one of: 15m, 30m, 1h, 4h, 1d"
            }), 400

        if market not in {"spot"}:
            return jsonify({
                "status": "error",
                "message": "Currently only 'spot' market is supported"
            }), 400

        result = run_gilsu_scan(limit=limit, timeframe=timeframe, market=market)
        return jsonify(result), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
