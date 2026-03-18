from flask import Flask, request
from scanner import scan, get_upbit_markets

app = Flask(__name__)

@app.route("/health")
def health():
    return {"ok": True}

@app.route("/debug/markets")
def debug_markets():
    markets = get_upbit_markets()
    return {
        "count": len(markets),
        "sample": markets[:10]
    }

@app.route("/scan/latest")
def latest():
    mode = request.args.get("mode", "main")
    coins = scan()

    if mode == "main":
        coins = coins[:2]

    return {
        "count": len(coins),
        "coins": coins
    }

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
