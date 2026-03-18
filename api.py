from flask import Flask, request
from scanner import scan_all, get_upbit_markets
import time

app = Flask(__name__)

CACHE_TTL = 300
CACHE = {
    "ts": 0,
    "data": None
}

print("INITIAL SCAN START")
try:
    CACHE["data"] = scan_all()
    CACHE["ts"] = time.time()
    print("INITIAL SCAN DONE")
except Exception as e:
    print("INITIAL SCAN ERROR:", e)


def get_scan_data():
    now = time.time()

    if CACHE["data"] is not None and (now - CACHE["ts"] < CACHE_TTL):
        return CACHE["data"]

    data = scan_all()
    CACHE["ts"] = now
    CACHE["data"] = data
    return data


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
    data = get_scan_data()

    if mode not in ["main", "sub"]:
        mode = "main"

    coins = data.get(mode, [])

    return {
        "scan_time_kst": data["scan_time_kst"],
        "mode": mode,
        "universe_count": data["universe_count"],
        "count": len(coins),
        "coins": coins
    }


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
