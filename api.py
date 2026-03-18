from flask import Flask, request
from scanner import scan

app = Flask(__name__)

@app.route("/health")
def health():
    return {"ok": True}

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
