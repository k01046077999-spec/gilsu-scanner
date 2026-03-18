from flask import Flask, request, jsonify
import json, os

app = Flask(__name__)
DATA_FILE = "result.json"

@app.route("/health")
def health():
    return {"ok": True}

@app.route("/scan/latest")
def latest():
    mode = request.args.get("mode", "main")
    if not os.path.exists(DATA_FILE):
        return {"coins": [], "count": 0}
    with open(DATA_FILE) as f:
        data = json.load(f)
    return data.get(mode, {"coins": [], "count": 0})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
