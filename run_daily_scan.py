import json
from scanner import scan

def run():
    coins = scan()
    result = {
        "main": {"count": len(coins), "coins": coins[:2]},
        "sub": {"count": len(coins), "coins": coins}
    }
    with open("result.json", "w") as f:
        json.dump(result, f)

if __name__ == "__main__":
    run()
