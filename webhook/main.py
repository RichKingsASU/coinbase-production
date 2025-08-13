import json, os, hmac, hashlib
from datetime import datetime, timezone
from flask import Flask, request, jsonify
from google.cloud import pubsub_v1, secretmanager

app = Flask(__name__)
PROJECT_ID = os.environ["GOOGLE_CLOUD_PROJECT"]
TOPIC = os.getenv("TOPIC", "trading-signals")
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC)

_sm = secretmanager.SecretManagerServiceClient()
def get_secret(name: str) -> str:
    resp = _sm.access_secret_version(
        request={"name": f"projects/{PROJECT_ID}/secrets/{name}/versions/latest"}
    )
    return resp.payload.data.decode()

WEBHOOK_SECRET = get_secret("tradingview-webhook-secret")

def constant_time_equals(a: str, b: str) -> bool:
    return hmac.compare_digest(a.encode(), b.encode())

@app.route("/health", methods=["GET"])
def health():
    return jsonify(ok=True), 200

@app.route("/webhook/tradingview", methods=["POST"])
def webhook():
    # Strategy: require a secret in body OR header.
    # (TradingView does not add a signature by default.)
    try:
        body = request.get_json(force=True, silent=False)
    except Exception:
        return jsonify(error="Invalid JSON"), 400

    # Accept either header or field inside JSON alert
    provided = request.headers.get("X-Webhook-Secret") or body.get("secret")
    if not provided or not constant_time_equals(provided, WEBHOOK_SECRET):
        return jsonify(error="Unauthorized"), 401

    # Validate minimal fields from your alert template
    symbol = (body.get("symbol") or body.get("ticker") or "").replace(":","-")
    action = (body.get("action") or body.get("signal") or "").lower()
    price  = body.get("price")
    if action not in {"buy","sell"} or not symbol:
        return jsonify(error="symbol/action required"), 400

    event = {
        "symbol": symbol if "-" in symbol else f"{symbol}-USD",
        "action": action,
        "price": price,
        "ts": datetime.now(timezone.utc).isoformat()
    }
    publisher.publish(topic_path, json.dumps(event).encode("utf-8"))
    return jsonify(ok=True), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))