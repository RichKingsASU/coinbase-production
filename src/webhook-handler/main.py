import json, os, hmac
from datetime import datetime, timezone
from flask import Flask, request, jsonify
from google.cloud import pubsub_v1, secretmanager

app = Flask(__name__)
PROJECT_ID = os.environ["GOOGLE_CLOUD_PROJECT"]
TOPIC = os.getenv("TOPIC", "trading-signals")
pub = pubsub_v1.PublisherClient()
topic_path = pub.topic_path(PROJECT_ID, TOPIC)
sm = secretmanager.SecretManagerServiceClient()

def get_secret(name):
    return sm.access_secret_version(
        request={"name": f"projects/{PROJECT_ID}/secrets/{name}/versions/latest"}
    ).payload.data.decode()

WEBHOOK_SECRET = get_secret("tradingview-webhook-secret")

 @app.get("/health")
def health(): return jsonify(ok=True), 200

 @app.post("/webhook/tradingview")
def tv_webhook():
    try:
        body = request.get_json(force=True)
    except Exception:
        return jsonify(error="invalid json"), 400

    provided = request.headers.get("X-Webhook-Secret") or body.get("secret")
    if not provided or not hmac.compare_digest(provided.encode(), WEBHOOK_SECRET.encode()):
        return jsonify(error="unauthorized"), 401

    symbol = (body.get("symbol") or body.get("ticker") or "").replace(":", "-")
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
    pub.publish(topic_path, json.dumps(event).encode("utf-8"))
    return jsonify(ok=True), 200
