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
ALLOWED_IPS = {"52.89.214.238", "34.212.75.30", "54.218.53.128", "52.32.178.7"}

@app.get("/health")
def health(): return jsonify(ok=True), 200

@app.post("/webhook/tradingview")
def tv_webhook():
    # GCP's load balancer will add the client's IP to this header.
    forwarded_for = request.headers.get('X-Forwarded-For')
    if not forwarded_for or forwarded_for.split(',')[-1].strip() not in ALLOWED_IPS:
        return jsonify(error="unauthorized ip"), 401

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
