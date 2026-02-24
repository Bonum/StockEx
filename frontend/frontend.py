import sys
sys.path.insert(0, "/app")

from flask import Flask, request, jsonify, render_template
import json, time, os

from shared.config import Config
from shared.kafka_utils import create_producer

app = Flask(__name__, template_folder=os.getenv("TEMPLATE_FOLDER", "templates"))

producer = create_producer(component_name="Frontend")

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/health')
def health():
    """Health check endpoint."""
    import requests
    status = {
        "status": "healthy",
        "service": "frontend",
        "timestamp": time.time()
    }
    # Check Kafka producer
    try:
        producer.flush(timeout=1)
        status["kafka"] = "connected"
    except Exception as e:
        status["kafka"] = f"error: {e}"
        status["status"] = "degraded"

    # Check matcher connectivity
    try:
        r = requests.get(f"{Config.MATCHER_URL}/health", timeout=2)
        if r.status_code == 200:
            status["matcher"] = "connected"
        else:
            status["matcher"] = f"error: status {r.status_code}"
    except Exception as e:
        status["matcher"] = f"error: {e}"

    return jsonify(status)

@app.route('/order', methods=['POST'])
def send_order():
    if request.is_json:
        data = request.json
    else:
        data = {
            'order_id': request.form.get('order_id') or str(time.time_ns()),
            'symbol': request.form.get('symbol'),
            'type': request.form.get('type'),
            'quantity': int(request.form.get('quantity')),
            'price': float(request.form.get('price')),
            'timestamp': time.time()
        }
    producer.send(Config.ORDERS_TOPIC, value=data)
    producer.flush()
    return jsonify({'status':'sent','data':data})

@app.route('/securities')
def securities():
    symbols = []
    try:
        with open(Config.SECURITIES_FILE) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    symbols.append(line.split('\t')[0])
    except Exception:
        pass
    return jsonify(symbols)

@app.route('/book')
def book():
    import requests
    symbol = request.args.get('symbol', '')
    if not symbol:
        return jsonify({"bids": [], "asks": []})
    r = requests.get(f"{Config.MATCHER_URL}/orderbook/{symbol}", timeout=3)
    return (r.text, r.status_code, {'Content-Type': 'application/json'})

@app.route('/trades')
def trades():
    import requests
    r = requests.get(f"{Config.MATCHER_URL}/trades", timeout=2)
    return (r.text, r.status_code, {'Content-Type': 'application/json'})

if __name__ == '__main__':
    port = int(os.getenv('PORT', '5000'))
    app.run(host='0.0.0.0', port=port)


@app.route('/submit', methods=['POST'])
def submit_order():
    from flask import request
    data = None
    try:
        data = request.get_json(force=True)
    except Exception as e:
        return jsonify({'status':'badjson','error': str(e)}), 400
    if not data:
        return jsonify({'status':'no_data'}), 400
    try:
        fut = producer.send(Config.ORDERS_TOPIC, value=data)
        meta = fut.get(timeout=10)
        producer.flush()
        app.logger.info("Produced order %s -> topic=%s partition=%s offset=%s", data.get('order_id'), meta.topic, meta.partition, meta.offset)
        return jsonify({'status':'sent','data':data})
    except Exception as e:
        app.logger.exception("Failed producing order: %s", e)
        return jsonify({'status':'failed','error':str(e)}), 500
