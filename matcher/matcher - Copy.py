import threading, time, json
from flask import Flask, jsonify
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = 'kafka:9092'

import json, sys, datetime
def jlog(event_type, payload):
    out = {
        "ts": datetime.datetime.utcnow().isoformat() + "Z",
        "component": "matcher",
        "event": event_type,
        "payload": payload
    }
    sys.stdout.write(json.dumps(out, default=str) + "\n")
    sys.stdout.flush()

app = Flask(__name__)

order_book = {'buy': [], 'sell': []}
trades = []
producer = None

def create_kafka_producer(retries=20, delay=2):
    global producer
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print('Producer connected')
            return
        except NoBrokersAvailable:
            print('Producer: Kafka not ready, retry', i)
            time.sleep(delay)
    raise RuntimeError('Producer: cannot connect to Kafka')

def create_kafka_consumer(topic='orders', retries=20, delay=2):
    for i in range(retries):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=BOOTSTRAP,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='matcher-group'
            )
            print('Consumer connected to topic', topic)
            return consumer
        except NoBrokersAvailable:
            print('Consumer: Kafka not ready, retry', i)
            time.sleep(delay)
    raise RuntimeError('Consumer: cannot connect to Kafka')

def find_match(order):
    side = order['type']
    opp = 'sell' if side == 'buy' else 'buy'
    candidates = [o for o in order_book[opp] if o['symbol'] == order['symbol'] and o['quantity'] > 0]
    if not candidates:
        return None
    # best-price matching: for buy -> lowest sell price; for sell -> highest buy price
    if side == 'buy':
        valid = [o for o in candidates if o['price'] <= order['price']]
        if not valid: return None
        best_price = min(o['price'] for o in valid)
        bests = [o for o in valid if o['price'] == best_price]
        return min(bests, key=lambda x: x['timestamp'])
    else:
        valid = [o for o in candidates if o['price'] >= order['price']]
        if not valid: return None
        best_price = max(o['price'] for o in valid)
        bests = [o for o in valid if o['price'] == best_price]
        return min(bests, key=lambda x: x['timestamp'])

def process_order(order):
    print('Processing order:', order)
    while order['quantity'] > 0:
        matched = find_match(order)
        if not matched:
            order_book[order['type']].append(order.copy())
            print('Added to book:', order)
            return
        traded_qty = min(order['quantity'], matched['quantity'])
        trade_price = matched['price']
        trade = {
            'buy_order_id': order['order_id'] if order['type']=='buy' else matched['order_id'],
            'sell_order_id': matched['order_id'] if order['type']=='buy' else order['order_id'],
            'symbol': order['symbol'],
            'price': trade_price,
            'quantity': traded_qty,
            'timestamp': time.time()
        }
        trades.append(trade)
        if producer:
            producer.send('trades', value=trade)
            producer.flush()
        print('TRADE:', trade)
        order['quantity'] -= traded_qty
        matched['quantity'] -= traded_qty
        if matched['quantity'] == 0:
            try:
                order_book['sell' if order['type']=='buy' else 'buy'].remove(matched)
                print('Removed matched from book:', matched)
            except ValueError:
                pass

def consumer_loop():
    consumer = create_kafka_consumer('orders')
    for msg in consumer:
        try:
            order = msg.value
            order['price'] = float(order['price'])
            order['quantity'] = int(order['quantity'])
            order.setdefault('timestamp', time.time())
            process_order(order)
        except Exception as e:
            print('Error processing message:', e)

@app.route('/book')
def get_book():
    # present best bids/asks sorted
    return jsonify(order_book)

@app.route('/trades')
def get_trades():
    return jsonify(trades)

if __name__ == '__main__':
    create_kafka_producer()
    t = threading.Thread(target=consumer_loop, daemon=True)
    t.start()
    app.run(host='0.0.0.0', port=6000)