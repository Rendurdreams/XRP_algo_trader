####### Coinbase websocket - Test

import websocket
import json
import threading
from xrp_db_manager import connect_db, insert_data

global_ws = None

# Websocket callback functions
def on_message(ws, message):
    print("Received Message:", message)
    data = json.loads(message)
    if data['type'] == 'ticker' and data['product_id'] == 'XRP-USD':
        process_data(data)

def on_error(ws, error):
    print("Error:", error)

def on_close(ws):
    print("WebSocket Closed")

def on_open(ws):
    global global_ws
    global_ws = ws
    print("WebSocket Opened")
    subscribe_message = {
        "type": "subscribe",
        "channels": [{"name": "ticker", "product_ids": ["XRP-USD"]}]
    }
    ws.send(json.dumps(subscribe_message))

def process_data(data):
    insert_data_tuple = (
        data.get('time'),
        float(data.get('price', 0)),
        float(data.get('open_24h', 0)),
        float(data.get('volume_24h', 0)),
        float(data.get('low_24h', 0)),
        float(data.get('high_24h', 0)),
        float(data.get('best_bid', 0)),
        float(data.get('best_bid_size', 0)),
        float(data.get('best_ask', 0)),
        float(data.get('best_ask_size', 0)),
        data.get('side'),
        int(data.get('trade_id', 0)),
        float(data.get('last_size', 0))
    )
    conn = connect_db()
    insert_data(conn, insert_data_tuple)
    conn.close()

# Starts connection
def start_websocket():
    websocket_url = "wss://ws-feed.pro.coinbase.com"
    ws = websocket.WebSocketApp(websocket_url,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.run_forever()

# Stops connection
def stop_websocket():
    global global_ws
    if global_ws is not None:
        global_ws.close()

# Run listener
if __name__ == "__main__":
    ws_thread = threading.Thread(target=start_websocket)
    ws_thread.start()

    input("Press Enter to stop WebSocket...\n")
    stop_websocket()
    ws_thread.join()

   