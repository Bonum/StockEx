import sys
sys.path.insert(0, "/app")

import time, json, threading, uuid
import quickfix as fix
import quickfix44 as fix44

from shared.config import Config
from shared.kafka_utils import create_producer, create_consumer

# Order tracking for execution reports
order_sessions = {}  # cl_ord_id -> (sessionID, order_details)
order_sessions_lock = threading.Lock()

class Application(fix.Application):
    def __init__(self):
        super().__init__()
        self.producer = create_producer(component_name="FIX-OEG")
        self.sessions = {}  # sessionID -> session for sending messages

    def onCreate(self, sessionID):
        print("FIX-OEG onCreate:", sessionID)
        self.sessions[sessionID] = sessionID

    def onLogon(self, sessionID):
        print("FIX-OEG onLogon:", sessionID)

    def onLogout(self, sessionID):
        print("FIX-OEG onLogout:", sessionID)
        if sessionID in self.sessions:
            del self.sessions[sessionID]

    def toAdmin(self, message, sessionID): pass
    def fromAdmin(self, message, sessionID): pass
    def toApp(self, message, sessionID): pass

    def fromApp(self, message, sessionID):
        msgType = fix.MsgType()
        message.getHeader().getField(msgType)
        mtype = msgType.getValue()
        if mtype == fix.MsgType_NewOrderSingle:
            self.onNewOrderSingle(message, sessionID)
        elif mtype == fix.MsgType_OrderCancelRequest:
            self.onOrderCancelRequest(message, sessionID)
        elif mtype == fix.MsgType_OrderCancelReplaceRequest:
            self.onOrderCancelReplaceRequest(message, sessionID)
        else:
            print("FIX-OEG: Unsupported MsgType:", mtype)
            self.sendReject(sessionID, message, f"Unsupported message type: {mtype}")

    def validateNewOrderSingle(self, message):
        """Validate required fields for NewOrderSingle. Returns (valid, error_msg)."""
        errors = []

        def gf(tag, name):
            try:
                return message.getField(tag)
            except Exception:
                return None

        # Required fields
        cl_ord_id = gf(11, "ClOrdID")
        symbol = gf(55, "Symbol")
        side = gf(54, "Side")
        qty = gf(38, "OrderQty")

        if not cl_ord_id:
            errors.append("Missing required field ClOrdID (11)")
        if not symbol:
            errors.append("Missing required field Symbol (55)")
        if not side:
            errors.append("Missing required field Side (54)")
        elif side not in ("1", "2"):
            errors.append(f"Invalid Side (54): must be 1 (Buy) or 2 (Sell), got {side}")
        if not qty:
            errors.append("Missing required field OrderQty (38)")
        else:
            try:
                qty_val = float(qty)
                if qty_val <= 0:
                    errors.append(f"OrderQty (38) must be positive, got {qty_val}")
            except ValueError:
                errors.append(f"Invalid OrderQty (38): {qty}")

        # Price validation for limit orders
        ord_type = gf(40, "OrdType")
        if ord_type == "2":  # Limit order
            price = gf(44, "Price")
            if not price:
                errors.append("Missing Price (44) for limit order")
            else:
                try:
                    if float(price) <= 0:
                        errors.append(f"Price (44) must be positive, got {price}")
                except ValueError:
                    errors.append(f"Invalid Price (44): {price}")

        return (len(errors) == 0, "; ".join(errors))

    def sendReject(self, sessionID, original_msg, reason):
        """Send a Reject message (35=3) for invalid messages."""
        reject = fix.Message()
        reject.getHeader().setField(fix.MsgType(fix.MsgType_Reject))
        reject.setField(fix.RefSeqNum(0))  # Reference sequence number
        reject.setField(fix.Text(reason))
        try:
            fix.Session.sendToTarget(reject, sessionID)
            print(f"FIX-OEG: Sent Reject: {reason}")
        except Exception as e:
            print(f"FIX-OEG: Failed to send Reject: {e}")

    def sendExecutionReport(self, sessionID, order, exec_type, ord_status,
                           exec_qty=0, exec_price=0, cum_qty=0, leaves_qty=0):
        """Send an ExecutionReport (35=8)."""
        exec_report = fix.Message()
        exec_report.getHeader().setField(fix.MsgType(fix.MsgType_ExecutionReport))

        # Required fields
        exec_report.setField(fix.OrderID(order.get('order_id', str(uuid.uuid4()))))
        exec_report.setField(fix.ClOrdID(order.get('cl_ord_id', '')))
        exec_report.setField(fix.ExecID(str(uuid.uuid4())))
        exec_report.setField(fix.ExecType(exec_type))
        exec_report.setField(fix.OrdStatus(ord_status))
        exec_report.setField(fix.Symbol(order.get('symbol', '')))
        exec_report.setField(fix.Side(fix.Side_BUY if order.get('type') == 'buy' else fix.Side_SELL))
        exec_report.setField(fix.OrderQty(order.get('quantity', 0)))

        if order.get('price'):
            exec_report.setField(fix.Price(order.get('price')))

        # Fill information
        exec_report.setField(fix.LastShares(exec_qty))
        exec_report.setField(fix.LastPx(exec_price))
        exec_report.setField(fix.CumQty(cum_qty))
        exec_report.setField(fix.LeavesQty(leaves_qty))
        exec_report.setField(fix.AvgPx(exec_price if cum_qty > 0 else 0))

        try:
            fix.Session.sendToTarget(exec_report, sessionID)
            print(f"FIX-OEG: Sent ExecutionReport: ExecType={exec_type}, OrdStatus={ord_status}")
        except Exception as e:
            print(f"FIX-OEG: Failed to send ExecutionReport: {e}")

    def onNewOrderSingle(self, message, sessionID):
        # Validate message first
        valid, error_msg = self.validateNewOrderSingle(message)
        if not valid:
            self.sendReject(sessionID, message, error_msg)
            return

        def gf(tag, default=None, cast=str):
            try:
                return cast(message.getField(tag))
            except Exception:
                return default

        cl_ord_id = gf(11)
        symbol    = gf(55, "FOO")
        side_val  = gf(54, "1")  # 1=Buy, 2=Sell
        qty       = gf(38, 0.0, float)
        price     = gf(44, 0.0, float)

        order_id = cl_ord_id or f"fix-{int(time.time()*1000)}"
        order = {
            "order_id": order_id,
            "cl_ord_id": cl_ord_id,
            "symbol": symbol,
            "side": "BUY" if str(side_val) == "1" else "SELL",
            "quantity": qty,
            "price": price,
            "timestamp": time.time(),
            "source": "fix-oeg"
        }

        # Track order for execution reports
        with order_sessions_lock:
            order_sessions[cl_ord_id] = (sessionID, order)

        try:
            meta = self.producer.send(Config.ORDERS_TOPIC, value=order).get(timeout=10)
            print(f"📥 FIX → Kafka: {order}")
            print(json.dumps({"component":"fix-oeg","event":"order_received","payload":{"order":order,"topic":meta.topic,"partition":meta.partition,"offset":meta.offset}}))

            # Send Execution Report: New (ExecType=0, OrdStatus=0)
            self.sendExecutionReport(
                sessionID, order,
                exec_type=fix.ExecType_NEW,
                ord_status=fix.OrdStatus_NEW,
                leaves_qty=qty
            )
        except Exception as e:
            print(json.dumps({"component":"fix-oeg","event":"produce_failed","payload":{"order":order,"error":str(e)}}))

    def onOrderCancelRequest(self, message, sessionID):
        """Handle Order Cancel Request (35=F)."""
        def gf(tag, default=None):
            try:
                return message.getField(tag)
            except Exception:
                return default

        orig_cl_ord_id = gf(41)  # OrigClOrdID
        cl_ord_id = gf(11)  # ClOrdID (for cancel request)
        symbol = gf(55)

        if not orig_cl_ord_id:
            self.sendReject(sessionID, message, "Missing OrigClOrdID (41)")
            return

        cancel_msg = {
            "type": "cancel",
            "orig_cl_ord_id": orig_cl_ord_id,
            "cl_ord_id": cl_ord_id,
            "symbol": symbol,
            "timestamp": time.time(),
            "source": "fix-oeg"
        }

        try:
            self.producer.send(Config.ORDERS_TOPIC, value=cancel_msg).get(timeout=10)
            print(f"📥 FIX Cancel → Kafka: {cancel_msg}")
        except Exception as e:
            print(f"FIX-OEG: Failed to send cancel: {e}")

    def onOrderCancelReplaceRequest(self, message, sessionID):
        """Handle Order Cancel/Replace Request (35=G) - amend order price/quantity."""
        def gf(tag, default=None, cast=str):
            try:
                return cast(message.getField(tag))
            except Exception:
                return default

        orig_cl_ord_id = gf(41)  # OrigClOrdID - order to modify
        cl_ord_id = gf(11)  # ClOrdID - new ID for modified order
        symbol = gf(55)
        side = gf(54)
        new_qty = gf(38, 0.0, float)  # New OrderQty
        new_price = gf(44, 0.0, float)  # New Price

        if not orig_cl_ord_id:
            self.sendReject(sessionID, message, "Missing OrigClOrdID (41)")
            return

        amend_msg = {
            "type": "amend",
            "orig_cl_ord_id": orig_cl_ord_id,
            "cl_ord_id": cl_ord_id,
            "symbol": symbol,
            "side": "buy" if str(side) == "1" else "sell",
            "quantity": new_qty,
            "price": new_price,
            "timestamp": time.time(),
            "source": "fix-oeg"
        }

        # Track for execution reports
        with order_sessions_lock:
            order_sessions[cl_ord_id] = (sessionID, amend_msg)

        try:
            self.producer.send(Config.ORDERS_TOPIC, value=amend_msg).get(timeout=10)
            print(f"📥 FIX Amend → Kafka: {amend_msg}")
        except Exception as e:
            print(f"FIX-OEG: Failed to send amend: {e}")


def start_trades_consumer(app):
    """Consume trades from Kafka and send execution reports to FIX clients."""
    try:
        consumer = create_consumer(
            topics=[Config.TRADES_TOPIC],
            group_id="fix-oeg-execreports",
            component_name="FIX-OEG-TradesConsumer"
        )
    except Exception as e:
        print(f"FIX-OEG: Failed to create trades consumer: {e}")
        return

    for msg in consumer:
        try:
            trade = msg.value
            buy_id = trade.get('buy_id')
            sell_id = trade.get('sell_id')
            price = trade.get('price', 0)
            qty = trade.get('quantity', 0)

            # Send fill report to buyer
            with order_sessions_lock:
                if buy_id and buy_id in order_sessions:
                    session_id, order = order_sessions[buy_id]
                    # Calculate remaining quantity (simplified - would need proper tracking)
                    app.sendExecutionReport(
                        session_id, order,
                        exec_type=fix.ExecType_TRADE,
                        ord_status=fix.OrdStatus_FILLED,  # Simplified: assume full fill
                        exec_qty=qty,
                        exec_price=price,
                        cum_qty=qty,
                        leaves_qty=0
                    )
                    print(f"FIX-OEG: Sent fill report to buyer {buy_id}")

                # Send fill report to seller
                if sell_id and sell_id in order_sessions:
                    session_id, order = order_sessions[sell_id]
                    app.sendExecutionReport(
                        session_id, order,
                        exec_type=fix.ExecType_TRADE,
                        ord_status=fix.OrdStatus_FILLED,
                        exec_qty=qty,
                        exec_price=price,
                        cum_qty=qty,
                        leaves_qty=0
                    )
                    print(f"FIX-OEG: Sent fill report to seller {sell_id}")

        except Exception as e:
            print(f"FIX-OEG: Error processing trade: {e}")


if __name__ == "__main__":
    settings = fix.SessionSettings("fix_server.cfg")
    app = Application()
    store_factory = fix.FileStoreFactory(settings)
    log_factory = fix.FileLogFactory(settings)
    acceptor = fix.SocketAcceptor(app, store_factory, settings, log_factory)
    acceptor.start()
    print("FIX OEG Gateway listening on 0.0.0.0:5001 (FIX 4.4)")

    # Start trades consumer for execution reports
    trades_thread = threading.Thread(target=start_trades_consumer, args=(app,), daemon=True)
    trades_thread.start()
    print("FIX-OEG: Started trades consumer for execution reports")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        acceptor.stop()
