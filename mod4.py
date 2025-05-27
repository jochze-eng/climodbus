import sqlite3
import asyncio
from threading import Thread
from datetime import datetime, timezone
from flask import Flask, request, jsonify
from pymodbus.server.async_io import StartAsyncTcpServer
from pymodbus.datastore import ModbusSlaveContext, ModbusServerContext
from pymodbus.datastore.store import ModbusSequentialDataBlock
import logging

# Configure logging
logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.INFO)

DB_FILE = "coils.db"
COIL_COUNT = 30

# Register datetime adapter and converter for SQLite
def adapt_datetime_isoformat(val):
    return val.isoformat()

def convert_datetime_isoformat(val):
    return datetime.fromisoformat(val.decode())

sqlite3.register_adapter(datetime, adapt_datetime_isoformat)
sqlite3.register_converter("timestamp", convert_datetime_isoformat)

# Initialize database
conn = sqlite3.connect(DB_FILE, check_same_thread=False, detect_types=sqlite3.PARSE_DECLTYPES)
cursor = conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS coils (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        address INTEGER UNIQUE NOT NULL,
        state INTEGER NOT NULL CHECK(state IN (0, 1)),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")
conn.commit()

# Ensure all 30 coils exist in DB
for i in range(COIL_COUNT):
    cursor.execute("INSERT OR IGNORE INTO coils (address, state) VALUES (?, 0)", (i,))
conn.commit()

# Load coil states with print verification
cursor.execute("SELECT address, state FROM coils ORDER BY address")
raw_coils = cursor.fetchall()
coil_states = [0] * COIL_COUNT
for address, state in raw_coils:
    if 0 <= address < COIL_COUNT:
        coil_states[address] = state

print("\n[DEBUG] Loaded coil states from database:")
for i, state in enumerate(coil_states):
    print(f"Address {i}: State {state}")

# Pad list with a leading dummy 0 to offset pymodbus issue
padded_coil_states = [0] + coil_states

# Initialize Modbus context
store = ModbusSlaveContext(
    co=ModbusSequentialDataBlock(0, padded_coil_states),
    di=ModbusSequentialDataBlock(0, padded_coil_states),
    hr=ModbusSequentialDataBlock(0, [0] * (COIL_COUNT + 1)),
    ir=ModbusSequentialDataBlock(0, [0] * (COIL_COUNT + 1))
)
context = ModbusServerContext(slaves=store, single=True)

# Flask app
app = Flask(__name__)

@app.route("/coils", methods=["GET"])
def get_coils():
    slave = context[0x00]
    coils = slave.getValues(1, 1, count=COIL_COUNT)
    return jsonify({"coils": coils})

@app.route("/coil/<int:coil_id>", methods=["GET"])
def get_coil(coil_id):
    if not 0 <= coil_id < COIL_COUNT:
        return jsonify({"error": "Invalid coil ID"}), 400
    slave = context[0x00]
    state = slave.getValues(1, coil_id, count=1)[0]
    return jsonify({"coil_id": coil_id, "state": state})

@app.route("/coil/<int:coil_id>", methods=["POST"])
def set_coil(coil_id):
    if not 0 <= coil_id < COIL_COUNT:
        return jsonify({"error": "Invalid coil ID"}), 400

    data = request.get_json()
    value = data.get("state")
    if value not in [0, 1]:
        return jsonify({"error": "State must be 0 or 1"}), 400

    slave = context[0x00]
    slave.setValues(1, coil_id, [value])

    cursor.execute("UPDATE coils SET state = ?, updated_at = ? WHERE address = ?",
                   (value, datetime.now(timezone.utc), coil_id))
    conn.commit()

    print(f"[DEBUG] Updated coil {coil_id} to state {value} in DB and Modbus")

    return jsonify({"message": f"Coil {coil_id} set to {value}"})

@app.route("/coils/bulk", methods=["POST"])
def bulk_update():
    data = request.get_json()
    updates = data.get("updates")
    if not isinstance(updates, list):
        return jsonify({"error": "Expected list of updates"}), 400

    slave = context[0x00]
    for update in updates:
        coil_id = update.get("coil_id")
        state = update.get("state")
        if not (isinstance(coil_id, int) and 0 <= coil_id < COIL_COUNT and state in [0, 1]):
            continue

        slave.setValues(1, coil_id + 1, [state])
        cursor.execute("UPDATE coils SET state = ?, updated_at = ? WHERE address = ?",
                       (state, datetime.now(timezone.utc), coil_id))

    conn.commit()
    print("[DEBUG] Bulk update applied")
    return jsonify({"message": "Bulk update completed"})

# Async Modbus server
async def start_modbus_server():
    await StartAsyncTcpServer(context=context, address=("0.0.0.0", 502))

def run_modbus_thread():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(start_modbus_server())

Thread(target=run_modbus_thread, daemon=True).start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)