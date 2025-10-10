import os
import asyncio
import uvloop
import gradio as gr
from pyrogram import Client
from pyrogram.errors import SessionPasswordNeeded
from motor.motor_asyncio import AsyncIOMotorClient

SESSION_DIR = "./sessions"
os.makedirs(SESSION_DIR, exist_ok=True)

MONGO_URI = os.environ.get("MONGO_URI")
DB_NAME = "tgpasa"
COLLECTION_NAME = "settings"

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

db_client = AsyncIOMotorClient(MONGO_URI)
db = db_client[DB_NAME][COLLECTION_NAME]

client = None
running = False
log_buffer = []

good_dice_count = 0
bad_roll_occurrences = {}
messages_to_delete = {}
last_bad_rolls = {}

# ---------------- UTILS ----------------
def log(msg):
    print(msg)
    log_buffer.append(msg)
    if len(log_buffer) > 300:
        log_buffer.pop(0)

async def save_settings(data):
    await db.update_one({}, {"$set": data}, upsert=True)

async def load_settings():
    doc = await db.find_one({})
    return doc or {}

def reset_round():
    global good_dice_count, bad_roll_occurrences, messages_to_delete, last_bad_rolls
    good_dice_count = 0
    bad_roll_occurrences = {}
    messages_to_delete = {}
    last_bad_rolls = {}

# ---------------- AUTHENTICATION ----------------
async def start_bot_with_auth(api_id, api_hash, phone_number, target_chat, bad_rolls, target_good_dice=6):
    global client, running

    session_file = os.path.join(SESSION_DIR, f"{api_id}.session")
    client = Client(
        name=session_file,
        api_id=int(api_id),
        api_hash=api_hash,
        workdir=SESSION_DIR
    )

    await client.connect()
    if not await client.is_connected():
        log("❌ Could not connect")
        return "\n".join(log_buffer)

    # Check if first-time login
    if not os.path.exists(session_file):
        log(f"📱 First-time login detected. Sending code to {phone_number}...")
        try:
            await client.send_code_request(phone_number)
            log("✅ Code sent. Enter code in 'Auth Code' input.")
        except Exception as e:
            log(f"❌ Error sending code: {e}")
            return "\n".join(log_buffer)

    running = True
    reset_round()

    # Wait for auth code input via UI
    return "\n".join(log_buffer)

async def complete_auth(auth_code):
    global client
    try:
        await client.sign_in(code=auth_code)
    except SessionPasswordNeeded:
        # 2FA password
        log("Enter your 2FA password in the same field.")
        return "\n".join(log_buffer)
    log("✅ Authentication successful!")
    return "\n".join(log_buffer)

# ---------------- GRADIO UI ----------------
async def start(api_id, api_hash, phone_number, target_chat, bad_rolls, target_good_dice=6):
    await save_settings({
        "api_id": api_id,
        "api_hash": api_hash,
        "phone_number": phone_number,
        "target_chat": target_chat,
        "bad_rolls": bad_rolls,
        "target_good_dice": target_good_dice
    })
    asyncio.create_task(start_bot_with_auth(api_id, api_hash, phone_number, target_chat, bad_rolls, target_good_dice))
    return "\n".join(log_buffer)

with gr.Blocks() as demo:
    gr.Markdown("## 🎯 Dice Bot Controller with Online Auth")
    with gr.Row():
        api_id = gr.Textbox(label="API ID")
        api_hash = gr.Textbox(label="API HASH")
    phone_number = gr.Textbox(label="Phone Number (+65...)")
    target_chat = gr.Textbox(label="Target Chat ID")
    target_good_dice = gr.Number(label="Target Good Dice", value=6)
    bad_rolls = gr.Textbox(label="Bad Rolls (comma separated)", value="1,2")
    auth_code = gr.Textbox(label="Auth Code (for first-time login)")
    console = gr.Textbox(label="Console Output", lines=20, interactive=False)

    start_btn = gr.Button("🚀 Start & Send Code")
    verify_btn = gr.Button("✅ Complete Auth")

    start_btn.click(start, inputs=[api_id, api_hash, phone_number, target_chat, bad_rolls, target_good_dice], outputs=console)
    verify_btn.click(complete_auth, inputs=[auth_code], outputs=console)

port = int(os.environ.get("PORT", 7860))
demo.launch(server_name="0.0.0.0", server_port=port)
