import asyncio
import os
import gradio as gr
import uvloop
from pyrogram import Client
from motor.motor_asyncio import AsyncIOMotorClient

# ========== CONFIG ==========
MONGO_URI = os.environ.get("MONGO_URI")
DB_NAME = "tgpasa"
COLLECTION_NAME = "settings"
SESSION_DIR = "./sessions"
# ============================

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

db_client = AsyncIOMotorClient(MONGO_URI)
db = db_client[DB_NAME][COLLECTION_NAME]

# Global variables
client = None
running = False
log_buffer = []

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

async def run_bot(api_id, api_hash, target_chat, bad_rolls, user_id):
    global client, running
    log("Starting Pyrogram client...")

    client = Client(
        name=str(api_id),
        api_id=int(api_id),
        api_hash=api_hash,
        workdir=SESSION_DIR,
        in_memory=False
    )

    await client.start()
    log("Bot started ✅")

    target_chat = int(target_chat)
    bad_rolls = [int(x.strip()) for x in bad_rolls.split(",") if x.strip().isdigit()]

    running = True

    while running:
        try:
            msg = await client.send_dice(target_chat, "🎲")
            value = msg.dice.value
            log(f"Rolled: {value}")

            if value in bad_rolls:
                log(f"Bad roll {value}, retrying...")
                await asyncio.sleep(2)
                continue

            log(f"Good roll {value}, stopping!")
            break

        except Exception as e:
            log(f"Error: {e}")
            await asyncio.sleep(5)

    await client.stop()
    log("Bot stopped ⛔")

async def stop_bot():
    global running, client
    running = False
    if client:
        await client.stop()
    log("Stopped manually ⛔")

async def start(api_id, api_hash, target_chat, bad_rolls, user_id):
    await save_settings({
        "api_id": api_id,
        "api_hash": api_hash,
        "target_chat": target_chat,
        "bad_rolls": bad_rolls,
        "user_id": user_id,
    })
    asyncio.create_task(run_bot(api_id, api_hash, target_chat, bad_rolls, user_id))
    return "\n".join(log_buffer)

async def stop():
    await stop_bot()
    return "\n".join(log_buffer)

async def load_defaults():
    data = await load_settings()
    return (
        data.get("api_id", ""),
        data.get("api_hash", ""),
        data.get("target_chat", ""),
        data.get("bad_rolls", "1,2"),
        data.get("user_id", ""),
    )

with gr.Blocks(theme=gr.themes.Soft()) as demo:
    gr.Markdown("## 🎯 Dice Bot Controller")
    with gr.Row():
        api_id = gr.Textbox(label="API ID")
        api_hash = gr.Textbox(label="API HASH")
    with gr.Row():
        target_chat = gr.Textbox(label="Target Chat ID")
        user_id = gr.Textbox(label="User ID (optional)")
    bad_rolls = gr.Textbox(label="Bad Rolls (comma separated)", value="1,2")
    console = gr.Textbox(label="Console Output", lines=20, interactive=False)
    start_btn = gr.Button("🚀 Start")
    stop_btn = gr.Button("🛑 Stop")

    start_btn.click(start, inputs=[api_id, api_hash, target_chat, bad_rolls, user_id], outputs=console)
    stop_btn.click(stop, outputs=console)

    demo.load(load_defaults, outputs=[api_id, api_hash, target_chat, bad_rolls, user_id])

demo.queue()
port = int(os.environ.get("PORT", 7860))
demo.launch(server_name="0.0.0.0", server_port=port)

