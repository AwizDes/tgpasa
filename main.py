# main.py
import os
import time
import threading
import asyncio
import uvloop
import traceback
from collections import defaultdict, deque

import gradio as gr
from pyrogram import Client, filters
from pyrogram.handlers import MessageHandler
from pyrogram.raw.functions.channels import DeleteMessages

# Optional S3 backup (only used if AWS env vars + bucket provided)
try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
    BOTO3_AVAILABLE = True
except Exception:
    BOTO3_AVAILABLE = False


SESSIONS_DIR = "sessions"
os.makedirs(SESSIONS_DIR, exist_ok=True)


# ---------- helper: threaded uvloop runner ----------
class LoopThread(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.loop = None
        self.started = threading.Event()

    def run(self):
        # Install uvloop policy in this thread
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.started.set()
        self.loop.run_forever()

    def stop(self):
        if self.loop and self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.join(timeout=5)


# ---------- Bot manager ----------
class BotManager:
    def __init__(self):
        self.loop_thread = None
        self.runner_future = None
        self.running = False
        self.log_lines = deque(maxlen=2000)
        self._lock = threading.Lock()

        # state (per-run)
        self._client = None
        self._stop_async_event = None

    def _log(self, *parts):
        text = " ".join(str(p) for p in parts)
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] {text}"
        with self._lock:
            self.log_lines.append(line)
        print(line)

    def get_logs(self):
        with self._lock:
            return list(self.log_lines)

    def start(self, api_id: int, api_hash: str, target_chat_id: int,
              bad_rolls: set, target_good_dice: int, your_user_id: int = None):
        if self.running:
            self._log("⚠ Bot already running. Stop it first.")
            return

        # Ensure loop thread exists
        if not self.loop_thread or not self.loop_thread.is_alive():
            self.loop_thread = LoopThread()
            self.loop_thread.start()
            # wait until loop ready
            self.loop_thread.started.wait(timeout=5)

        # schedule runner coroutine in that loop
        coro = self._runner(api_id, api_hash, target_chat_id,
                            bad_rolls, target_good_dice, your_user_id)
        self.runner_future = asyncio.run_coroutine_threadsafe(coro, self.loop_thread.loop)
        self.running = True
        self._log("▶ Bot start requested. Running in background loop thread.")

    def stop(self):
        if not self.running:
            self._log("⚠ Bot not running.")
            return

        # set the asyncio Event created in the runner to request shutdown
        if self._stop_async_event is not None and self.loop_thread and self.loop_thread.loop:
            self.loop_thread.loop.call_soon_threadsafe(self._stop_async_event.set)
            self._log("⏹ Stop signal sent — waiting for client to shutdown...")
        else:
            self._log("⚠ Couldn't send stop signal (no event).")

        # Don't block here waiting for complete shutdown. The generator stream will show final logs.
        self.running = False

    # ---------- internal runner ----------
    async def _runner(self, api_id, api_hash, target_chat_id,
                      bad_rolls, target_good_dice, your_user_id):
        # per-run state
        messages_to_delete = []
        last_bad_rolls = {}
        bad_roll_occurrences = defaultdict(int)
        good_dice_count = 0

        session_filename = os.path.join(SESSIONS_DIR, f"{api_id}.session")
        self._log("Session file:", session_filename)
        # Ensure sessions dir exists
        os.makedirs(SESSIONS_DIR, exist_ok=True)

        # define helper functions used in the bot
        async def send_replacement_until_good(client, chat_id):
            nonlocal good_dice_count, bad_roll_occurrences, messages_to_delete, last_bad_rolls
            while True:
                try:
                    new_msg = await client.send_dice(chat_id)
                    val = getattr(new_msg.dice, "value", None)
                    self._log(" ↻ Replacement rolled", val)
                    if val in bad_rolls:
                        bad_roll_occurrences[val] += 1
                        messages_to_delete.append(new_msg)
                        last_bad_rolls[val] = new_msg
                        self._log(f" ❌ Bad replacement {val}, retrying...")
                        await asyncio.sleep(0.3)
                        continue
                    good_dice_count += 1
                    self._log(f" ✓ Replacement accepted good {val} → total {good_dice_count}")
                    return new_msg
                except Exception as e:
                    self._log("❌ Error during replacement roll:", e)
                    await asyncio.sleep(1)

        async def perform_batch_deletion(client):
            try:
                if not messages_to_delete:
                    self._log("🧹 No bad messages queued.")
                    return
                keep_ids = {msg.id for msg in last_bad_rolls.values() if msg}
                all_ids = {msg.id for msg in messages_to_delete if msg}
                delete_ids = list(all_ids - keep_ids)
                if delete_ids:
                    self._log(f"🧹 Deleting {len(delete_ids)} messages, keeping {len(keep_ids)} latest bad rolls...")
                    peer = await client.resolve_peer(target_chat_id)
                    await client.invoke(DeleteMessages(channel=peer, id=delete_ids))
                    self._log(f"✅ Deleted {len(delete_ids)} messages successfully.")
                else:
                    self._log("🧹 Nothing to delete after filtering.")
            except Exception as e:
                self._log("❌ Deletion error:", e)

        def reset_session_state():
            nonlocal good_dice_count, bad_roll_occurrences, messages_to_delete, last_bad_rolls
            good_dice_count = 0
            bad_roll_occurrences.clear()
            messages_to_delete.clear()
            last_bad_rolls.clear()

        # define handler
        async def handle_dice(client, message):
            nonlocal good_dice_count, bad_roll_occurrences, messages_to_delete, last_bad_rolls
            try:
                if message.chat.id != target_chat_id:
                    return
                val = message.dice.value
                sender_name = message.from_user.first_name if message.from_user else "Unknown User"
                self._log(f"🎲 {sender_name} rolled: {val} ({good_dice_count + 1}/{target_good_dice})")

                if val in bad_rolls:
                    bad_roll_occurrences[val] += 1
                    messages_to_delete.append(message)
                    last_bad_rolls[val] = message
                    occurrence = bad_roll_occurrences[val]

                    if occurrence == 1:
                        good_dice_count += 1
                        self._log(f"⚠ First occurrence of bad roll {val}: counted as good.")
                    else:
                        self._log(f"❌ Duplicate bad roll {val} — sending replacements.")
                        await send_replacement_until_good(client, message.chat.id)
                else:
                    good_dice_count += 1
                    self._log(f"✓ Good roll: {val} → total {good_dice_count}")

                if good_dice_count >= target_good_dice:
                    self._log(f"\n🎯 Target reached ({good_dice_count}/{target_good_dice}). Cleaning up...")
                    await perform_batch_deletion(client)
                    reset_session_state()
                    self._log("✅ Round complete! Ready for next one.\n")
            except Exception as e:
                self._log("❌ Handler error:", e)
                traceback.print_exc()

        # Try to start the Pyrogram client
        try:
            self._log("🔌 Starting Pyrogram client...")
            client = Client(session_filename, api_id=api_id, api_hash=api_hash)
            self._client = client

            await client.start()
            self._log(f"⚡ Client started as {await client.get_me().then(lambda m: m.first_name) if False else 'user'} (session created).")
            # Add message handler for dice
            client.add_handler(MessageHandler(handle_dice, filters.chat(target_chat_id) & filters.dice))

            # create an asyncio Event we can set from another thread to stop
            stop_event = asyncio.Event()
            self._stop_async_event = stop_event

            self._log("✅ Bot running. Listening for dice in chat:", target_chat_id)

            # If S3 is configured, try to download existing session at start (optional)
            if BOTO3_AVAILABLE and os.environ.get("AWS_S3_BUCKET"):
                bucket = os.environ.get("AWS_S3_BUCKET")
                key = f"sessions/{os.path.basename(session_filename)}"
                try:
                    s3 = boto3.client("s3")
                    self._log("⬇ Checking S3 for an existing session file:", key)
                    s3.download_file(bucket, key, session_filename)
                    self._log("⬇ Downloaded session from S3 (if it existed).")
                except Exception:
                    self._log("⬇ No session in S3 (or download failed). Continuing with a fresh session.")

            # Wait until stop_event is set from main thread (via loop.call_soon_threadsafe)
            await stop_event.wait()
            self._log("⏳ Stop event received — closing Pyrogram client...")

            # When stopping: perform final cleanup deletion if needed
            try:
                await perform_batch_deletion(client)
            except Exception as e:
                self._log("❌ Error performing final cleanup:", e)

            await client.stop()
            self._log("🛑 Client stopped cleanly.")
        except Exception as e:
            self._log("❌ Exception in bot runner:", e)
            traceback.print_exc()
        finally:
            # Mark not running
            self.running = False

            # If S3 configured, attempt to upload session file for persistence
            if BOTO3_AVAILABLE and os.environ.get("AWS_S3_BUCKET"):
                bucket = os.environ.get("AWS_S3_BUCKET")
                key = f"sessions/{os.path.basename(session_filename)}"
                try:
                    s3 = boto3.client("s3")
                    if os.path.exists(session_filename):
                        self._log("⬆ Uploading session file to S3 for persistence...")
                        s3.upload_file(session_filename, bucket, key)
                        self._log("⬆ Uploaded session to S3.")
                except Exception as e:
                    self._log("❌ Could not upload session to S3:", e)

            # remove references
            self._client = None
            self._stop_async_event = None
            self._log("🏁 Runner coroutine ended.")


# ---------- Gradio UI wiring ----------
bot_manager = BotManager()

def parse_bad_rolls(text):
    if not text:
        return {3,4,6}
    parts = [p.strip() for p in text.split(",") if p.strip()]
    s = set()
    for p in parts:
        try:
            s.add(int(p))
        except:
            pass
    return s if s else {3,4,6}

def start_and_stream(api_id, api_hash, target_chat_id, bad_rolls_text, target_good_dice, your_user_id):
    # basic validation & parsing
    try:
        api_id = int(str(api_id).strip())
    except:
        yield "❌ API ID must be an integer."
        return

    api_hash = str(api_hash).strip()
    if not api_hash:
        yield "❌ API HASH is required."
        return

    try:
        tcid = int(str(target_chat_id).strip())
    except:
        yield "❌ TARGET CHAT ID must be an integer (e.g. -1001234567890)."
        return

    try:
        target_good_dice = int(target_good_dice)
    except:
        target_good_dice = 6

    bad_rolls = parse_bad_rolls(bad_rolls_text)
    try:
        your_user_id = int(str(your_user_id).strip()) if your_user_id and str(your_user_id).strip() else None
    except:
        your_user_id = None

    bot_manager.start(api_id, api_hash, tcid, bad_rolls, target_good_dice, your_user_id)

    # streaming loop: yield logs while bot_manager.running is True
    yield "▶ Bot started. Streaming logs..."
    while bot_manager.running:
        logs = "\n".join(bot_manager.get_logs())
        yield logs
        time.sleep(0.6)

    # once stopped, yield final logs and a closing message
    yield "\n".join(bot_manager.get_logs())
    yield "⏹ Bot stopped."

def stop_and_report():
    bot_manager.stop()
    # Return immediate feedback; stream will show final logs soon
    return "⏹ Stop requested. Bot will shut down shortly."

# Build the Gradio interface
with gr.Blocks(title="Telegram Dice Monitor (Gradio)") as demo:
    gr.Markdown("## Telegram Dice Monitor — Start / Stop with live logs (Gradio + uvloop)")
    with gr.Row():
        with gr.Column(scale=2):
            api_id = gr.Textbox(label="API ID", placeholder="e.g. 123456", value="")
            api_hash = gr.Textbox(label="API HASH", placeholder="your api_hash", type="password")
            target_chat_id = gr.Textbox(label="TARGET CHAT ID (numeric)", placeholder="-1001234567890")
            bad_rolls = gr.Textbox(label="Bad Rolls (comma-separated)", value="3,4,6", placeholder="3,4,6")
            target_good_dice = gr.Number(label="Target Good Dice", value=6)
            your_user_id = gr.Textbox(label="Your User ID (optional)", placeholder="your Telegram user id")
            with gr.Row():
                start_btn = gr.Button("Start Bot (stream logs)")
                stop_btn = gr.Button("Stop Bot")
        with gr.Column(scale=3):
            console = gr.Textbox(label="Console logs (streaming)", lines=20)

    # Wiring: start_btn triggers a generator that streams into console
    start_btn.click(fn=start_and_stream,
                    inputs=[api_id, api_hash, target_chat_id, bad_rolls, target_good_dice, your_user_id],
                    outputs=console)

    stop_btn.click(fn=stop_and_report, inputs=None, outputs=console)

# Launch: use PORT env var from Render
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 7860))
    demo.launch(server_name="0.0.0.0", server_port=port, share=False)
