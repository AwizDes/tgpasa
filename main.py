import asyncio
import sys
from pyrogram import Client, filters
from pyrogram.raw.functions.channels import DeleteMessages
from collections import defaultdict
import os
from dotenv import load_dotenv
import gradio as gr
from threading import Thread
import queue

# Use uvloop for better performance (Unix only)
if sys.platform != 'win32':
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        print("[Performance] Using uvloop for better async performance")
    except ImportError:
        print("[Info] uvloop not available, using default event loop")
else:
    print("[Info] Running on Windows, using default event loop")

# Load environment variables
load_dotenv()

# Configuration
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")

# Load session string from file instead of env var (Windows has 32KB limit)
SESSION_STRING = os.getenv("SESSION_STRING")  # Try env var first

if not SESSION_STRING and os.path.exists("session_string.txt"):
    with open("session_string.txt", "r") as f:
        SESSION_STRING = f.read().strip()
    print("[Session] Loaded session string from session_string.txt")
elif SESSION_STRING:
    print("[Session] Loaded session string from environment variable")
else:
    print("[Warning] No session string found! Bot will require interactive login.")

# Global state
app = None
good_dice_count = 0
bad_roll_occurrences = defaultdict(int)
messages_to_delete = []
last_bad_rolls = {}
is_running = False
log_queue = queue.Queue()
bot_thread = None
bot_loop = None
accumulated_logs = []
stop_event = None

# Configuration from UI
TARGET_CHAT_ID = None
BAD_ROLLS = set()
TARGET_GOOD_DICE = 6


def setup_session_file():
    """Load session from string if available"""
    # Session string will be used directly by Pyrogram, no file needed
    pass


def log(message):
    """Add message to log queue for display"""
    print(message)
    log_queue.put(message)


async def send_replacement_until_good(client, chat_id):
    """Replacement loop: keep rolling until a good roll appears"""
    global good_dice_count, bad_roll_occurrences, messages_to_delete, last_bad_rolls

    while True:
        new_msg = await client.send_dice(chat_id)
        val = new_msg.dice.value
        log(f" -> Replacement rolled {val}")

        if val in BAD_ROLLS:
            occurrence_before = bad_roll_occurrences[val]
            bad_roll_occurrences[val] += 1
            messages_to_delete.append(new_msg)
            last_bad_rolls[val] = new_msg
            
            # If this is the first occurrence of this bad roll (even in replacement), accept it
            if occurrence_before == 0:
                good_dice_count += 1
                log(f" OK First occurrence of bad roll {val} in replacement: counted as good -> total {good_dice_count}")
                return new_msg
            
            log(f" X Bad replacement {val} (occurrence #{bad_roll_occurrences[val]}), retrying...")
            await asyncio.sleep(0.3)
            continue

        good_dice_count += 1
        log(f" OK Replacement accepted good {val} -> total {good_dice_count}")
        return new_msg


async def perform_batch_deletion():
    """Delete all bad messages except the latest occurrence per bad value"""
    try:
        if not messages_to_delete:
            log("[Clean] No bad messages queued.")
            return

        keep_ids = {msg.id for msg in last_bad_rolls.values() if msg}
        all_ids = {msg.id for msg in messages_to_delete if msg}
        delete_ids = list(all_ids - keep_ids)

        if delete_ids:
            log(f"[Clean] Deleting {len(delete_ids)} messages, keeping {len(keep_ids)} latest bad rolls...")
            await app.get_chat(TARGET_CHAT_ID)
            peer = await app.resolve_peer(TARGET_CHAT_ID)
            await app.invoke(DeleteMessages(channel=peer, id=delete_ids))
            log(f"[OK] Deleted {len(delete_ids)} messages successfully.")
        else:
            log("[Clean] Nothing to delete after filtering.")
    except Exception as e:
        log(f"[Error] Deletion error: {e}")


def reset_session():
    global good_dice_count, bad_roll_occurrences, messages_to_delete, last_bad_rolls
    good_dice_count = 0
    bad_roll_occurrences.clear()
    messages_to_delete.clear()
    last_bad_rolls.clear()


async def handle_dice(client, message):
    global good_dice_count, bad_roll_occurrences, messages_to_delete, last_bad_rolls

    if message.chat.id != TARGET_CHAT_ID:
        return

    val = message.dice.value
    sender_name = message.from_user.first_name if message.from_user else "Unknown User"
    log(f"[Dice] {sender_name} rolled: {val} ({good_dice_count + 1}/{TARGET_GOOD_DICE})")

    if val in BAD_ROLLS:
        bad_roll_occurrences[val] += 1
        messages_to_delete.append(message)
        last_bad_rolls[val] = message
        occurrence = bad_roll_occurrences[val]

        if occurrence == 1:
            good_dice_count += 1
            log(f"[Warning] First occurrence of bad roll {val}: counted as good.")
        else:
            log(f"[X] Duplicate bad roll {val} - sending replacements.")
            await send_replacement_until_good(client, message.chat.id)
    else:
        good_dice_count += 1
        log(f"[OK] Good roll: {val} -> total {good_dice_count}")

    if good_dice_count >= TARGET_GOOD_DICE:
        log(f"\n[Target] Target reached ({good_dice_count}/{TARGET_GOOD_DICE}). Cleaning up...")
        await perform_batch_deletion()
        reset_session()
        log("[OK] Round complete! Ready for next one.\n")


async def start_monitoring():
    global app, stop_event
    
    stop_event = asyncio.Event()
    
    # Use string session if available, otherwise use file-based session
    if SESSION_STRING:
        log(f"[Session] Using string session (length: {len(SESSION_STRING)} chars)")
        log(f"[Session] Session string starts with: {SESSION_STRING[:10]}...")
        app = Client("tgpasa", api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING)
    else:
        log("[Session] No session string found - using file-based session (will require login)")
        app = Client("tgpasa", api_id=API_ID, api_hash=API_HASH)
    
    @app.on_message(filters.chat(TARGET_CHAT_ID) & filters.dice)
    async def dice_handler(client, message):
        await handle_dice(client, message)
    
    async with app:
        me = await app.get_me()
        log(f"[Start] Running as {me.first_name} (ID: {me.id})")
        log(f"[Target] Target chat: {TARGET_CHAT_ID}")
        log(f"[Config] Bad rolls: {BAD_ROLLS}")
        log(f"[Config] Target: {TARGET_GOOD_DICE} total dice before cleanup")
        log("[Wait] Waiting for ANY dice rolls in the target chat...\n")
        await stop_event.wait()


def run_bot_in_thread():
    """Run bot in separate thread with its own event loop"""
    global bot_loop, is_running
    
    # Create new event loop (uvloop will be used if available on Unix)
    if sys.platform != 'win32':
        try:
            import uvloop
            bot_loop = uvloop.new_event_loop()
        except ImportError:
            bot_loop = asyncio.new_event_loop()
    else:
        bot_loop = asyncio.new_event_loop()
    
    asyncio.set_event_loop(bot_loop)
    try:
        bot_loop.run_until_complete(start_monitoring())
    except Exception as e:
        log(f"[Error] Bot error: {e}")
    finally:
        is_running = False
        # Proper cleanup: cancel all pending tasks
        try:
            pending = asyncio.all_tasks(bot_loop)
            for task in pending:
                task.cancel()
            # Wait for all tasks to complete cancellation
            bot_loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except Exception as e:
            log(f"[Debug] Task cleanup: {e}")
        finally:
            try:
                bot_loop.close()
            except:
                pass


def start_bot(chat_id, bad_rolls_input, target_dice):
    global TARGET_CHAT_ID, BAD_ROLLS, TARGET_GOOD_DICE, is_running, bot_thread, accumulated_logs
    
    if is_running:
        return "[Error] Bot is already running!", ""
    
    try:
        accumulated_logs.clear()
        
        TARGET_CHAT_ID = int(chat_id)
        bad_rolls_list = [int(x.strip()) for x in bad_rolls_input.split(",")]
        BAD_ROLLS = set(bad_rolls_list)
        TARGET_GOOD_DICE = int(target_dice)
        
        if not all(1 <= x <= 6 for x in BAD_ROLLS):
            return "[Error] Bad rolls must be between 1 and 6!", ""
        
        if len(BAD_ROLLS) >= 6:
            return "[Error] You must have at least one good roll!", ""
        
        is_running = True
        bot_thread = Thread(target=run_bot_in_thread, daemon=True)
        bot_thread.start()
        
        return "[OK] Bot started successfully!", "[Start] Bot is initializing..."
        
    except ValueError as e:
        is_running = False
        return f"[Error] Invalid input: {e}", ""
    except Exception as e:
        is_running = False
        return f"[Error] Error starting bot: {e}", ""


def stop_bot():
    global is_running, app, bot_loop, stop_event
    if not is_running:
        return "[Error] Bot is not running!"
    
    log("[Stop] Stopping bot gracefully...")
    is_running = False
    
    # Signal the monitoring loop to stop
    if stop_event and bot_loop:
        try:
            bot_loop.call_soon_threadsafe(stop_event.set)
        except Exception as e:
            log(f"[Debug] Error setting stop event: {e}")
    
    # Wait a moment for graceful shutdown
    import time
    time.sleep(1)
    
    log("[Stop] Bot stopped!")
    return "[Stop] Bot stopped!"


def get_logs():
    """Retrieve logs from queue and accumulate them"""
    global accumulated_logs
    
    new_logs = []
    while not log_queue.empty():
        try:
            new_logs.append(log_queue.get_nowait())
        except:
            break
    
    if new_logs:
        accumulated_logs.extend(new_logs)
    
    return "\n".join(accumulated_logs) if accumulated_logs else ""


# Gradio UI
with gr.Blocks(title="Telegram Dice") as demo:
    gr.Markdown("# Telegram Dice Controller")
    
    with gr.Row():
        with gr.Column():
            chat_id_input = gr.Textbox(
                label="Target Chat ID",
                placeholder="-1003107059457",
                value="-1003107059457"
            )
            bad_rolls_input = gr.Textbox(
                label="Bad Rolls (comma-separated)",
                placeholder="enter dice value",
                value="3,4"
            )
            target_dice_input = gr.Textbox(
                label="Target Good Dice Count",
                placeholder="6",
                value="6"
            )
            
            with gr.Row():
                start_btn = gr.Button("Start Bot", variant="primary")
                stop_btn = gr.Button("Stop Bot", variant="stop")
            
            status_output = gr.Textbox(label="Status", interactive=False)
        
        with gr.Column():
            gr.Markdown("### Live Logs")
            log_output = gr.Textbox(
                label="Bot Logs",
                lines=20,
                max_lines=30,
                interactive=False,
                autoscroll=True
            )
    
    start_btn.click(
        fn=start_bot,
        inputs=[chat_id_input, bad_rolls_input, target_dice_input],
        outputs=[status_output, log_output]
    )
    
    stop_btn.click(
        fn=stop_bot,
        outputs=[status_output]
    )
    
    timer = gr.Timer(value=0.5, active=True)
    timer.tick(
        fn=get_logs,
        outputs=[log_output]
    )


if __name__ == "__main__":
    if not API_ID or not API_HASH:
        print("[Error] API_ID and API_HASH must be set in .env file!")
        print("Create a .env file with:")
        print("API_ID=your_api_id")
        print("API_HASH=your_api_hash")
        exit(1)
    
    # Get port from environment variable (Render sets this)
    port = int(os.getenv("PORT", 7860))
    
    # Launch with server_name and port for Render deployment
    demo.launch(
        server_name="0.0.0.0",  # Bind to all interfaces
        server_port=port,        # Use Render's PORT
        share=False              # Don't create Gradio share link
    )
