import asyncio
import sys
import os
import queue
from threading import Thread
from collections import defaultdict
from pyrogram import Client, filters
from pyrogram.raw.functions.channels import DeleteMessages
from dotenv import load_dotenv
import gradio as gr

# Use uvloop for better performance (Unix only)
if sys.platform != 'win32':
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        print("[Performance] Using uvloop")
    except ImportError:
        pass

load_dotenv()

# Configuration
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")

if not SESSION_STRING and os.path.exists("session_string.txt"):
    with open("session_string.txt", "r") as f:
        SESSION_STRING = f.read().strip()

# Global state
app = None
good_dice_count = 0
bad_roll_occurrences = defaultdict(int)
messages_to_delete = []
last_bad_rolls = {}
all_good_messages = []
active_replacement_tasks = set()
is_running = False
log_queue = queue.Queue()
bot_thread = None
bot_loop = None
accumulated_logs = []
stop_event = None

# Config from UI
TARGET_CHAT_ID = None
BAD_ROLLS = set()
TARGET_GOOD_DICE = 6


def log(message):
    """Add message to log queue"""
    print(message)
    log_queue.put(message)


async def send_replacement_until_good(client, chat_id):
    """Keep rolling until a good roll appears"""
    global good_dice_count, bad_roll_occurrences, messages_to_delete, last_bad_rolls, all_good_messages

    try:
        while True:
            new_msg = await client.send_dice(chat_id)
            val = new_msg.dice.value
            log(f" -> Replacement rolled {val}")

            if val in BAD_ROLLS:
                occurrence_before = bad_roll_occurrences[val]
                bad_roll_occurrences[val] += 1
                messages_to_delete.append(new_msg)
                last_bad_rolls[val] = new_msg
                
                if occurrence_before == 0:
                    good_dice_count += 1
                    all_good_messages.append(new_msg)
                    log(f" OK First occurrence of bad roll {val} in replacement -> total {good_dice_count}")
                    return new_msg
                
                log(f" X Bad replacement {val} (occurrence #{bad_roll_occurrences[val]}), retrying...")
                await asyncio.sleep(0.3)
                continue

            good_dice_count += 1
            all_good_messages.append(new_msg)
            log(f" OK Replacement accepted good {val} -> total {good_dice_count}")
            return new_msg
    except asyncio.CancelledError:
        log(f" [!] Replacement task cancelled")
        raise
    except Exception as e:
        log(f" [Error] Replacement error: {e}")


async def perform_batch_deletion():
    """Delete bad messages (except latest of each) and excess good messages"""
    try:
        keep_bad_ids = {msg.id for msg in last_bad_rolls.values() if msg}
        all_bad_ids = {msg.id for msg in messages_to_delete if msg}
        delete_bad_ids = list(all_bad_ids - keep_bad_ids)
        
        delete_excess_good_ids = []
        if len(all_good_messages) > TARGET_GOOD_DICE:
            excess_count = len(all_good_messages) - TARGET_GOOD_DICE
            log(f"[Clean] Found {len(all_good_messages)} good dice, deleting {excess_count} excess...")
            excess_messages = all_good_messages[:excess_count]
            delete_excess_good_ids = [msg.id for msg in excess_messages if msg]
        
        all_delete_ids = delete_bad_ids + delete_excess_good_ids
        
        if all_delete_ids:
            log(f"[Clean] Deleting {len(delete_bad_ids)} bad rolls, {len(delete_excess_good_ids)} excess good rolls...")
            peer = await app.resolve_peer(TARGET_CHAT_ID)
            await app.invoke(DeleteMessages(channel=peer, id=all_delete_ids))
            log(f"[OK] Deleted {len(all_delete_ids)} messages. Kept exactly {TARGET_GOOD_DICE} good dice.")
        else:
            log(f"[Clean] No messages to delete. Exactly {TARGET_GOOD_DICE} good dice present.")
    except Exception as e:
        log(f"[Error] Deletion error: {e}")


def reset_session():
    """Reset all tracking variables and cancel active tasks"""
    global good_dice_count, bad_roll_occurrences, messages_to_delete, last_bad_rolls
    global active_replacement_tasks, all_good_messages
    
    for task in list(active_replacement_tasks):
        if not task.done():
            task.cancel()
            log(f"[Cancel] Cancelled running replacement task")
    
    good_dice_count = 0
    bad_roll_occurrences.clear()
    messages_to_delete.clear()
    last_bad_rolls.clear()
    all_good_messages.clear()
    active_replacement_tasks.clear()


async def handle_dice(client, message):
    """Main dice handler"""
    global good_dice_count, bad_roll_occurrences, messages_to_delete, last_bad_rolls
    global active_replacement_tasks, all_good_messages

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
            all_good_messages.append(message)
            log(f"[Warning] First occurrence of bad roll {val}: counted as good.")
        else:
            log(f"[X] Duplicate bad roll {val} - sending replacements.")
            task = asyncio.create_task(send_replacement_until_good(client, message.chat.id))
            active_replacement_tasks.add(task)
            task.add_done_callback(lambda t: active_replacement_tasks.discard(t))
            await task
    else:
        good_dice_count += 1
        all_good_messages.append(message)
        log(f"[OK] Good roll: {val} -> total {good_dice_count}")

    if good_dice_count >= TARGET_GOOD_DICE:
        log(f"\n[Target] Target reached ({good_dice_count}/{TARGET_GOOD_DICE}).")
        
        if active_replacement_tasks:
            log(f"[Wait] Waiting for {len(active_replacement_tasks)} remaining replacement task(s)...")
            await asyncio.gather(*active_replacement_tasks, return_exceptions=True)
            log(f"[Wait] All replacements complete. Final count: {good_dice_count}")
        
        await perform_batch_deletion()
        reset_session()
        log("[OK] Round complete! Ready for next one.\n")


async def start_monitoring():
    """Start the Telegram client and monitor dice"""
    global app, stop_event
    
    stop_event = asyncio.Event()
    
    if SESSION_STRING:
        app = Client("tgpasa", api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING)
    else:
        app = Client("tgpasa", api_id=API_ID, api_hash=API_HASH)
    
    async with app:
        me = await app.get_me()
        log(f"[Start] Running as {me.first_name} (ID: {me.id})")
        
        # Fetch the chat to ensure it's in the session cache
        try:
            chat = await app.get_chat(TARGET_CHAT_ID)
            log(f"[Target] Connected to chat: {chat.title if chat.title else 'Private Chat'}")
        except Exception as e:
            log(f"[Error] Cannot access chat {TARGET_CHAT_ID}: {e}")
            log("[Error] Make sure the bot is a member of this chat!")
            return
        
        # Register handler AFTER confirming chat access - filter by specific chat only
        @app.on_message(filters.chat(TARGET_CHAT_ID) & filters.dice)
        async def dice_handler(client, message):
            await handle_dice(client, message)
        
        log(f"[Config] Bad rolls: {BAD_ROLLS}, Target: {TARGET_GOOD_DICE}")
        log("[Wait] Waiting for dice rolls...\n")
        await stop_event.wait()


def run_bot_in_thread():
    """Run bot in separate thread with its own event loop"""
    global bot_loop, is_running
    
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
        try:
            pending = asyncio.all_tasks(bot_loop)
            for task in pending:
                task.cancel()
            bot_loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except:
            pass
        finally:
            try:
                bot_loop.close()
            except:
                pass


def start_bot(chat_id, bad_rolls_input, target_dice):
    """Start the bot with given configuration"""
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
    """Stop the bot gracefully"""
    global is_running, stop_event, bot_loop
    
    if not is_running:
        return "[Error] Bot is not running!"
    
    log("[Stop] Stopping bot gracefully...")
    is_running = False
    
    if stop_event and bot_loop:
        try:
            bot_loop.call_soon_threadsafe(stop_event.set)
        except Exception as e:
            log(f"[Debug] Error setting stop event: {e}")
    
    import time
    time.sleep(1)
    
    log("[Stop] Bot stopped!")
    return "[Stop] Bot stopped!"


def get_logs():
    """Retrieve logs from queue"""
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
                placeholder="enter chat ID starts from -100",
                value="-1003151338912"
            )
            bad_rolls_input = gr.Textbox(
                label="Bad Rolls (comma-separated)",
                placeholder="3,4",
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
    
    stop_btn.click(fn=stop_bot, outputs=[status_output])
    
    timer = gr.Timer(value=0.5, active=True)
    timer.tick(fn=get_logs, outputs=[log_output])


if __name__ == "__main__":
    if not API_ID or not API_HASH:
        print("[Error] API_ID and API_HASH must be set in .env file!")
        exit(1)
    
    port = int(os.getenv("PORT", 7860))
    demo.launch(server_name="0.0.0.0", server_port=port, share=False)

