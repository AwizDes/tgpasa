import asyncio
import sys
import os
import queue
import logging
import warnings
from threading import Thread
from collections import defaultdict
from pyrogram import Client, filters
from dotenv import load_dotenv
import gradio as gr

# Suppress Pyrogram's peer resolution errors and asyncio warnings
logging.getLogger("pyrogram").setLevel(logging.CRITICAL)
logging.getLogger("pyrogram.client").setLevel(logging.CRITICAL)
warnings.filterwarnings("ignore")

# Suppress "Task exception was never retrieved" errors
def custom_exception_handler(loop, context):
    exception = context.get('exception')
    if exception and 'Peer id invalid' in str(exception):
        return
    loop.default_exception_handler(context)

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
BOT_TOKEN = os.getenv("BOT_TOKEN")
ACCESS_PASSWORD = os.getenv("ACCESS_PASSWORD", "")

if not SESSION_STRING and os.path.exists("session_string.txt"):
    with open("session_string.txt", "r") as f:
        SESSION_STRING = f.read().strip()

# Global state
user_client = None
bot_client = None
good_dice_count = 0
bad_roll_occurrences = defaultdict(int)
messages_to_delete = []
last_bad_rolls = {}
all_good_messages = []
active_replacement_tasks = set()
accepted_bad_at_max_retries = 0  # NEW: Counter for top-up
is_running = False
is_cleaning = False
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


async def top_up_missing_dice():
    """Send additional dice to compensate for accepted bad rolls at max retries"""
    global accepted_bad_at_max_retries
    
    if accepted_bad_at_max_retries > 0:
        log(f"Sending {accepted_bad_at_max_retries} top-up dice...")
        for i in range(accepted_bad_at_max_retries):
            await user_client.send_dice(TARGET_CHAT_ID)
        log(f"Top-up complete")
        accepted_bad_at_max_retries = 0


async def check_and_cleanup():
    """Check if target reached and perform cleanup (with race condition protection)"""
    global good_dice_count, is_cleaning
    
    if good_dice_count >= TARGET_GOOD_DICE and not is_cleaning:
        is_cleaning = True
        log(f"Target reached! Cleaning up...")
        await perform_batch_deletion()
        await top_up_missing_dice()  # NEW: Top-up after deletion
        reset_session()
        is_cleaning = False
        log("Ready for next round\n")


async def send_replacement_until_good(client, chat_id):
    """Keep rolling until a good roll appears (max 2 attempts)"""
    global good_dice_count, bad_roll_occurrences, messages_to_delete, last_bad_rolls, all_good_messages
    global accepted_bad_at_max_retries  # NEW

    max_retries = 2  # Changed from 50 to 2
    attempt = 0
    last_msg = None
    
    try:
        while attempt < max_retries:
            attempt += 1
            new_msg = await client.send_dice(chat_id)
            val = new_msg.dice.value
            last_msg = new_msg

            if val in BAD_ROLLS:
                occurrence_before = bad_roll_occurrences[val]
                
                if occurrence_before == 0:
                    bad_roll_occurrences[val] += 1
                    messages_to_delete.append(new_msg)
                    last_bad_rolls[val] = new_msg
                    good_dice_count += 1
                    all_good_messages.append(new_msg)
                    log(f"First bad {val} → counted as good ({good_dice_count}/{TARGET_GOOD_DICE})")
                    return new_msg
                
                bad_roll_occurrences[val] += 1
                messages_to_delete.append(new_msg)
                last_bad_rolls[val] = new_msg
                
                # If this is the last attempt, accept it and move on
                if attempt == max_retries:
                    accepted_bad_at_max_retries += 1  # NEW: Increment counter
                    good_dice_count += 1
                    all_good_messages.append(new_msg)
                    log(f"Max retries reached (attempt {attempt}), accepting bad roll {val} ({good_dice_count}/{TARGET_GOOD_DICE})")
                    return new_msg
                
                continue

            good_dice_count += 1
            all_good_messages.append(new_msg)
            log(f"Replacement {val} → good ({good_dice_count}/{TARGET_GOOD_DICE})")
            return new_msg
        
        # Fallback (shouldn't reach here, but just in case)
        if last_msg:
            good_dice_count += 1
            all_good_messages.append(last_msg)
            log(f"Accepting last roll ({good_dice_count}/{TARGET_GOOD_DICE})")
        return last_msg
        
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log(f"Error: {e}")
        return None


async def perform_batch_deletion():
    """Delete bad messages (except latest of each) and excess good messages using bot client"""
    try:
        keep_bad_ids = {msg.id for msg in last_bad_rolls.values() if msg}
        all_bad_ids = {msg.id for msg in messages_to_delete if msg}
        delete_bad_ids = list(all_bad_ids - keep_bad_ids)
        
        delete_excess_good_ids = []
        if len(all_good_messages) > TARGET_GOOD_DICE:
            excess_count = len(all_good_messages) - TARGET_GOOD_DICE
            excess_messages = all_good_messages[:excess_count]
            delete_excess_good_ids = [msg.id for msg in excess_messages if msg]
        
        all_delete_ids = delete_bad_ids + delete_excess_good_ids
        
        if all_delete_ids:
            await bot_client.delete_messages(TARGET_CHAT_ID, all_delete_ids)
            log(f"Deleted {len(all_delete_ids)} messages. Kept {TARGET_GOOD_DICE} good dice.")
        else:
            log(f"No deletion needed. Exactly {TARGET_GOOD_DICE} good dice.")
    except Exception as e:
        log(f"Deletion error: {e}")


def reset_session():
    """Reset all tracking variables and cancel active tasks"""
    global good_dice_count, bad_roll_occurrences, messages_to_delete, last_bad_rolls
    global active_replacement_tasks, all_good_messages, is_cleaning, accepted_bad_at_max_retries
    
    for task in list(active_replacement_tasks):
        if not task.done():
            task.cancel()
    
    good_dice_count = 0
    bad_roll_occurrences.clear()
    messages_to_delete.clear()
    last_bad_rolls.clear()
    all_good_messages.clear()
    active_replacement_tasks.clear()
    accepted_bad_at_max_retries = 0  # NEW: Reset counter
    is_cleaning = False


async def handle_dice(client, message):
    """Main dice handler"""
    global good_dice_count, bad_roll_occurrences, messages_to_delete, last_bad_rolls
    global active_replacement_tasks, all_good_messages

    if message.chat.id != TARGET_CHAT_ID:
        return

    val = message.dice.value
    sender = message.from_user.first_name if message.from_user else "Unknown"

    if val in BAD_ROLLS:
        bad_roll_occurrences[val] += 1
        messages_to_delete.append(message)
        last_bad_rolls[val] = message
        occurrence = bad_roll_occurrences[val]

        if occurrence == 1:
            good_dice_count += 1
            all_good_messages.append(message)
            log(f"{sender} rolled {val} - First bad → counted as good ({good_dice_count}/{TARGET_GOOD_DICE})")
        else:
            log(f"{sender} rolled {val} - Duplicate bad → replacing")
            task = asyncio.create_task(send_replacement_until_good(user_client, message.chat.id))
            active_replacement_tasks.add(task)
            task.add_done_callback(lambda t: active_replacement_tasks.discard(t))
    else:
        good_dice_count += 1
        all_good_messages.append(message)
        log(f"{sender} rolled {val} ({good_dice_count}/{TARGET_GOOD_DICE})")
    
    # Wait for all replacement tasks to complete before checking
    if active_replacement_tasks:
        await asyncio.gather(*list(active_replacement_tasks), return_exceptions=True)
    
    # Single check point after all dice processing
    await check_and_cleanup()


async def start_monitoring():
    """Start both Telegram clients and monitor dice"""
    global user_client, bot_client, stop_event
    
    stop_event = asyncio.Event()
    
    if SESSION_STRING:
        user_client = Client("user_session", api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING)
    else:
        user_client = Client("user_session", api_id=API_ID, api_hash=API_HASH)
    
    bot_client = Client("bot_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
    
    @user_client.on_message(filters.dice)
    async def dice_handler(client, message):
        if message.chat.id == TARGET_CHAT_ID:
            await handle_dice(client, message)
    
    async with user_client, bot_client:
        user_me = await user_client.get_me()
        bot_me = await bot_client.get_me()
        log(f"User: {user_me.first_name} | Bot: @{bot_me.username}")
        log(f"Monitoring chat: {TARGET_CHAT_ID}")
        log(f"Bad rolls: {BAD_ROLLS} | Target: {TARGET_GOOD_DICE}\n")
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
    
    bot_loop.set_exception_handler(custom_exception_handler)
    
    asyncio.set_event_loop(bot_loop)
    try:
        bot_loop.run_until_complete(start_monitoring())
    except Exception as e:
        log(f"Error: {e}")
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
        return "Bot already running!", ""
    
    try:
        accumulated_logs.clear()
        
        TARGET_CHAT_ID = int(chat_id)
        bad_rolls_list = [int(x.strip()) for x in bad_rolls_input.split(",")]
        BAD_ROLLS = set(bad_rolls_list)
        TARGET_GOOD_DICE = int(target_dice)
        
        if not all(1 <= x <= 6 for x in BAD_ROLLS):
            return "Bad rolls must be 1-6!", ""
        
        if len(BAD_ROLLS) >= 6:
            return "Must have at least one good roll!", ""
        
        is_running = True
        bot_thread = Thread(target=run_bot_in_thread, daemon=True)
        bot_thread.start()
        
        return "Bot started!", "Initializing..."
        
    except ValueError as e:
        is_running = False
        return f"Invalid input: {e}", ""
    except Exception as e:
        is_running = False
        return f"Error: {e}", ""


def stop_bot():
    """Stop the bot gracefully"""
    global is_running, stop_event, bot_loop, accumulated_logs
    
    if not is_running:
        return "Bot not running!"
    
    log("Stopping...")
    is_running = False
    
    reset_session()
    accumulated_logs.clear()
    
    if stop_event and bot_loop:
        try:
            bot_loop.call_soon_threadsafe(stop_event.set)
        except:
            pass
    
    import time
    time.sleep(1)
    
    log("Bot stopped!")
    return "Bot stopped!"


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


def verify_password(password):
    """Verify the entered password"""
    if not ACCESS_PASSWORD:
        return True
    return password == ACCESS_PASSWORD


def create_main_interface():
    """Create the main bot control interface"""
    with gr.Column():
        gr.Markdown("# Telegram Dice Controller")
        gr.Markdown("**User Bot:** Sends dice | **Bot:** Deletes messages")
        
        with gr.Row():
            with gr.Column():
                chat_id_input = gr.Textbox(
                    label="Target Chat ID",
                    placeholder="enter group Id start from -100",
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


def create_login_interface():
    """Create the password login interface"""
    with gr.Column():
        gr.Markdown("# 🔐 Access Required")
        gr.Markdown("Please enter the password to access the Telegram Dice Controller")
        
        password_input = gr.Textbox(
            label="Password",
            type="password",
            placeholder="Enter password"
        )
        login_btn = gr.Button("Login", variant="primary")
        error_msg = gr.Markdown("", visible=False)
        
        return password_input, login_btn, error_msg


# Gradio UI with authentication
with gr.Blocks(title="Telegram Dice") as demo:
    authenticated = gr.State(False)
    
    with gr.Group(visible=True) as login_group:
        password_input, login_btn, error_msg = create_login_interface()
    
    with gr.Group(visible=False) as main_group:
        create_main_interface()
    
    def login(password, auth_state):
        """Handle login attempt"""
        if verify_password(password):
            return {
                authenticated: True,
                login_group: gr.update(visible=False),
                main_group: gr.update(visible=True),
                error_msg: gr.update(visible=False)
            }
        else:
            return {
                authenticated: False,
                login_group: gr.update(visible=True),
                main_group: gr.update(visible=False),
                error_msg: gr.update("❌ Invalid password. Please try again.", visible=True)
            }
    
    login_btn.click(
        fn=login,
        inputs=[password_input, authenticated],
        outputs=[authenticated, login_group, main_group, error_msg]
    )
    
    password_input.submit(
        fn=login,
        inputs=[password_input, authenticated],
        outputs=[authenticated, login_group, main_group, error_msg]
    )


if __name__ == "__main__":
    if not API_ID or not API_HASH:
        print("[Error] API_ID and API_HASH must be set in .env file!")
        exit(1)
    
    if not BOT_TOKEN:
        print("[Error] BOT_TOKEN must be set in .env file!")
        exit(1)
    
    if not ACCESS_PASSWORD:
        print("[Warning] No ACCESS_PASSWORD set in .env file. Access will be unrestricted!")
    else:
        print("[Security] Password authentication enabled.")
    
    port = int(os.getenv("PORT", 7860))
    demo.launch(server_name="0.0.0.0", server_port=port, share=False)
