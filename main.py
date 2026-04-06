import asyncio
import sys
import os
import queue
import logging
import warnings
from threading import Thread, Lock
from collections import defaultdict
from pyrogram import Client, filters
from dotenv import load_dotenv
from flask import Flask, render_template, request, jsonify, session, Response, redirect, url_for
from functools import wraps
import time

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
accepted_bad_at_max_retries = 0
is_running = False
is_cleaning = False
log_queue = queue.Queue()
bot_thread = None
bot_loop = None
stop_event = None

# Enhanced log management
log_lock = Lock()
error_logs = []  # Keep error logs separate
dice_logs = []   # Clear after each round
MAX_ERROR_LOGS = 100
MAX_DICE_LOGS = 50  # Reduced since they're cleared after rounds

# Config from UI
TARGET_CHAT_ID = None
BAD_ROLLS = set()
TARGET_GOOD_DICE = 6

# Flask app
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", os.urandom(24).hex())


def log(message, is_error=False):
    """Add message to appropriate log queue with memory management"""
    print(message)
    
    with log_lock:
        if is_error or "Error" in message or "error" in message.lower():
            error_logs.append(f"[{time.strftime('%H:%M:%S')}] {message}")
            # Keep only last MAX_ERROR_LOGS
            if len(error_logs) > MAX_ERROR_LOGS:
                error_logs.pop(0)
        else:
            dice_logs.append(f"[{time.strftime('%H:%M:%S')}] {message}")
            # Keep only last MAX_DICE_LOGS
            if len(dice_logs) > MAX_DICE_LOGS:
                dice_logs.pop(0)
    
    # Also put in queue for SSE
    log_queue.put(message)


def clear_dice_logs():
    """Clear dice logs after successful round"""
    with log_lock:
        dice_logs.clear()


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
        await top_up_missing_dice()
        reset_session()
        clear_dice_logs()  # Clear dice logs after successful round
        is_cleaning = False
        log("Ready for next round\n")


async def send_replacement_until_good(client, chat_id):
    """Keep rolling until a good roll appears (max 2 attempts)"""
    global good_dice_count, bad_roll_occurrences, messages_to_delete, last_bad_rolls, all_good_messages
    global accepted_bad_at_max_retries

    max_retries = 2
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
                
                if attempt == max_retries:
                    accepted_bad_at_max_retries += 1
                    good_dice_count += 1
                    all_good_messages.append(new_msg)
                    log(f"Max retries reached (attempt {attempt}), accepting bad roll {val} ({good_dice_count}/{TARGET_GOOD_DICE})")
                    return new_msg
                
                continue

            good_dice_count += 1
            all_good_messages.append(new_msg)
            log(f"Replacement {val} → good ({good_dice_count}/{TARGET_GOOD_DICE})")
            return new_msg
        
        if last_msg:
            good_dice_count += 1
            all_good_messages.append(last_msg)
            log(f"Accepting last roll ({good_dice_count}/{TARGET_GOOD_DICE})")
        return last_msg
        
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log(f"Error in replacement: {e}", is_error=True)
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
        log(f"Deletion error: {e}", is_error=True)


def reset_session():
    """Reset all tracking variables and cancel active tasks"""
    global good_dice_count, bad_roll_occurrences, messages_to_delete, last_bad_rolls
    global active_replacement_tasks, all_good_messages, is_cleaning, accepted_bad_at_max_retries
    
    # Cancel active tasks
    for task in list(active_replacement_tasks):
        if not task.done():
            task.cancel()
    
    # Clear all collections to free memory
    good_dice_count = 0
    bad_roll_occurrences.clear()
    messages_to_delete.clear()
    last_bad_rolls.clear()
    all_good_messages.clear()
    active_replacement_tasks.clear()
    accepted_bad_at_max_retries = 0
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
        await asyncio.gather(*active_replacement_tasks, return_exceptions=True)
    
    await check_and_cleanup()


async def start_monitoring():
    """Initialize and start both clients"""
    global user_client, bot_client, stop_event
    
    stop_event = asyncio.Event()
    
    try:
        user_client = Client(
            "user_session",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=SESSION_STRING,
            in_memory=True
        )
        
        bot_client = Client(
            "bot_session",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            in_memory=True
        )
        
        @user_client.on_message(filters.dice)
        async def user_dice_handler(client, message):
            await handle_dice(client, message)
        
        log("Starting clients...")
        await user_client.start()
        await bot_client.start()
        
        # Get client information
        user_me = await user_client.get_me()
        bot_me = await bot_client.get_me()
        
        log(f"User: {user_me.first_name} | Bot: @{bot_me.username}")
        log(f"Monitoring chat: {TARGET_CHAT_ID}")
        log(f"Bad rolls: {BAD_ROLLS} | Target: {TARGET_GOOD_DICE}\n")
        
        await stop_event.wait()
        
    except Exception as e:
        log(f"Client error: {e}", is_error=True)
    finally:
        log("Shutting down clients...")
        try:
            if user_client:
                await user_client.stop()
            if bot_client:
                await bot_client.stop()
        except:
            pass


def run_bot_in_thread():
    """Run the bot in a separate thread with its own event loop"""
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
        log(f"Bot thread error: {e}", is_error=True)
    finally:
        is_running = False
        # Cleanup
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


# Flask routes and authentication

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('authenticated'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function


@app.route('/')
def login():
    """Login page"""
    if session.get('authenticated'):
        return redirect(url_for('index'))
    return render_template('login.html')


@app.route('/auth', methods=['POST'])
def authenticate():
    """Handle login"""
    password = request.form.get('password', '')
    if not ACCESS_PASSWORD or password == ACCESS_PASSWORD:
        session['authenticated'] = True
        return redirect(url_for('index'))
    return render_template('login.html', error='Invalid password')


@app.route('/logout')
def logout():
    """Logout"""
    session.clear()
    return redirect(url_for('login'))


@app.route('/dashboard')
@login_required
def index():
    """Main dashboard"""
    return render_template('dashboard.html')


@app.route('/start', methods=['POST'])
@login_required
def start_bot_route():
    """Start the bot with given configuration"""
    global TARGET_CHAT_ID, BAD_ROLLS, TARGET_GOOD_DICE, is_running, bot_thread
    
    if is_running:
        return jsonify({'status': 'error', 'message': 'Bot already running!'})
    
    try:
        data = request.json
        TARGET_CHAT_ID = int(data['chat_id'])
        bad_rolls_list = [int(x.strip()) for x in data['bad_rolls'].split(",")]
        BAD_ROLLS = set(bad_rolls_list)
        TARGET_GOOD_DICE = int(data['target_dice'])
        
        if not all(1 <= x <= 6 for x in BAD_ROLLS):
            return jsonify({'status': 'error', 'message': 'Bad rolls must be 1-6!'})
        
        if len(BAD_ROLLS) >= 6:
            return jsonify({'status': 'error', 'message': 'Must have at least one good roll!'})
        
        # Clear logs before starting
        with log_lock:
            dice_logs.clear()
            error_logs.clear()
        
        if bot_thread and bot_thread.is_alive():
            bot_thread.join(timeout=5)

        is_running = True
        bot_thread = Thread(target=run_bot_in_thread, daemon=True)
        bot_thread.start()
        
        return jsonify({'status': 'success', 'message': 'Bot started!'})
        
    except ValueError as e:
        is_running = False
        return jsonify({'status': 'error', 'message': f'Invalid input: {e}'})
    except Exception as e:
        is_running = False
        return jsonify({'status': 'error', 'message': f'Error: {e}'})


@app.route('/stop', methods=['POST'])
@login_required
def stop_bot_route():
    """Stop the bot gracefully"""
    global is_running, stop_event, bot_loop
    
    if not is_running:
        return jsonify({'status': 'error', 'message': 'Bot not running!'})
    
    log("Stopping...")
    is_running = False
    
    reset_session()
    
    if stop_event and bot_loop:
        try:
            bot_loop.call_soon_threadsafe(stop_event.set)
        except:
            pass
    
    time.sleep(1)
    
    # Clear all logs on stop
    with log_lock:
        error_logs.clear()
        dice_logs.clear()
    
    log("Bot stopped!")
    return jsonify({'status': 'success', 'message': 'Bot stopped!'})


@app.route('/status')
@login_required
def get_status():
    """Get current bot status"""
    return jsonify({
        'running': is_running,
        'good_dice': good_dice_count,
        'target': TARGET_GOOD_DICE
    })


@app.route('/stream')
@login_required
def stream():
    """Server-Sent Events endpoint for live logs"""
    def generate():
        while True:
            try:
                # Get log from queue with timeout
                message = log_queue.get(timeout=1)
                yield f"data: {message}\n\n"
            except queue.Empty:
                # Send heartbeat to keep connection alive
                yield f": heartbeat\n\n"
            except GeneratorExit:
                break
    
    return Response(generate(), mimetype='text/event-stream')


@app.route('/get_logs')
@login_required
def get_logs():
    """Get all current logs (for initial load)"""
    with log_lock:
        all_logs = error_logs + dice_logs
    return jsonify({'logs': all_logs})


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
    
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    
    print(f"[Server] Starting on port {port}...")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
