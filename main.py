import asyncio
import os
import sys
from pyrogram import Client, filters
from pyrogram.raw.functions.channels import DeleteMessages
from collections import defaultdict
import uvloop
import gradio as gr

# --- CONFIGURATION (Reads sensitive data from environment variables) ---
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
STRING_SESSION = os.environ.get("STRING_SESSION", None) # The Pyrogram String Session

# Variables to be set via Gradio
global TARGET_CHAT_ID, BAD_ROLLS, TARGET_GOOD_DICE
TARGET_CHAT_ID = None
BAD_ROLLS = set()
TARGET_GOOD_DICE = 6 # This value remains fixed as in the original script

# --- GLOBAL STATE ---
app = None
client_running = asyncio.Event()
good_dice_count = 0
bad_roll_occurrences = defaultdict(int)
messages_to_delete = []
last_bad_rolls = {}

def reset_session():
    """Resets all game state variables."""
    global good_dice_count, bad_roll_occurrences, messages_to_delete, last_bad_rolls
    good_dice_count = 0
    bad_roll_occurrences.clear()
    messages_to_delete.clear()
    last_bad_rolls.clear()
    print("\n[SESSION] Game session state reset.")


# --- CORE LOGIC FUNCTIONS (Preserved from original d.py) ---

async def perform_batch_deletion():
    """Deletes all bad messages except the latest occurrence per bad value."""
    if not TARGET_CHAT_ID or not app or not client_running.is_set():
        return "ERROR: Bot is not fully initialized or running."
        
    try:
        if not messages_to_delete:
            return "🧹 No bad messages queued."

        # Determine which messages to keep (the last bad roll of each value)
        keep_ids = {msg.id for msg in last_bad_rolls.values() if msg}
        all_ids = {msg.id for msg in messages_to_delete if msg}
        delete_ids = list(all_ids - keep_ids)

        if delete_ids:
            print(f"[CLEANUP] Deleting {len(delete_ids)} messages, keeping {len(keep_ids)} latest bad rolls...")
            
            # Resolve peer is necessary for DeleteMessages to work on supergroups/channels
            # We use app.resolve_peer directly on the client instance
            peer = await app.resolve_peer(TARGET_CHAT_ID)
            await app.invoke(DeleteMessages(channel=peer, id=delete_ids))
            return f"✅ Deleted {len(delete_ids)} messages successfully."
        else:
            return "🧹 Nothing to delete after filtering."
    except Exception as e:
        return f"❌ Deletion error: {e}"

async def send_replacement_until_good(client, chat_id):
    """
    Replacement loop: keep rolling until a good roll appears.
    A bad replacement roll always triggers a retry. (Logic preserved)
    """
    global good_dice_count, bad_roll_occurrences, messages_to_delete, last_bad_rolls

    while client_running.is_set():
        new_msg = await client.send_dice(chat_id)
        val = new_msg.dice.value
        print(f" ↻ Replacement rolled {val}")

        if val in BAD_ROLLS:
            bad_roll_occurrences[val] += 1
            messages_to_delete.append(new_msg)
            last_bad_rolls[val] = new_msg
            
            # Always retry on a bad replacement roll
            print(f" ❌ Bad replacement {val}, retrying...")
            await asyncio.sleep(0.3)
            continue

        # Good replacement
        good_dice_count += 1
        print(f" ✓ Replacement accepted good {val} → total {good_dice_count}")
        return new_msg
    return None

# --- DICE HANDLER (Preserved from original d.py) ---
@Client.on_message(filters.dice)
async def handle_dice(client, message):
    global good_dice_count, bad_roll_occurrences, messages_to_delete, last_bad_rolls

    # Check if the message is from the configured chat AND the bot is active
    if message.chat.id != TARGET_CHAT_ID or not client_running.is_set():
        return
        
    val = message.dice.value
    sender_name = message.from_user.first_name if message.from_user else "Unknown User"
    
    print(f"🎲 {sender_name} rolled: {val} ({good_dice_count + 1}/{TARGET_GOOD_DICE})")

    if val in BAD_ROLLS:
        bad_roll_occurrences[val] += 1
        messages_to_delete.append(message)
        last_bad_rolls[val] = message
        occurrence = bad_roll_occurrences[val]

        if occurrence == 1:
            # First occurrence of original roll: count as good
            good_dice_count += 1
            print(f"⚠ First occurrence of bad roll {val}: counted as good.")
        else:
            # Duplicate: send replacement
            print(f"❌ Duplicate bad roll {val} — sending replacements.")
            await send_replacement_until_good(client, message.chat.id)
    else:
        # Good roll
        good_dice_count += 1
        print(f"✓ Good roll: {val} → total {good_dice_count}")

    # Cleanup after reaching target
    if good_dice_count >= TARGET_GOOD_DICE:
        print(f"\n🎯 Target reached ({good_dice_count}/{TARGET_GOOD_DICE}). Cleaning up...")
        result = await perform_batch_deletion()
        print(result)
        reset_session()
        print("✅ Round complete! Ready for next one.\n")


# --- BOT LIFECYCLE MANAGEMENT FOR GRADIO ---

async def start_client_task():
    """Initializes and runs the Pyrogram client non-blockingly using STRING_SESSION."""
    global app, TARGET_CHAT_ID
    
    if API_ID == 0 or not API_HASH:
        return f"ERROR: API_ID or API_HASH not set as environment variables."
    if not STRING_SESSION:
        return f"ERROR: STRING_SESSION is required and not set as an environment variable."
        
    print(f"\n[CLIENT] Initializing Pyrogram client using String Session...")
    # Initialize client using the string session directly. The session file 'tgpasa.session' will not be created.
    app = Client(STRING_SESSION, api_id=API_ID, api_hash=API_HASH)

    try:
        await app.start()
        client_running.set()
        me = await app.get_me()
        print(f"⚡ Running as {me.first_name} (ID: {me.id})")
        print(f"🎯 Target chat: {TARGET_CHAT_ID}")
        print(f"❌ Bad rolls: {list(BAD_ROLLS)}")
        print("⏳ Bot is monitoring chat. Look for log activity below.\n")

        # Keep the task alive until manually stopped
        await asyncio.Future()

    except asyncio.CancelledError:
        print("[CLIENT] Pyrogram client task cancelled.")
    except Exception as e:
        print(f"❌ [CLIENT] Failed to start client: {e}")
        client_running.clear()
        return f"❌ Failed to start client. Error: {e}"
    finally:
        if app and app.is_connected:
            await app.stop()
            print("[CLIENT] Pyrogram client stopped.")
        client_running.clear()
    
    return "✅ Bot Stopped."

async def start_bot(chat_id_str, bad_rolls_str):
    """Gradio handler to start the bot."""
    global TARGET_CHAT_ID, BAD_ROLLS

    if client_running.is_set():
        return "⚠️ Bot is already running!"
    if not STRING_SESSION:
        return "❌ Cannot start: STRING_SESSION environment variable is missing."

    try:
        # 1. Parse and set TARGET_CHAT_ID
        TARGET_CHAT_ID = int(chat_id_str.strip())
        
        # 2. Parse and set BAD_ROLLS (comma-separated integers)
        BAD_ROLLS = set(int(r.strip()) for r in bad_rolls_str.split(',') if r.strip().isdigit())
        
        if not BAD_ROLLS:
            return "❌ Error: Please enter valid, comma-separated bad rolls (e.g., 3,4,6)."
        if not TARGET_CHAT_ID:
            return "❌ Error: Target Chat ID is required."


        reset_session()
        
        # 3. Start the Pyrogram client in a background task
        asyncio.create_task(start_client_task())
        
        await asyncio.sleep(2) # Give it time to initialize

        if client_running.is_set():
             return f"✅ Bot started! Monitoring Chat ID: {TARGET_CHAT_ID}. Bad Rolls: {list(BAD_ROLLS)}."
        else:
             # This means start_client_task returned an error
             return "❌ Bot failed to start (check console logs for details on API key/session errors)."

    except ValueError:
        return "❌ Error: Chat ID must be an integer (including negative sign for supergroups), and Bad Rolls must be comma-separated numbers."
    except Exception as e:
        return f"❌ An unexpected error occurred during startup: {e}"

async def stop_bot():
    """Gradio handler to stop the bot."""
    global app
    if not client_running.is_set():
        return "⚠️ Bot is not running."
    
    try:
        # Cancel the task which is blocked on asyncio.Future()
        if app and hasattr(app, 'stop'):
            # The client task is waiting on asyncio.Future(), we need to manually cancel it 
            # to let the finally block execute app.stop() and clean up.
            
            # Find the running client task and cancel it.
            tasks = [t for t in asyncio.all_tasks() if t.get_name() == 'Task-Pyrogram-Client']
            for task in tasks:
                 task.cancel()
                 
            # Wait a moment for cleanup
            await asyncio.sleep(1)
            
        client_running.clear()
        return "🛑 Bot successfully stopped."
    except Exception as e:
        return f"❌ Error stopping bot: {e}"


# --- UI DEFINITION ---
def create_gradio_ui():
    """Defines and launches the Gradio Interface."""
    with gr.Blocks(title="Pyrogram Dice Manager Bot") as demo:
        gr.Markdown("# Pyrogram Dice Manager Bot (Render Ready)")
        gr.Markdown(
            "Set your **API_ID**, **API_HASH**, and **STRING_SESSION** as environment variables on Render. "
            "Then configure the target chat and bad rolls below to start monitoring."
        )

        with gr.Row():
            chat_id_input = gr.Textbox(
                label="Target Chat ID (Must include '-' for supergroups)", 
                placeholder="-1001234567890",
                value="",
                interactive=True
            )
            bad_rolls_input = gr.Textbox(
                label="Bad Dice Rolls (1-6, comma-separated)", 
                placeholder="3, 4, 6",
                value="3, 4, 6",
                interactive=True
            )

        output_message = gr.Textbox(label="Status", value="Ready to start.", interactive=False)

        with gr.Row():
            start_btn = gr.Button("🚀 Start Bot", variant="primary")
            stop_btn = gr.Button("🛑 Stop Bot", variant="secondary")

        gr.Markdown("---")
        gr.Markdown("### Bot Activity Log")
        
        # Connect buttons to functions
        start_btn.click(
            start_bot,
            inputs=[chat_id_input, bad_rolls_input],
            outputs=[output_message]
        )
        stop_btn.click(
            stop_bot,
            inputs=[],
            outputs=[output_message]
        )
        
    return demo

# --- ENTRY POINT ---
if __name__ == "__main__":
    # 1. Install uvloop for faster async operations
    if sys.platform != 'win32':
        try:
            uvloop.install()
            print("[PERFORMANCE] uvloop installed successfully.")
        except Exception as e:
            # Fallback if uvloop is not available/installable
            print(f"[PERFORMANCE] uvloop not used: {e}") 

    # 2. Run the Gradio UI
    ui = create_gradio_ui()
    # Use 0.0.0.0 and the PORT environment variable for Render deployment
    ui.launch(server_name="0.0.0.0", server_port=int(os.environ.get("PORT", 7860)))
