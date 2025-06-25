import asyncio
import logging
import sqlite3
import time
from telethon import TelegramClient, events
from telethon.tl.types import Channel
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.errors import FloodWaitError, PeerIdInvalidError, UserNotParticipantError, ChannelPrivateError, ChatAdminRequiredError

# Import configuration
from config import API_ID, API_HASH, BOT_TOKEN, ADMIN_ID, DATABASE_NAME, POLLING_INTERVAL

# Configure logging
logging.basicConfig(format='[%(levelname) 5s/%(asctime)s] %(name)s: %(message)s', level=logging.WARNING)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Initialize Telegram Client
client = TelegramClient('bot_session', API_ID, API_HASH)

# --- Enhanced Database Functions ---

def init_db():
    """Initializes the SQLite database with subscriber tracking."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY,
            title TEXT,
            access_hash INTEGER,
            link TEXT,
            subscribers INTEGER DEFAULT 0,
            last_checked REAL DEFAULT 0
        )
    ''')
    conn.commit()
    conn.close()
    logger.info("Database initialized with subscriber tracking.")

def add_channel_to_db(channel_id, title, access_hash, link=None, subscribers=0):
    """Adds a channel to the database with subscriber count."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT OR REPLACE INTO channels 
            (id, title, access_hash, link, subscribers) 
            VALUES (?, ?, ?, ?, ?)
            """, (channel_id, title, access_hash, link, subscribers))
        conn.commit()
        if cursor.rowcount > 0:
            logger.info(f"Channel '{title}' ({channel_id}) added/updated in DB with {subscribers} subscribers.")
            return True
        return False
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        return False
    finally:
        conn.close()

def remove_channel_from_db(channel_id):
    """Removes a channel from the database."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM channels WHERE id = ?", (channel_id,))
        conn.commit()
        return cursor.rowcount > 0
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        return False
    finally:
        conn.close()

def get_all_channels_from_db():
    """Retrieves all channels from the database."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT id, title, access_hash, link, subscribers, last_checked FROM channels")
    channels = cursor.fetchall()
    conn.close()
    return channels

def update_channel_subscribers(channel_id, subscribers):
    """Updates subscriber count and last checked timestamp."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE channels 
            SET subscribers = ?, last_checked = ?
            WHERE id = ?
            """, (subscribers, time.time(), channel_id))
        conn.commit()
        return cursor.rowcount > 0
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        return False
    finally:
        conn.close()

# --- Enhanced Helper Functions ---

async def get_channel_info(peer):
    """
    Fetches comprehensive channel info including subscribers.
    Handles various errors if the bot cannot access the channel.
    """
    try:
        entity = await client.get_entity(peer)
        
        if isinstance(entity, Channel):
            channel_id = entity.id
            title = entity.title
            access_hash = entity.access_hash
            link = f"https://t.me/c/{channel_id}"
            if entity.username:
                link = f"https://t.me/{entity.username}"
            
            # Get subscriber count
            subscribers = 0
            try:
                full_chat = await client(GetFullChannelRequest(entity))
                if hasattr(full_chat.full_chat, 'participants_count'):
                    subscribers = full_chat.full_chat.participants_count
            except Exception as e:
                logger.warning(f"Couldn't get subscribers for {channel_id}: {e}")
            
            return {
                'id': channel_id,
                'title': title,
                'access_hash': access_hash,
                'link': link,
                'subscribers': subscribers,
                'entity': entity
            }
        else:
            logger.warning(f"Peer {peer} is not a channel.")
            return None
    except (PeerIdInvalidError, ValueError):
        logger.error(f"Invalid Peer ID: {peer}.")
        return {'id': peer, 'title': 'Unknown (Invalid ID)', 'access_hash': None, 'link': 'N/A', 'error': 'Invalid Peer ID'}
    except UserNotParticipantError:
        logger.warning(f"Bot not in channel {peer}.")
        return {'id': peer, 'title': 'Unknown (Not Participant)', 'access_hash': None, 'link': 'N/A', 'error': 'UserNotParticipantError'}
    except ChannelPrivateError:
        logger.warning(f"Channel {peer} is private.")
        return {'id': peer, 'title': 'Unknown (Private Channel)', 'access_hash': None, 'link': 'N/A', 'error': 'ChannelPrivateError'}
    except ChatAdminRequiredError:
        logger.warning(f"Admin rights needed for {peer}.")
        return {'id': peer, 'title': 'Unknown (Admin Rights Required)', 'access_hash': None, 'link': 'N/A', 'error': 'ChatAdminRequiredError'}
    except FloodWaitError as e:
        logger.warning(f"Flood wait: {e.seconds} seconds.")
        await asyncio.sleep(e.seconds)
        return await get_channel_info(peer)  # Retry after wait
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {'id': peer, 'title': 'Unknown (Error)', 'access_hash': None, 'link': 'N/A', 'error': str(e)}

async def send_admin_notification(message):
    """Sends a notification message to the admin with flood control."""
    try:
        await client.send_message(ADMIN_ID, message)
    except FloodWaitError as e:
        logger.warning(f"Flood wait: {e.seconds} seconds.")
        await asyncio.sleep(e.seconds)
        await client.send_message(ADMIN_ID, message)
    except Exception as e:
        logger.error(f"Notification error: {e}")

# --- Event Handlers with Subscriber Tracking ---

@client.on(events.ChatAction)
async def handle_chat_action(event):
    """Handles bot being added/removed from channels with subscriber info."""
    if event.user_id == (await client.get_me()).id:
        me = await client.get_me()
        if event.user_id == me.id:
            if event.user_added or event.user_joined:
                if event.is_channel:
                    channel_info = await get_channel_info(event.chat_id)
                    if channel_info and not channel_info.get('error'):
                        if add_channel_to_db(
                            channel_info['id'],
                            channel_info['title'],
                            channel_info['access_hash'],
                            channel_info['link'],
                            channel_info['subscribers']
                        ):
                            notification_msg = (
                                f"ðŸ¤– Added to new channel!\n"
                                f"Title: {channel_info['title']}\n"
                                f"ID: {channel_info['id']}\n"
                                f"Subscribers: {channel_info['subscribers']}\n"
                                f"Link: {channel_info['link']}"
                            )
                            await send_admin_notification(notification_msg)

            elif event.user_left or event.user_kicked:
                if event.is_channel:
                    channel_id = event.chat_id
                    channels = get_all_channels_from_db()
                    channel_data = next((c for c in channels if c[0] == channel_id), None)
                    
                    if remove_channel_from_db(channel_id):
                        notification_msg = f"ðŸ¤– Removed from channel!\nID: {channel_id}"
                        if channel_data:
                            notification_msg += (
                                f"\nTitle: {channel_data[1]}\n"
                                f"Subscribers: {channel_data[4]}\n"
                                f"Link: {channel_data[3]}"
                            )
                        await send_admin_notification(notification_msg)

# --- Enhanced Admin Commands ---

@client.on(events.NewMessage(pattern='/list_channels', func=lambda e: e.is_private and e.sender_id == ADMIN_ID))
async def list_channels_command(event):
    """Lists all tracked channels with subscriber counts."""
    channels = get_all_channels_from_db()
    if not channels:
        await event.respond("No channels being tracked.")
        return

    # Sort by subscriber count descending
    channels_sorted = sorted(channels, key=lambda x: x[4], reverse=True)
    
    message = "ðŸ“Š Tracked Channels:\n\n"
    for idx, (channel_id, title, _, link, subscribers, _) in enumerate(channels_sorted):
        message += (
            f"{idx + 1}. {title}\n"
            f"   ðŸ‘¥ Subscribers: {subscribers}\n"
            f"   ðŸ”— Link: {link or 'N/A'}\n"
            f"   ðŸ†” ID: {channel_id}\n\n"
        )
    
    await event.respond(message)

@client.on(events.NewMessage(pattern='/status', func=lambda e: e.is_private and e.sender_id == ADMIN_ID))
async def status_command(event):
    """Shows bot status with subscriber statistics."""
    channels = get_all_channels_from_db()
    total_channels = len(channels)
    total_subscribers = sum(c[4] for c in channels) if channels else 0
    
    status_msg = (
        f"ðŸ¤– Bot Status:\n"
        f"âœ… Operational\n"
        f"ðŸ“Š Tracking {total_channels} channels\n"
        f"ðŸ‘¥ Total subscribers: {total_subscribers}\n"
        f"â³ Next poll in {POLLING_INTERVAL} seconds"
    )
    await event.respond(status_msg)

@client.on(events.NewMessage(pattern=r'/add (-?\d+)', func=lambda e: e.is_private and e.sender_id == ADMIN_ID))
async def add_channel_command(event):
    """Adds a channel by ID with subscriber info."""
    try:
        channel_id = int(event.pattern_match.group(1))
        await event.respond(f"Adding channel ID: `{channel_id}`...")
        
        channel_info = await get_channel_info(channel_id)
        
        if channel_info and not channel_info.get('error'):
            # Attempt to join public channels
            if channel_info.get('entity') and hasattr(channel_info['entity'], 'join'):
                try:
                    await client(channel_info['entity'].join())
                except Exception:
                    pass  # Join isn't critical for adding
            
            if add_channel_to_db(
                channel_info['id'],
                channel_info['title'],
                channel_info['access_hash'],
                channel_info['link'],
                channel_info['subscribers']
            ):
                response = (
                    f"âœ… Channel added!\n"
                    f"Title: {channel_info['title']}\n"
                    f"ID: {channel_info['id']}\n"
                    f"Subscribers: {channel_info['subscribers']}\n"
                    f"Link: {channel_info['link']}"
                )
            else:
                response = f"â„¹ï¸ Channel `{channel_id}` already tracked."
        else:
            error = channel_info.get('error', 'Unknown error') if channel_info else 'Invalid channel'
            response = f"âŒ Failed to add channel: {error}"
        
        await event.respond(response)
    except Exception as e:
        logger.error(f"Add command error: {e}")
        await event.respond(f"âŒ Error: {e}")

@client.on(events.NewMessage(pattern=r'/add_range (-?\d+) (-?\d+)', func=lambda e: e.is_private and e.sender_id == ADMIN_ID))
async def add_range_command(event):
    """Adds a range of channels with unlimited size and progress updates."""
    try:
        start_id = int(event.pattern_match.group(1))
        end_id = int(event.pattern_match.group(2))
        
        # Normalize range
        if start_id > end_id:
            start_id, end_id = end_id, start_id
            
        total_channels = end_id - start_id + 1
        CHUNK_SIZE = 100  # Process in chunks to avoid flooding
        added_count = 0
        
        progress_msg = await event.respond(
            f"â³ Processing {total_channels} channels...\n"
            f"0% completed (0/{total_channels})"
        )
        
        for i, channel_id in enumerate(range(start_id, end_id + 1)):
            # Skip admin ID if in range
            if channel_id == ADMIN_ID:
                continue
                
            try:
                channel_info = await get_channel_info(channel_id)
                if channel_info and not channel_info.get('error'):
                    if add_channel_to_db(
                        channel_info['id'],
                        channel_info['title'],
                        channel_info['access_hash'],
                        channel_info['link'],
                        channel_info['subscribers']
                    ):
                        added_count += 1
            except FloodWaitError as e:
                await asyncio.sleep(e.seconds)
            except Exception:
                pass
            
            # Update progress every 50 channels or 5%
            if i % 50 == 0 or i == total_channels - 1:
                percent = int((i + 1) / total_channels * 100)
                await progress_msg.edit(
                    f"â³ Processing {total_channels} channels...\n"
                    f"{percent}% completed ({i + 1}/{total_channels})\n"
                    f"âœ… Added: {added_count}"
                )
            
            # Brief pause between channels
            if i % 10 == 0:
                await asyncio.sleep(0.5)
        
        await event.respond(
            f"âœ… Range processing complete!\n"
            f"Total processed: {total_channels}\n"
            f"Channels added: {added_count}"
        )
    except Exception as e:
        logger.error(f"Range error: {e}")
        await event.respond(f"âŒ Error: {e}")

# --- Optimized Polling Mechanism ---

async def poll_channels():
    """Efficient channel monitoring with subscriber tracking."""
    while True:
        logger.info("Starting optimized polling cycle...")
        
        try:
            # Get current channels from Telegram
            dialogs = await client.get_dialogs()
            current_channels = {d.entity.id for d in dialogs if isinstance(d.entity, Channel)}
            
            # Get database channels
            db_channels = get_all_channels_from_db()
            db_ids = {c[0] for c in db_channels}
            db_map = {c[0]: c for c in db_channels}
            
            # Find new and removed channels
            new_channels = current_channels - db_ids
            removed_channels = db_ids - current_channels
            
            # Process new channels
            for channel_id in new_channels:
                entity = next((d.entity for d in dialogs if d.entity.id == channel_id), None)
                if entity:
                    channel_info = await get_channel_info(entity)
                    if channel_info and not channel_info.get('error'):
                        add_channel_to_db(
                            channel_info['id'],
                            channel_info['title'],
                            channel_info['access_hash'],
                            channel_info['link'],
                            channel_info['subscribers']
                        )
                        await send_admin_notification(
                            f"ðŸ¤– New channel detected!\n"
                            f"Title: {channel_info['title']}\n"
                            f"Subscribers: {channel_info['subscribers']}"
                        )
            
            # Process removed channels
            for channel_id in removed_channels:
                channel_data = db_map.get(channel_id)
                if channel_data and remove_channel_from_db(channel_id):
                    await send_admin_notification(
                        f"ðŸ¤– Removed from channel!\n"
                        f"Title: {channel_data[1]}\n"
                        f"Last subscribers: {channel_data[4]}"
                    )
            
            # Update subscriber counts (staggered)
            db_channels = get_all_channels_from_db()  # Refresh data
            channels_to_update = [c for c in db_channels if time.time() - c[5] > 86400]  # 24h cooldown
            
            logger.info(f"Updating subscribers for {len(channels_to_update)} channels")
            
            for idx, channel in enumerate(channels_to_update):
                try:
                    channel_info = await get_channel_info(channel[0])
                    if channel_info and not channel_info.get('error'):
                        # Update if subscriber count changed significantly
                        if abs(channel_info['subscribers'] - channel[4]) > 50:
                            update_channel_subscribers(channel[0], channel_info['subscribers'])
                            
                            # Notify for significant changes
                            if abs(channel_info['subscribers'] - channel[4]) > 100:
                                await send_admin_notification(
                                    f"ðŸ‘¥ Subscriber change!\n"
                                    f"{channel[1]}\n"
                                    f"Old: {channel[4]} â†’ New: {channel_info['subscribers']}\n"
                                    f"Î”: {channel_info['subscribers'] - channel[4]}"
                                )
                        
                        # Pause every 10 channels to prevent flooding
                        if idx % 10 == 0:
                            await asyncio.sleep(2)
                except FloodWaitError as e:
                    await asyncio.sleep(e.seconds)
                except Exception as e:
                    logger.error(f"Subscriber update error: {e}")
        
        except FloodWaitError as e:
            logger.warning(f"Flood wait: {e.seconds} seconds.")
            await asyncio.sleep(e.seconds)
        except Exception as e:
            logger.error(f"Polling error: {e}")
        
        logger.info(f"Polling completed. Next in {POLLING_INTERVAL}s")
        await asyncio.sleep(POLLING_INTERVAL)

# --- Main Execution ---

async def main():
    init_db()
    
    await client.start(bot_token=BOT_TOKEN)
    logger.info("Bot started. Listening for events...")

    # Create polling task
    asyncio.create_task(poll_channels())

    # Notify admin
    me = await client.get_me()
    await send_admin_notification(f"ðŸ¤– Bot is online!\nUsername: @{me.username}\nID: {me.id}")

    await client.run_until_disconnected()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
