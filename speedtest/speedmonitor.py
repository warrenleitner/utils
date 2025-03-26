import os
import time
import subprocess
import csv
import threading
import datetime
import logging
import argparse

import speedtest  # pip install speedtest-cli

# Telegram imports
from telegram import Update, Bot
from telegram.ext import Updater, CommandHandler, CallbackContext

# ---------------------------
# CONFIGURATION & THRESHOLDS
# ---------------------------
PING_TARGET = "8.8.8.8"  # Can be overridden via command line
# Thresholds: adjust these as needed or via command line.
PING_THRESHOLD_MS = 100.0     # If average ping (ms) exceeds this, trigger investigation
SPEED_THRESHOLD_MBPS = 100.0  # If download speed (Mbps) is below this, trigger investigation

# File names for logs (will be combined with log_dir)
PING_CSV = "ping.csv"
SPEED_CSV = "speed.csv"
INVESTIGATIONS_CSV = "investigations.csv"
TRACEROUTE_CSV = "traceroute.csv"
DNSLOOKUP_CSV = "dnslookup.csv"

# Global state variables for investigation status
investigation_active = False
investigation_start = None
last_investigation_end = None
investigation_lock = threading.Lock()

# Telegram globals (to be set in main())
telegram_bot = None
TELEGRAM_CHAT_ID = None  # Should be a string representing your chat ID
telegram_enabled = True  # Flag to track if Telegram is enabled
log_dir = None  # Will be set in main()

# ---------------------------
# LOG DIRECTORY HANDLING
# ---------------------------
def ensure_log_directory(directory):
    """
    Ensures the log directory exists, creating it if necessary.
    """
    logging.info(f"Ensuring log directory exists: {directory}")  # Verbose logging
    if not os.path.exists(directory):
        os.makedirs(directory)
        logging.info(f"Created log directory: {directory}")
    return directory

def get_log_path(filename):
    """
    Returns the full path for a log file.
    """
    logging.debug(f"Getting log path for filename: {filename}")  # Verbose logging
    return os.path.join(log_dir, filename)

def clean_log_directory(directory):
    """
    Cleans the log directory by removing all files within it.
    """
    logging.info(f"Cleaning log directory: {directory}")
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
                logging.info(f"Deleted file: {file_path}")
        except Exception as e:
            logging.error(f"Error deleting file {file_path}: {e}")

def ensure_csv_headers():
    """
    Ensures that CSV files have headers if they are new.
    """
    logging.info("Ensuring CSV headers are present.")
    files_and_headers = {
        PING_CSV: ["timestamp", "avg_ping_ms"],
        SPEED_CSV: ["timestamp", "download_mbps", "upload_mbps"],
        INVESTIGATIONS_CSV: ["start_time", "end_time", "trigger_reason"],  # Added trigger_reason column
        TRACEROUTE_CSV: ["timestamp", "traceroute_output"],
        DNSLOOKUP_CSV: ["timestamp", "dns_lookup_output"]
    }
    for filename, headers in files_and_headers.items():
        file_path = get_log_path(filename)
        if not os.path.exists(file_path):
            with open(file_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
            logging.info(f"Added headers to new CSV file: {file_path}")

# ---------------------------
# PING TEST FUNCTION
# ---------------------------
def run_ping_test():
    """
    Runs a ping test to PING_TARGET and logs the average ping to ping.csv.
    If the average ping is above the threshold, triggers an investigation.
    """
    logging.info(f"Starting ping test to {PING_TARGET}...")  # Verbose logging
    try:
        result = subprocess.run(["ping", "-c", "3", PING_TARGET],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                universal_newlines=True)
        output = result.stdout
        logging.debug(f"Ping test output: {output}")  # Verbose logging
        avg_ping = None
        for line in output.splitlines():
            if "min/avg/max" in line:
                # Example line: rtt min/avg/max/mdev = 14.123/15.456/20.789/1.234 ms
                parts = line.split("=")[1].split("/")[1]
                avg_ping = float(parts)
                break
        if avg_ping is None:
            avg_ping = -1.0  # indicate an error
            logging.warning("Could not parse average ping from ping output.")  # Verbose logging
    except Exception as e:
        logging.error("Ping test error: " + str(e))
        avg_ping = -1.0

    timestamp = datetime.datetime.now().isoformat()
    with open(get_log_path(PING_CSV), "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([timestamp, avg_ping])
    
    logging.info(f"Ping test at {timestamp}: {avg_ping} ms")
    if avg_ping > PING_THRESHOLD_MS:
        trigger_investigation("ping", avg_ping)
    logging.info(f"Ping test completed. Average ping: {avg_ping} ms")  # Verbose logging
    return avg_ping


# ---------------------------
# SPEED TEST FUNCTION
# ---------------------------
def run_speed_test():
    """
    Runs a speed test and logs the download and upload speeds to speed.csv.
    If the download speed is below the threshold, triggers an investigation.
    """
    logging.info("Starting speed test...")  # Verbose logging
    try:
        st = speedtest.Speedtest()
        logging.info("Getting best server...")  # Verbose logging
        st.get_best_server()
        logging.info("Starting download test...")  # Verbose logging
        st.download()
        logging.info("Starting upload test...")  # Verbose logging
        st.upload()
        results = st.results.dict()
        download_speed = results.get("download", 0) / 1e6  # Convert to Mbps
        upload_speed = results.get("upload", 0) / 1e6
        logging.debug(f"Speed test results: {results}")  # Verbose logging
    except Exception as e:
        logging.error("Speed test error: " + str(e))
        download_speed, upload_speed = -1.0, -1.0

    timestamp = datetime.datetime.now().isoformat()
    with open(get_log_path(SPEED_CSV), "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([timestamp, download_speed, upload_speed])
    
    logging.info(f"Speed test at {timestamp}: Download {download_speed} Mbps, Upload {upload_speed} Mbps")
    if download_speed < SPEED_THRESHOLD_MBPS:
        trigger_investigation("speed", download_speed)
    logging.info(f"Speed test completed. Download: {download_speed} Mbps, Upload: {upload_speed} Mbps")  # Verbose logging
    return download_speed, upload_speed


# ---------------------------
# INVESTIGATION HANDLING
# ---------------------------
def trigger_investigation(reason, value):
    """
    If an investigation is not already active, mark the start of an investigation and
    start the deep test cycle in a new thread.
    """
    global investigation_active, investigation_start, investigation_reason
    with investigation_lock:
        if not investigation_active:
            investigation_active = True
            investigation_start = datetime.datetime.now()
            investigation_reason = reason  # Store the trigger reason
            message = f"Investigation triggered due to {reason} test (value: {value:.2f})."
            logging.info(message)
            send_telegram_message(message)
            threading.Thread(target=deep_test_cycle, daemon=True).start()
            logging.info("Deep test cycle started in a new thread.")  # Verbose logging
        else:
            logging.info("Investigation already active; not triggering another.")


def deep_test_cycle():
    """
    During an investigation, run deep tests (ping, speed test, traceroute, and DNS lookup)
    every 5 minutes until both the ping and download speeds return to acceptable levels.
    Logs all results to their respective CSVs.
    """
    logging.info("Starting deep investigation cycle...")  # Verbose logging
    send_telegram_message("Starting deep investigation cycle.")
    while True:
        current_ping = run_ping_test()
        download_speed, _ = run_speed_test()
        run_traceroute()
        run_dns_lookup()
        # Check if both conditions are back to normal
        if current_ping <= PING_THRESHOLD_MS and download_speed >= SPEED_THRESHOLD_MBPS:
            break
        logging.info(f"Investigation ongoing: waiting {investigation_interval} minutes before next deep test.")
        time.sleep(investigation_interval * 60)
    end_investigation()
    logging.info("Deep investigation cycle completed.")  # Verbose logging


def end_investigation():
    """
    Ends an ongoing investigation, logs the investigation period to investigations.csv,
    and notifies via Telegram.
    """
    global investigation_active, investigation_start, last_investigation_end, investigation_reason
    with investigation_lock:
        if investigation_active:
            investigation_end = datetime.datetime.now()
            with open(get_log_path(INVESTIGATIONS_CSV), "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([investigation_start.isoformat(), investigation_end.isoformat(), investigation_reason])  # Include reason
            investigation_active = False
            last_investigation_end = investigation_end
            message = f"Investigation ended at {investigation_end.isoformat()}."
            send_telegram_message(message)
            logging.info(message)
        else:
            logging.info("No active investigation to end.")


# ---------------------------
# TELEGRAM BOT HANDLERS
# ---------------------------
def investigate_command(update: Update, context: CallbackContext):
    """
    Handler for the /investigate command. If no investigation is currently active
    manually trigger an investigation.
    """
    global investigation_active, last_investigation_end
    now = datetime.datetime.now()
    with investigation_lock:
        if not investigation_active:
            if last_investigation_end is None or (now - last_investigation_end).total_seconds():
                send_telegram_message("Manual investigation triggered via Telegram.")
                threading.Thread(target=deep_test_cycle, daemon=True).start()
                logging.info("Manual investigation started via Telegram command.")  # Verbose logging
            else:
                send_telegram_message("Investigation was recently concluded. Not triggering a new one.")
                logging.info("Investigation recently concluded; not triggering a new one.")  # Verbose logging
        else:
            send_telegram_message("Investigation already in progress.")
            logging.info("Investigation already in progress; ignoring command.")  # Verbose logging


def telegram_listener(token):
    """
    Starts the Telegram bot to listen for commands.
    Only called if Telegram is enabled.
    """
    logging.info("Starting Telegram listener...")  # Verbose logging
    updater = Updater(token, use_context=True)
    dispatcher = updater.dispatcher
    dispatcher.add_handler(CommandHandler("investigate", investigate_command))
    updater.start_polling()
    updater.idle()
    logging.info("Telegram listener stopped.")  # Verbose logging


def send_telegram_message(message):
    """
    Sends a message via the Telegram bot to the configured chat.
    Only sends if Telegram is enabled.
    """
    global telegram_bot, TELEGRAM_CHAT_ID, telegram_enabled
    if telegram_enabled and telegram_bot and TELEGRAM_CHAT_ID:
        try:
            telegram_bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)
            logging.info(f"Telegram message sent: {message}")  # Verbose logging
        except Exception as e:
            logging.error("Failed to send Telegram message: " + str(e))
    elif not telegram_enabled:
        logging.info(f"Telegram disabled, not sending message: {message}")
    else:
        logging.warning("Telegram bot or chat ID not configured.")


# ---------------------------
# MONITORING LOOP
# ---------------------------
def monitor_loop():
    """
    Main loop for running scheduled tests. It:
      - Runs a ping test every minute.
      - Runs a speed test every 30 minutes (in normal mode).
      - Always logs to CSV files.
    The deep investigation cycle runs in a separate thread when triggered.
    """
    logging.info("Starting monitoring loop...")  # Verbose logging
    counter = 0
    while True:
        if investigation_active:
            logging.info("Investigation active; skipping scheduled tests.")
            time.sleep(investigation_interval * 60)
            continue
        # Run the ping test every ping_interval minutes
        run_ping_test()

        # Determine if it's time for a speed test (every speed_interval minutes)
        if counter <= 0:
            run_speed_test()
            counter = speed_interval // ping_interval
        else:
            logging.info(f"{counter} cycles until next speed test.")  # Verbose logging
        counter -= 1

        # Sleep for ping_interval minutes before next cycle
        logging.info(f"Waiting {ping_interval} minutes before next cycle.")
        if not investigation_active:
            print("All systems are normal")
            
        time.sleep(ping_interval * 60)
    logging.info("Monitoring loop stopped.")  # This will likely never be reached


# ---------------------------
# MAIN FUNCTION
# ---------------------------
def main():
    global TELEGRAM_CHAT_ID, telegram_bot, log_dir, telegram_enabled
    global PING_TARGET, PING_THRESHOLD_MS, SPEED_THRESHOLD_MBPS
    global investigation_interval, ping_interval, speed_interval
    parser = argparse.ArgumentParser(description="Network monitoring and investigation script.")
    parser.add_argument("--telegram-token", type=str, help="Telegram Bot Token")
    parser.add_argument("--telegram-chat-id", type=str, help="Telegram Chat ID")
    parser.add_argument("--disable-telegram", action="store_true", help="Disable Telegram functionality")
    parser.add_argument("--log-dir", type=str, default="./logs", help="Directory for storing log files (default: logs)")
    parser.add_argument("--clean-logs", action="store_true", help="Clean the log directory before starting")
    # Add new command line arguments for configurable thresholds
    parser.add_argument("--ping-target", type=str, default=PING_TARGET, 
                        help=f"Target for ping tests (default: {PING_TARGET})")
    parser.add_argument("--ping-threshold", type=float, default=PING_THRESHOLD_MS, 
                        help=f"Ping threshold in ms (default: {PING_THRESHOLD_MS})")
    parser.add_argument("--speed-threshold", type=float, default=SPEED_THRESHOLD_MBPS, 
                        help=f"Download speed threshold in Mbps (default: {SPEED_THRESHOLD_MBPS})")
    parser.add_argument("--investigation-interval", type=int, default=5, 
                        help="Interval (in minutes) between deep tests during investigation (default: 5)")
    parser.add_argument("--ping-interval", type=int, default=5, 
                        help="Interval (in minutes) between ping tests (default: 5)")
    parser.add_argument("--speed-interval", type=int, default=30, 
                        help="Interval (in minutes) between speed tests (default: 30)")
    args = parser.parse_args()
    
    # Pretty print the configuration
    logging.info("Configuration:")
    for key, value in vars(args).items():
        logging.info(f"  {key}: {value}")
    
    # Set configuration values from command line
    log_dir = ensure_log_directory(args.log_dir)

    if args.clean_logs:
        clean_log_directory(log_dir)

    ensure_csv_headers()

    # Explicitly configure logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)  # Set the root logger level to INFO
    logger.handlers = []  # Clear any existing handlers

    # FileHandler for logging to log.txt
    file_handler = logging.FileHandler(os.path.join(log_dir, "log.txt"))
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)

    # StreamHandler for logging to the terminal
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(stream_handler)

    # Pretty print the configuration
    logger.info("Configuration:")
    for key, value in vars(args).items():
        logger.info(f"  {key}: {value}")
    
    # Handle Telegram configuration
    telegram_enabled = not args.disable_telegram
    if telegram_enabled:
        if args.telegram_token and args.telegram_chat_id:
            TELEGRAM_CHAT_ID = args.telegram_chat_id
            telegram_bot = Bot(args.telegram_token)
            # Start Telegram listener in a separate thread
            threading.Thread(target=telegram_listener, args=(args.telegram_token,), daemon=True).start()
        else:
            telegram_enabled = False
            logging.warning("Telegram functionality disabled: both --telegram-token and --telegram-chat-id are required.")
    else:
        logging.info("Telegram functionality disabled by --disable-telegram flag.")
    
    # Update configurable values with command line arguments
    PING_TARGET = args.ping_target
    PING_THRESHOLD_MS = args.ping_threshold
    SPEED_THRESHOLD_MBPS = args.speed_threshold
    investigation_interval = args.investigation_interval
    ping_interval = args.ping_interval
    speed_interval = args.speed_interval
    
    logging.info(f"Using log directory: {log_dir}")
    logging.info(f"Using ping target: {PING_TARGET}")
    logging.info(f"Using ping threshold: {PING_THRESHOLD_MS} ms")
    logging.info(f"Using speed threshold: {SPEED_THRESHOLD_MBPS} Mbps")
    logging.info(f"Using investigation interval: {investigation_interval} minutes")
    logging.info(f"Using ping interval: {ping_interval} minutes")
    logging.info(f"Using speed interval: {speed_interval} minutes")
    logging.info(f"Telegram notifications: {'enabled' if telegram_enabled else 'disabled'}")

    # Start the monitoring loop (this will run indefinitely)
    monitor_loop()


def run_traceroute():
    """
    Runs a traceroute to PING_TARGET and logs the results to traceroute.csv.
    """
    logging.info(f"Starting traceroute to {PING_TARGET}...")  # Verbose logging
    try:
        result = subprocess.run(["traceroute", PING_TARGET],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                universal_newlines=True)
        output = result.stdout
        logging.debug(f"Traceroute output: {output}")  # Verbose logging
    except Exception as e:
        logging.error("Traceroute error: " + str(e))
        output = "Error"

    timestamp = datetime.datetime.now().isoformat()
    with open(get_log_path(TRACEROUTE_CSV), "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([timestamp, output])
    
    logging.info(f"Traceroute logged at {timestamp}.")
    return output


def run_dns_lookup():
    """
    Performs a DNS lookup for PING_TARGET and logs the results to dnslookup.csv.
    """
    logging.info(f"Starting DNS lookup for {PING_TARGET}...")  # Verbose logging
    try:
        result = subprocess.run(["nslookup", PING_TARGET],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                universal_newlines=True)
        output = result.stdout
        logging.debug(f"DNS lookup output: {output}")  # Verbose logging
    except Exception as e:
        logging.error("DNS lookup error: " + str(e))
        output = "Error"

    timestamp = datetime.datetime.now().isoformat()
    with open(get_log_path(DNSLOOKUP_CSV), "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([timestamp, output])
    
    logging.info(f"DNS lookup logged at {timestamp}.")
    return output


if __name__ == "__main__":
    main()