# main.py
import os, threading, sys, asyncio, time, logging, signal, traceback
sys.modules["main"] = sys.modules[__name__]
from flask import Flask, send_file, abort
from dotenv import load_dotenv
from db import db

# Load environment variables (including DATABASE_URL)
load_dotenv()

def create_app(include_migrate: bool = False):
    from dashboard import dash
    app = Flask(__name__)

    logging.getLogger('werkzeug').setLevel(logging.ERROR)

    # 1) Database configuration
    uri = os.getenv("DATABASE_URL", "sqlite:///local.db")
    if uri.startswith("postgres://"):
        uri = uri.replace("postgres://", "postgresql://", 1)
    app.config["SQLALCHEMY_DATABASE_URI"] = uri
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    # 2) Initialize DB + migrations
    db.init_app(app)
    if include_migrate:
        from flask_migrate import Migrate
        Migrate(app, db)

    app.register_blueprint(dash)

    @app.route("/")
    def home():
        return "Bot is running."

    @app.route("/images/<filename>")
    def serve_image(filename):
        filepath = os.path.join('/tmp', filename)
        if not os.path.isfile(filepath):
            abort(404)
        return send_file(filepath, mimetype='image/png')

    return app

app = create_app(include_migrate=True)

_bot_holder: dict[str, object] = {}

# ─────────────────────────────────────────────────────────────────────────────
# Flask (runs alongside the bot)
# ─────────────────────────────────────────────────────────────────────────────
def run_flask():
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False, threaded=False)

# ─────────────────────────────────────────────────────────────────────────────
# Bot supervisor: recreate the client on fatal errors and backoff
# ─────────────────────────────────────────────────────────────────────────────
async def run_bot_forever():
    if os.getenv("RUN_BOT", "1") != "1":
        # RUN_BOT disabled → sleep forever to keep dyno alive for Flask
        await asyncio.Event().wait()

    backoff = 1  # seconds, doubles up to 5 minutes
    while True:
        try:
            from stats_bot import StatsBot
            import constants

            channels = constants.MAIN_CHANNELS + getattr(constants, "TEST_USERS", [])
            bot = StatsBot(channels=channels)
            _bot_holder["stats_bot"] = bot

            # Start the bot; this should run until a fatal error or an explicit close.
            # TwitchIO handles reconnects internally; we only recreate the whole client on crash.
            await bot.start()

        except asyncio.CancelledError:
            # Graceful shutdown path
            break
        except Exception as e:
            print("[FATAL] Bot crashed:", repr(e))
            traceback.print_exc()
        finally:
            try:
                # Ensure the client is fully closed before restarting
                b = _bot_holder.pop("stats_bot", None)
                if b:
                    await b.close()
            except Exception:
                pass

        # Exponential backoff before re-creating a fresh client
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 300)  # cap at 5 minutes

# ─────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # 1) start Flask in a background thread
    threading.Thread(target=run_flask, daemon=True).start()

    # 2) run the bot supervisor forever
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Graceful shutdown on SIGTERM/SIGINT (Heroku dyno stop, etc.)
    stop_event = asyncio.Event()

    def _handle_signal(signum, frame):
        try:
            loop.call_soon_threadsafe(stop_event.set)
        except Exception:
            pass

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handle_signal)
        except Exception:
            # Some environments (e.g., Windows) may not support all signals
            pass

    async def _main():
        bot_task = asyncio.create_task(run_bot_forever())
        stopper = asyncio.create_task(stop_event.wait())

        done, pending = await asyncio.wait({bot_task, stopper}, return_when=asyncio.FIRST_COMPLETED)
        # If stop_event fired, cancel the bot task
        if stopper in done and not bot_task.done():
            bot_task.cancel()
            with contextlib.suppress(Exception):
                await bot_task

    import contextlib
    try:
        loop.run_until_complete(_main())
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        asyncio.set_event_loop(None)
        loop.close()
