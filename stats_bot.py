# stats_bot.py
# Fully-refactored version of cogs/daily_stats_collector_test.py
# Runs as a standalone TwitchIO Bot that can be imported and started from main.py

import os, time, re, asyncio, aiohttp, pytz, holidays
from datetime import datetime, timedelta, date
from twitchio.ext import commands, routines
from sqlalchemy import func
from openai import OpenAI
from openai import BadRequestError

from db import db
from models import DailyStats, TimeSeries
from utils import get_oauth_token
import utils
from constants import MAIN_CHANNELS

# ─────────────────────────────  ENV / TOKENS  ────────────────────────────────
CLIENT_ID       = os.getenv("CLIENT_ID_BILLY")
CLIENT_SECRET   = os.getenv("CLIENT_SECRET_BILLY")
REFRESH_TOKEN   = os.getenv("REFRESH_TOKEN_BILLY")
OAUTH_TOKEN, REFRESH_TOKEN = get_oauth_token(CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN)

EST             = pytz.timezone("US/Eastern")
US_HOLIDAYS     = holidays.US()
METRICS_INC = 60

# ───────────────────────  TWITCH APP-TOKEN HELPER  ───────────────────────────
_app_token:   str | None = None
_token_expiry:          float = 0.0       # unix epoch
est = pytz.timezone('America/New_York')

async def get_app_access_token() -> str:
    """Return (and cache) an app access-token for helix calls."""
    global _app_token, _token_expiry
    if _app_token and time.time() < (_token_expiry - 60):
        return _app_token

    data = {
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type":    "client_credentials",
    }
    async with aiohttp.ClientSession() as sess:
        async with sess.post("https://id.twitch.tv/oauth2/token", data=data) as r:
            r.raise_for_status()
            js = await r.json()

    _app_token    = js["access_token"]
    _token_expiry = time.time() + js["expires_in"]
    return _app_token


async def fetch_follower_count(user_id: str, user_token: str) -> int:
    url = "https://api.twitch.tv/helix/channels/followers"
    headers = {
        "Client-ID":     CLIENT_ID,
        "Authorization": f"Bearer {user_token}",
    }
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url, headers=headers,
                            params={"broadcaster_id": user_id}) as r:
            r.raise_for_status()
            return (await r.json())["total"]


async def fetch_stream_tags(broadcaster_id: str, token: str) -> list[str]:
    url = "https://api.twitch.tv/helix/channels"
    headers = {
        "Client-ID":     CLIENT_ID,
        "Authorization": f"Bearer {token}",
    }
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url, headers=headers,
                            params={"broadcaster_id": broadcaster_id}) as r:
            r.raise_for_status()
            payload = await r.json()
    if not payload["data"]:
        return []
    return payload["data"][0].get("tags", [])


# ─────────────────────────────  MAIN BOT  ────────────────────────────────────
class StatsBot(commands.Bot):
    """Standalone bot version of DailyStatsCollector Cog."""

    SENTIMENT_INTERVAL = timedelta(minutes=5)

    def __init__(self, channels: list[str]):
        first, *rest = [c.lower() for c in channels]
        super().__init__(
            token=OAUTH_TOKEN,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            refresh_token=REFRESH_TOKEN,
            prefix="!",
            initial_channels=[first],
            initial_membership = True,
            join_timeout       = 10,
        )
        self._queued_channels = rest
        # runtime state
        self.live_channels:          set[str] = set()
        self.stats_by_channel:       dict[str, dict] = {}
        self._last_sent_at:          dict[str, datetime] = {}
        self.processed_events:       set[str] = set()
        self.bulk_gift_ids:          set[str] = set()
        self.conversation_history_metadata: list[dict] = []   # external code can append
        self.client = OpenAI(api_key=os.getenv('OPENAI_KEY'))
        self.google_service = utils.authenticate_gdrive()
        self.load_chat_history()
        self.last_ping_time = 0
        self._reconnect_delay = 1

        # start the polling loop
        # self.metrics_collector.start()

    # async def event_disconnect(self):
    #     print("Disconnected from Twitch. Attempting to reconnect...")
    #     delay = getattr(self, "_reconnect_delay", 1)
    #     while True:
    #         await asyncio.sleep(delay)
    #         try:
    #             await self.connect()
    #             print("Reconnected to Twitch.")
    #             self._reconnect_delay = 1
    #             break
    #         except Exception as e:
    #             print(f"Reconnect failed: {e}")
    #             delay = min(delay * 2, 300)
    #             self._reconnect_delay = delay

    def _rehydrate_stats(self, row: TimeSeries) -> dict:
        """Reconstruct in-memory stats dict from a TimeSeries row."""
        start_dt = datetime.combine(row.stream_date, row.stream_start_time)
        if start_dt.tzinfo is None:
            start_dt = EST.localize(start_dt)
        stats = {
            'stream_name':            row.stream_name,
            'stream_date':            row.stream_date,
            'start_time':             start_dt,
            'viewer_counts':          [row.avg_concurrent_viewers],
            'unique_chatters':        {f'pre_{i}' for i in range(row.total_chatters)},
            'emote_set':              {f'pre_{i}' for i in range(row.unique_emotes_used)},
            'total_num_chats':        row.total_num_chats,
            'followers_start':        row.followers_start,
            'followers_end':          row.followers_end,
            'new_subscriptions_t1':   row.new_subscriptions_t1,
            'new_subscriptions_t2_t3':row.new_subscriptions_t2_t3,
            'resubscriptions':        row.resubscriptions,
            'gifted_subs_received':   row.gifted_subs_received,
            'gifted_subs_given':      row.gifted_subs_given,
            'subscription_cancellations': row.subscription_cancellations,
            'bits_donated':           row.bits_donated,
            'donation_events_count':  row.donation_events_count,
            'total_donation_amount':  row.total_donation_amount,
            'raids_received':         row.raids_received,
            'raid_viewers_received':  row.raid_viewers_received,
            'polls_run':              row.polls_run,
            'poll_participation':     row.poll_participation,
            'predictions_run':        row.predictions_run,
            'prediction_participants':row.prediction_participants,
            'game_category':          row.game_category,
            'category_changes':       row.category_changes,
            'title_length':           row.title_length,
            'has_giveaway':           row.has_giveaway,
            'has_qna':                row.has_qna,
            'tags':                   row.tags or [],
            'moderation_actions':     row.moderation_actions,
            'messages_deleted':       row.messages_deleted,
            'timeouts_bans':          row.timeouts_bans,
            'avg_sentiment_score':    row.avg_sentiment_score or 0.5,
            'min_sentiment_score':    row.avg_sentiment_score or 0.5,
            'max_sentiment_score':    row.avg_sentiment_score or 0.5,
            'sentiment_scores':       [row.avg_sentiment_score or 0.5],
            'positive_negative_ratio':row.positive_negative_ratio,
            'gift_subs_bool':         row.gift_subs_bool,
        }
        return stats

    async def event_ready(self):
        print(f"Logged in as | {self.nick}")
        # join the remaining channels in small bursts
        for i in range(0, len(self._queued_channels), 5):
            await self.join_channels(self._queued_channels[i : i + 5])
            await asyncio.sleep(2)
        print(f"Connected to: {[ch.name for ch in self.connected_channels if ch]}")

        # 🔺  NOW start the polling loop (all joins finished)
        try:
            self.metrics_collector.start()
        except RuntimeError:
            # Routine already running (or similar) — safe to ignore
            pass
        try:
            self._watchdog.start()
        except RuntimeError:
            pass

    async def save_chat_history(self):
        await utils.save_data(
            data=self.conversation_history_metadata,
            filename='chat_history.json',
            google_service=self.google_service,
            append=False,
            date_based_filename=True
        )

    def load_chat_history(self, max_messages=50):
        # load everything…
        history = utils.load_data(
            filename='chat_history.json',
            google_service=self.google_service,
            default_data=[],
            date_based_filename=True
        )
        # only keep the last `max_messages` entries
        tail = history[-max_messages:]

        # now filter fields and assign
        self.conversation_history = [
            {k: v for k, v in entry.items() if k in {'role', 'content', 'name'}}
            for entry in tail
        ]



    @routines.routine(seconds=60)
    async def _watchdog(self):
        # Lightweight: no manual reconnect here.
        # Just log and let the outer loop / TwitchIO handle recovery.
        try:
            # If absolutely necessary, you can check connected channels:
            chs = [ch.name for ch in self.connected_channels if ch]
            if not chs:
                print("[watchdog] No connected channels detected.")
        except Exception as e:
            print(f"[watchdog] Error checking status: {e}")


    # ─────────────────────────  POLLING LOOP  ───────────────────────────────
    @routines.routine(seconds=METRICS_INC)
    async def metrics_collector(self):
        await self.wait_for_ready()
        await self.save_chat_history()

        channels = [ch.name for ch in self.connected_channels if ch]
        if not channels:          # nothing joined yet → just wait for next tick
            return
        streams  = await self.fetch_streams(user_logins=channels)   # live only
        now_live = {s.user.name.lower() for s in streams}

        # newly-started streams
        for s in streams:
            name = s.user.name.lower()
            if name not in self.live_channels:
                try:
                    await self._on_stream_start(s)
                except Exception:
                    import traceback; traceback.print_exc()

        # per-stream polling metrics
        await self._collect_polling_metrics(streams)

        # streams that ended
        for ended in self.live_channels - now_live:
            await self._on_stream_end(ended)

        self.live_channels = now_live

    # ─────────────────────────  STREAM START  ───────────────────────────────
    async def _on_stream_start(self, live):
        try:
            # Twitch provides the actual stream start time; use it for accuracy.
            # Falling back to "now" can cause drift and, with the rehydrate
            # logic below, may mistakenly re-use an old start time.
            start   = live.started_at.replace(tzinfo=pytz.utc).astimezone(EST)
            chan    = live.user.name.lower()
            user    = (await self.fetch_users(names=[chan]))[0]
            global REFRESH_TOKEN
            token, REFRESH_TOKEN = get_oauth_token(CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN)
            # update the bot's tokens so future refreshes succeed
            self._http.token = token
            self._http._refresh_token = REFRESH_TOKEN
            f_cnt   = await fetch_follower_count(user.id, token)

            try:
                tag_names = await fetch_stream_tags(user.id, token)
            except Exception as e:
                print(f"[{chan}] failed to fetch tags: {e}")
                tag_names = []

            # Attempt to rehydrate existing stats to avoid data loss after restart
            from main import app
            with app.app_context():
                last = (
                    TimeSeries.query
                    .filter_by(stream_name=chan, stream_date=start.date())
                    .order_by(TimeSeries.id.desc())
                    .first()
                )

            if last:
                # Only rehydrate if the last snapshot belongs to this stream.
                last_start = datetime.combine(last.stream_date, last.stream_start_time)
                if last_start.tzinfo is None:
                    last_start = EST.localize(last_start)
                if abs((start - last_start).total_seconds()) <= 15 * 60:
                    stats = self._rehydrate_stats(last)
                    stats['followers_end'] = f_cnt
                    stats['tags'] = tag_names
                    self.stats_by_channel[chan] = stats
                    print(f"[{chan}] stream resumed – rehydrated from DB")
                else:
                    last = None

            if not last:
                stats = {
                    'stream_name':            chan,
                    'stream_date':            start.date(),
                    'start_time':             start,
                    'viewer_counts':          [],
                    'unique_chatters':        set(),
                    'emote_set':              set(),
                    'total_num_chats':        0,
                    'followers_start':        f_cnt,
                    'followers_end':          f_cnt,
                    'new_subscriptions_t1':   0,
                    'new_subscriptions_t2_t3':0,
                    'resubscriptions':        0,
                    'gifted_subs_received':   0,
                    'gifted_subs_given':      0,
                    'subscription_cancellations': 0,
                    'bits_donated':           0,
                    'donation_events_count':  0,
                    'total_donation_amount':  0.0,
                    'raids_received':         0,
                    'raid_viewers_received':  0,
                    'polls_run':              0,
                    'poll_participation':     0,
                    'predictions_run':        0,
                    'prediction_participants':0,
                    'game_category':          (live.game_name or "unknown").lower(),
                    'category_changes':       0,
                    'title_length':           len(live.title or ""),
                    'has_giveaway':           False,
                    'has_qna':                False,
                    'tags':                   tag_names,
                    'moderation_actions':     0,
                    'messages_deleted':       0,
                    'timeouts_bans':          0,
                    'avg_sentiment_score':    0.5,
                    'min_sentiment_score':    0.5,
                    'max_sentiment_score':    0.5,
                    'sentiment_scores':       [],
                    'positive_negative_ratio':None,
                    'gift_subs_bool':         False,
                }
                self.stats_by_channel[chan] = stats
                print(f"[{chan}] stream started – tracking…")

            self._last_sent_at[chan] = datetime.utcnow()
            self.live_channels.add(chan)
        except Exception:
            import traceback; traceback.print_exc()

    # ─────────────────────────  LIVE POLLING  ───────────────────────────────
    async def _collect_polling_metrics(self, streams):
        global OAUTH_TOKEN, REFRESH_TOKEN
        now = datetime.utcnow()

        for live in streams:
            chan  = live.user.name.lower()
            stats = self.stats_by_channel.get(chan)
            if not stats:
                from main import app
                with app.app_context():
                    last = (
                        TimeSeries.query
                        .filter_by(stream_name=chan, stream_date=datetime.now(EST).date())
                        .order_by(TimeSeries.id.desc())
                        .first()
                    )
                if last:
                    stats = self._rehydrate_stats(last)
                    self.stats_by_channel[chan] = stats
                    self._last_sent_at[chan] = datetime.utcnow()
                    self.live_channels.add(chan)
                else:
                    continue

            # raw samples
            stats['viewer_counts'].append(live.viewer_count)

            # refresh follower token when necessary
            try:
                stats['followers_end'] = await fetch_follower_count(
                    live.user.id, OAUTH_TOKEN
                )
            except aiohttp.ClientResponseError as e:
                if e.status in (401, 403):
                    # token likely expired – refresh and retry once
                    OAUTH_TOKEN, REFRESH_TOKEN = get_oauth_token(CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN)
                    self._http.token = OAUTH_TOKEN
                    self._http._refresh_token = REFRESH_TOKEN
                    try:
                        stats['followers_end'] = await fetch_follower_count(
                            live.user.id, OAUTH_TOKEN
                        )
                    except aiohttp.ClientResponseError:
                        pass
                else:
                    pass

            # sentiment every 20 min
            # if now - self._last_sent_at[chan] >= self.SENTIMENT_INTERVAL:
            #     stats['avg_sentiment_score'] = await self.calculate_avg_sentiment_score(stats, chan)
            #     self._last_sent_at[chan]     = now

            # derived viewer / chat metrics
            duration_min = (datetime.now(EST) - stats['start_time']).total_seconds() / 60
            stats['stream_duration'] = int(duration_min)

            if stats['viewer_counts']:
                counts = stats['viewer_counts']
                # Ignore the first and last five minutes to avoid early spikes
                # as viewers join and drops when the stream winds down.
                trimmed_counts = counts[5:-5] if len(counts) > 10 else []

                if trimmed_counts:
                    avg_v  = sum(trimmed_counts) / len(trimmed_counts)
                    peak_v = max(trimmed_counts)
                    first_v = next((v for v in trimmed_counts if v > 0), trimmed_counts[0])
                    stats['avg_concurrent_viewers'] = avg_v
                    stats['peak_concurrent_viewers'] = peak_v
                    stats['viewer_growth_rate'] = (peak_v - first_v) / (first_v or 1)
                else:
                    stats['avg_concurrent_viewers'] = 0
                    stats['peak_concurrent_viewers'] = 0
                    stats['viewer_growth_rate'] = 0.0
            else:
                stats['avg_concurrent_viewers'] = 0
                stats['peak_concurrent_viewers'] = 0
                stats['viewer_growth_rate'] = 0.0

            uniq_chatters                = len(stats['unique_chatters'])
            stats['unique_viewers']       = uniq_chatters
            stats['total_chatters']       = uniq_chatters
            stats['chat_msgs_per_minute'] = stats['total_num_chats'] / (duration_min or 1)

            # emote metrics
            stats['total_emotes_used']   = sum(len(e.split(":")) for e in stats['emote_set'])
            stats['unique_emotes_used']  = len(stats['emote_set'])

            # subs & follower deltas
            total_subs = (
                stats['new_subscriptions_t1']
                + stats['new_subscriptions_t2_t3']
                + stats['resubscriptions']
                + stats['gifted_subs_received']
                - stats['gifted_subs_given']
                - stats['subscription_cancellations']
            )
            stats['total_subscriptions']   = total_subs
            stats['net_follower_change']   = stats['followers_end'] - stats['followers_start']
            stats['subs_per_avg_viewer']   = total_subs / (stats['avg_concurrent_viewers'] or 1)
            stats['chat_msgs_per_viewer']  = stats['total_num_chats'] / (uniq_chatters or 1)

            if stats['game_category'] != live.game_name:
                stats['game_category'] = live.game_name
                stats['category_changes'] += 1

            await self.live_stream_data(chan)

    # ─────────────────────────  STREAM END  ───────────────────────────────
    async def _on_stream_end(self, chan: str):
        from main import app

        with app.app_context():
            last = (
                TimeSeries.query
                .filter_by(stream_name=chan)
                .order_by(TimeSeries.id.desc())
                .first()
            )
            if not last:
                return

            first = (
                TimeSeries.query
                .filter_by(stream_name=chan, stream_date=last.stream_date)
                .order_by(TimeSeries.id)
                .first()
            )
            if not first:
                return

            # sentiment stats across all snapshots
            avg_sent, min_sent, max_sent = db.session.query(
                func.avg(TimeSeries.avg_sentiment_score),
                func.min(TimeSeries.avg_sentiment_score),
                func.max(TimeSeries.avg_sentiment_score),
            ).filter_by(stream_name=chan, stream_date=last.stream_date).first()

            prev = (
                DailyStats.query
                .filter_by(stream_name=chan)
                .order_by(
                    DailyStats.stream_date.desc(),
                    DailyStats.stream_start_time.desc(),
                )
                .first()
            )

            days_since = (last.stream_date - prev.stream_date).days if prev else 0
            seven_days_ago = last.stream_date - timedelta(days=7)
            three_days_ago = last.stream_date - timedelta(days=3)

            avg_subs_7 = (
                db.session.query(func.avg(DailyStats.total_subscriptions))
                .filter(
                    DailyStats.stream_name == chan,
                    DailyStats.stream_date >= seven_days_ago,
                    DailyStats.stream_date < last.stream_date,
                )
                .scalar() or 0.0
            )
            avg_subs_3 = (
                db.session.query(func.avg(DailyStats.total_subscriptions))
                .filter(
                    DailyStats.stream_name == chan,
                    DailyStats.stream_date >= three_days_ago,
                    DailyStats.stream_date < last.stream_date,
                )
                .scalar() or 0.0
            )
            viewers_3d_moving_avg = (
                db.session.query(func.avg(DailyStats.avg_concurrent_viewers))
                .filter(
                    DailyStats.stream_name == chan,
                    DailyStats.stream_date >= three_days_ago,
                    DailyStats.stream_date < last.stream_date,
                )
                .scalar() or 0.0
            )

            prev_peak = prev.peak_concurrent_viewers if prev else last.peak_concurrent_viewers
            day_over_day_peak_change = last.peak_concurrent_viewers - prev_peak

            daily = DailyStats(
                stream_name               = chan,
                stream_date               = last.stream_date,
                day_of_week               = last.stream_date.strftime("%A"),
                is_weekend                = last.stream_date.weekday() >= 5,
                is_holiday                = last.stream_date in US_HOLIDAYS,
                stream_start_time         = first.stream_start_time,
                days_since_previous_stream= days_since,
                stream_duration           = last.stream_duration,
                avg_concurrent_viewers    = last.avg_concurrent_viewers,
                peak_concurrent_viewers   = last.peak_concurrent_viewers,
                unique_viewers            = last.unique_viewers,
                viewer_growth_rate        = last.viewer_growth_rate,
                total_num_chats           = last.total_num_chats,
                total_chatters            = last.total_chatters,
                chat_msgs_per_minute      = last.chat_msgs_per_minute,
                total_emotes_used         = last.total_emotes_used,
                unique_emotes_used        = last.unique_emotes_used,
                followers_start           = first.followers_start,
                followers_end             = last.followers_end,
                net_follower_change       = last.net_follower_change,
                total_subscriptions       = last.total_subscriptions,
                new_subscriptions_t1      = last.new_subscriptions_t1,
                new_subscriptions_t2_t3   = last.new_subscriptions_t2_t3,
                resubscriptions           = last.resubscriptions,
                gifted_subs_received      = last.gifted_subs_received,
                gifted_subs_given         = last.gifted_subs_given,
                subscription_cancellations= last.subscription_cancellations,
                bits_donated              = last.bits_donated,
                donation_events_count     = last.donation_events_count,
                total_donation_amount     = last.total_donation_amount,
                raids_received            = last.raids_received,
                raid_viewers_received     = last.raid_viewers_received,
                polls_run                 = last.polls_run,
                poll_participation        = last.poll_participation,
                predictions_run           = last.predictions_run,
                prediction_participants   = last.prediction_participants,
                game_category             = last.game_category,
                category_changes          = last.category_changes,
                title_length              = last.title_length,
                has_giveaway              = last.has_giveaway,
                has_qna                   = last.has_qna,
                tags                      = last.tags,
                moderation_actions        = last.moderation_actions,
                messages_deleted          = last.messages_deleted,
                timeouts_bans             = last.timeouts_bans,
                avg_sentiment_score       = float(avg_sent or 0.0),
                min_sentiment_score       = min_sent,
                max_sentiment_score       = max_sent,
                positive_negative_ratio   = last.positive_negative_ratio,
                subs_per_avg_viewer       = last.subs_per_avg_viewer,
                chat_msgs_per_viewer      = last.chat_msgs_per_viewer,
                subs_7d_moving_avg        = float(avg_subs_7),
                subs_3d_moving_avg        = float(avg_subs_3),
                viewers_3d_moving_avg     = float(viewers_3d_moving_avg),
                day_over_day_peak_change  = day_over_day_peak_change,
                gift_subs_bool            = last.gift_subs_bool,
            )
            db.session.add(daily)
            db.session.commit()
            print(f"[{chan}] stats committed to DB")

        # clean-up
        self.stats_by_channel.pop(chan, None)
        self._last_sent_at.pop(chan, None)
        self.live_channels.discard(chan)

        # Reset event caches when no streams remain to prevent unbounded growth
        if not self.live_channels:
            self.processed_events.clear()
            self.bulk_gift_ids.clear()



    # ─────────────────────────  LIVE STREAM  ───────────────────────────────
    async def live_stream_data(self, chan: str):
        stats = self.stats_by_channel.get(chan)
        if not stats:
            return

        now = datetime.utcnow()
        now_est = now.replace(tzinfo=pytz.utc).astimezone(EST)

        # sentiment is recalculated at a slower interval, but we still
        # commit a snapshot every time this function is called
        last = self._last_sent_at.get(chan)
        if last is None or (now - last) >= self.SENTIMENT_INTERVAL:
            self._last_sent_at[chan] = now
            try:
                stats['avg_sentiment_score'] = await self.calculate_avg_sentiment_score(
                    stats, chan, live=True
                )
                stats['sentiment_scores'].append(stats['avg_sentiment_score'])
            except BadRequestError:
                stats['avg_sentiment_score'] = 0.5
                stats['sentiment_scores'].append(0.5)

        sentiment_score = stats.get('avg_sentiment_score', 0.5)
        pos_neg_ratio   = stats.get("positive_negative_ratio")

        # ── 8) Build & commit the interval snapshot ─────────────────────
        from main import app

        with app.app_context():
            # ── 7) Static fields & previous‐row lookup ───────────────────────
            row_date = stats["stream_date"]
            last_row = TimeSeries.query\
                        .filter_by(stream_name=chan)\
                        .order_by(TimeSeries.id.desc())\
                        .first()
            days_prev = (row_date - last_row.stream_date).days if last_row else 0
            now_est = now.replace(tzinfo=pytz.utc).astimezone(EST)
            naive_now_est = now_est.replace(tzinfo=None)


            row = TimeSeries(
                stream_name               = chan,
                snapshot_time             = naive_now_est,
                stream_date               = row_date,
                day_of_week               = row_date.strftime("%A"),
                is_weekend                = row_date.weekday() >= 5,
                is_holiday                = row_date in US_HOLIDAYS,
                stream_start_time         = stats["start_time"].time(),
                days_since_previous_stream= days_prev,
                stream_duration           = stats["stream_duration"],

                avg_concurrent_viewers    = stats["avg_concurrent_viewers"],
                peak_concurrent_viewers   = stats["peak_concurrent_viewers"],
                unique_viewers            = stats["unique_viewers"],
                viewer_growth_rate        = stats["viewer_growth_rate"],

                total_num_chats           = stats["total_num_chats"],
                total_chatters            = stats["total_chatters"],
                chat_msgs_per_minute      = stats["chat_msgs_per_minute"],

                total_emotes_used         = stats["total_emotes_used"],
                unique_emotes_used        = stats["unique_emotes_used"],

                followers_start           = stats["followers_start"],
                followers_end             = stats["followers_end"],
                net_follower_change       = stats["net_follower_change"],

                total_subscriptions       = stats["total_subscriptions"],
                new_subscriptions_t1      = stats["new_subscriptions_t1"],
                new_subscriptions_t2_t3   = stats["new_subscriptions_t2_t3"],
                resubscriptions           = stats["resubscriptions"],
                gifted_subs_received      = stats["gifted_subs_received"],
                gifted_subs_given         = stats["gifted_subs_given"],
                subscription_cancellations= stats["subscription_cancellations"],

                bits_donated              = stats["bits_donated"],
                donation_events_count     = stats["donation_events_count"],
                total_donation_amount     = stats["total_donation_amount"],

                raids_received            = stats["raids_received"],
                raid_viewers_received     = stats["raid_viewers_received"],

                polls_run                 = stats["polls_run"],
                poll_participation        = stats["poll_participation"],
                predictions_run           = stats["predictions_run"],
                prediction_participants   = stats["prediction_participants"],

                game_category             = stats["game_category"] or "Unknown",
                category_changes          = stats["category_changes"],
                title_length              = stats["title_length"],
                has_giveaway              = stats["has_giveaway"],
                has_qna                   = stats["has_qna"],
                tags                      = stats["tags"],

                moderation_actions        = stats["moderation_actions"],
                messages_deleted          = stats["messages_deleted"],
                timeouts_bans             = stats["timeouts_bans"],

                avg_sentiment_score       = sentiment_score,
                positive_negative_ratio   = pos_neg_ratio,

                subs_per_avg_viewer       = stats["subs_per_avg_viewer"],
                chat_msgs_per_viewer      = stats["chat_msgs_per_viewer"],

                subs_7d_moving_avg        = None,
                subs_3d_moving_avg        = None,
                viewers_3d_moving_avg     = None,
                day_over_day_peak_change  = None,

                gift_subs_bool            = stats["gift_subs_bool"],
            )
            db.session.add(row)
            db.session.commit()
            print(f"[{chan}] stats committed to DB")



    # ──────────────────────  CHAT / EVENT HANDLERS  ─────────────────────────
    async def event_message(self, message):
        if (message.echo or message.author is None or
            message.author.name.lower() == self.nick.lower()):
            return

        chan  = message.channel.name.lower()
        stats = self.stats_by_channel.get(chan)
        if not stats:
            return

        await self.keep_alive(chan)
        author_name = message.author.name
        
        # print(f"{author_name}: {message.content} ({chan})")

        if author_name != 'nightbot' and author_name != 'wizebot':
            self.conversation_history.append({
                'role': 'user',
                'content': message.content,
                'name': author_name
            })
            if len(self.conversation_history) > 500:
                self.conversation_history = self.conversation_history[-500:]
            # print(f"[DEBUG➜APPEND] raw message.channel = {message.channel!r}")
            # print(f"[DEBUG➜APPEND] channel_name var = {channel_name!r}")
            self.conversation_history_metadata.append({
                'role': 'user',
                'content': message.content,
                'name': author_name,
                'timestamp': datetime.now(EST).isoformat(),
                'channel_name': chan.lower()
            })
            if len(self.conversation_history_metadata) > 500:
                self.conversation_history_metadata = self.conversation_history_metadata[-500:]

        stats['total_num_chats'] += 1
        stats['unique_chatters'].add(message.author.name)

        emotes = message.tags.get('emotes')
        if emotes:
            for p in emotes.split('/'):
                if p:
                    stats['emote_set'].add(p.split(':')[0])

        bits = message.tags.get("bits")
        if bits:
            stats["bits_donated"]          += int(bits)
            stats["donation_events_count"] += 1


    async def event_raw_usernotice(self, channel, tags):
        chan  = channel.name.lower()
        stats = self.stats_by_channel.get(chan)
        if not stats:
            return

        msg_id        = tags.get('msg-id')
        user          = tags.get('login') or 'unknown'
        origin_id     = tags.get('msg-param-origin-id')
        community_id  = tags.get('msg-param-community-gift-id', 'no_community_id')

        event_uid = f"{msg_id}-{user}-{origin_id or community_id}"
        if event_uid in self.processed_events:
            return
        self.processed_events.add(event_uid)

        if msg_id == 'submysterygift':
            count = int(tags.get('msg-param-mass-gift-count', '1'))
            stats['gifted_subs_received'] += count
            stats['gift_subs_bool'] = True
            self.bulk_gift_ids.add(community_id)
        elif msg_id == 'subgift':
            if community_id not in self.bulk_gift_ids:
                stats['gifted_subs_received'] += 1
                stats['gift_subs_bool'] = True
        elif msg_id == 'sub':
            stats['new_subscriptions_t1'] += 1
        elif msg_id == 'resub':
            stats['resubscriptions'] += 1
        elif msg_id == 'raid':
            viewers = int(tags.get('msg-param-viewerCount', '0'))
            stats['raids_received']       += 1
            stats['raid_viewers_received'] += viewers


    async def event_clearchat(self, channel, tags):
        chan = channel.name.lower()
        stats = self.stats_by_channel.get(chan)
        if stats:
            stats["timeouts_bans"] = stats.get("timeouts_bans", 0) + 1

    async def event_cheer(self, event):
        chan = event.channel.name.lower()
        stats = self.stats_by_channel.get(chan)
        if stats:
            stats['bits_donated']          += event.bits
            stats['donation_events_count'] += 1

    async def calculate_avg_sentiment_score(
        self,
        stats,
        chan,
        model: str = "gpt-4o-mini",
        live: bool = False
    ) -> float:
        est = pytz.timezone("US/Eastern")

        # ── 1) Determine the timestamp threshold ────────────────────────────────
        if live:
            threshold = datetime.now(EST) - timedelta(minutes=30)
        else:
            threshold = stats["start_time"]

        # ── 2) Collect messages for this channel after threshold ────────────────
        msgs = [
            m["content"]
            for m in self.conversation_history_metadata
            if m.get("role") == "user"
            and m.get("channel_name") == chan
            and datetime.fromisoformat(m["timestamp"]).astimezone(est) >= threshold
        ]

        # ── 3) Cap the number of messages to avoid context overflow ─────────────
        MAX_MSGS = 100
        msgs = msgs[-MAX_MSGS:]

        if not msgs:
            print("\n" + "*"*58)
            print(f"No messages for sentiment analysis on channel: {chan}")
            print("*"*58 + "\n")
            return 0.5  # neutral default

        # ── 4) Build the prompt ─────────────────────────────────────────────────
        prompt = (
            "You are a sentiment analysis assistant. Return ONE decimal number "
            "between 0.00 and 1.00 (e.g. 0.75) for these messages:\n\n"
            + "\n".join(msgs)
        )

        # ── 5) Call OpenAI & handle errors ──────────────────────────────────────
        try:
            resp = await self.openai_model_calls(
                model=model,
                messages=[{"role": "system", "content": prompt}],
                max_tokens=5,
                temperature=0.0
            )
            raw = resp.choices[0].message.content.strip()
            m = re.search(r"\d\.\d+", raw)
            score = float(m.group()) if m else float(raw)
            return max(0.0, min(1.0, score))

        except BadRequestError as e:
            print(f"[{chan}] sentiment analysis failed: {e}")
            return 0.5
    # ─────────────────────────  CLEANUP  ──────────────────────────────────
    def __del__(self):
        try:
            self.metrics_collector.cancel()
        except Exception:
            pass


    async def openai_model_calls(self, model, messages, max_tokens=30, temperature=0.8):
        if model == 'o3-mini':
            response = self.client.chat.completions.create(
                model=model,
                messages=messages
            )
        else:
            response = self.client.chat.completions.create(
                model=model,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature
            )
        return response
    
    async def keep_alive(self, channel_name):
        current_time_ = time.time()
        save_freq = 60 * 5
        if current_time_ - self.last_ping_time >= save_freq:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get('https://darksharkstatscollect-a6c60b29865d.herokuapp.com') as resp:
                        await resp.text()
                self.last_ping_time = current_time_
            except Exception as e:
                print(f"Error while sending keep-alive ping: {e}")
                