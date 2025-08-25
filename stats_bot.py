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
OAUTH_TOKEN     = get_oauth_token(CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN)

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

    async def event_disconnect(self):
        print("Disconnected from Twitch. Attempting to reconnect...")
        delay = getattr(self, "_reconnect_delay", 1)
        while True:
            await asyncio.sleep(delay)
            try:
                await self.connect()
                print("Reconnected to Twitch.")
                self._reconnect_delay = 1
                break
            except Exception as e:
                print(f"Reconnect failed: {e}")
                delay = min(delay * 2, 300)
                self._reconnect_delay = delay

    async def event_ready(self):
        print(f"Logged in as | {self.nick}")
        # join the remaining channels in small bursts
        for i in range(0, len(self._queued_channels), 5):
            await self.join_channels(self._queued_channels[i : i + 5])
            await asyncio.sleep(2)
        print(f"Connected to: {[ch.name for ch in self.connected_channels if ch]}")

        # 🔺  NOW start the polling loop (all joins finished)
        self.metrics_collector.start()

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
            start   = datetime.now(EST)
            chan    = live.user.name.lower()
            user    = (await self.fetch_users(names=[chan]))[0]
            token   = get_oauth_token(CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN)
            f_cnt   = await fetch_follower_count(user.id, token)
            
            try:
                tag_names = await fetch_stream_tags(user.id, token)
            except Exception as e:
                print(f"[{chan}] failed to fetch tags: {e}")
                tag_names = []

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
                'game_category':          live.game_name.lower(),
                'category_changes':       0,
                'title_length':           len(live.game_name),
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
            self._last_sent_at[chan]    = datetime.utcnow()
            self.live_channels.add(chan)

            print(f"[{chan}] stream started – tracking…")
        except Exception:
            import traceback; traceback.print_exc()

    # ─────────────────────────  LIVE POLLING  ───────────────────────────────
    async def _collect_polling_metrics(self, streams):
        now = datetime.utcnow()

        for live in streams:
            chan  = live.user.name.lower()
            stats = self.stats_by_channel.get(chan)
            if not stats:
                continue

            # raw samples
            stats['viewer_counts'].append(live.viewer_count)
            try:
                stats['followers_end'] = await fetch_follower_count(
                    live.user.id, OAUTH_TOKEN
                )
            except aiohttp.ClientResponseError:
                pass

            # sentiment every 20 min
            # if now - self._last_sent_at[chan] >= self.SENTIMENT_INTERVAL:
            #     stats['avg_sentiment_score'] = await self.calculate_avg_sentiment_score(stats, chan)
            #     self._last_sent_at[chan]     = now

            # derived viewer / chat metrics
            duration_min = (datetime.now(EST) - stats['start_time']).total_seconds() / 60
            stats['stream_duration'] = int(duration_min)

            if stats['viewer_counts']:
                avg_v     = sum(stats['viewer_counts']) / len(stats['viewer_counts'])
                peak_v    = max(stats['viewer_counts'])
                first_v   = stats['viewer_counts'][0]
                stats['avg_concurrent_viewers'] = avg_v
                stats['peak_concurrent_viewers'] = peak_v
                stats['viewer_growth_rate'] = (peak_v - first_v) / (first_v or 1)
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
        stats = self.stats_by_channel.get(chan)
        if not stats:
            return

        stats['avg_sentiment_score'] = await self.calculate_avg_sentiment_score(stats, chan)

        duration_min   = (datetime.now(EST) - stats["start_time"]).total_seconds() / 60
        avg_viewers    = stats.get("avg_concurrent_viewers", 0)
        peak_viewers   = stats.get("peak_concurrent_viewers", 0)
        uniq_viewers   = len(stats["unique_chatters"])
        total_subs     = (
            stats["new_subscriptions_t1"]
            + stats["new_subscriptions_t2_t3"]
            + stats["resubscriptions"]
            + stats["gifted_subs_received"]
            - stats["gifted_subs_given"]
            - stats["subscription_cancellations"]
        )
        stats["total_subscriptions"] = total_subs

        # import here to avoid circular main <-> bot
        from main import app

        with app.app_context():
            last = (
                DailyStats.query
                .filter_by(stream_name=chan)
                .order_by(
                    DailyStats.stream_date.desc(),
                    DailyStats.stream_start_time.desc(),
                )
                .first()
            )
            days_since = (stats["stream_date"] - last.stream_date).days if last else 0

            seven_days_ago = stats["stream_date"] - timedelta(days=7)
            three_days_ago = stats["stream_date"] - timedelta(days=3)

            avg_subs_7 = (
                db.session.query(func.avg(DailyStats.total_subscriptions))
                .filter(
                    DailyStats.stream_name == chan,
                    DailyStats.stream_date >= seven_days_ago,
                    DailyStats.stream_date < stats["stream_date"],
                )
                .scalar() or 0.0
            )
            avg_subs_3 = (
                db.session.query(func.avg(DailyStats.total_subscriptions))
                .filter(
                    DailyStats.stream_name == chan,
                    DailyStats.stream_date >= three_days_ago,
                    DailyStats.stream_date < stats["stream_date"],
                )
                .scalar() or 0.0
            )
            viewers_3d_moving_avg = (
                db.session. query(func.avg(DailyStats.avg_concurrent_viewers))
                .filter(
                    DailyStats.stream_name == chan,
                    DailyStats.stream_date >= three_days_ago,
                    DailyStats.stream_date < stats["stream_date"],
                )
                .scalar() or 0.0
            )
            sent_scores = stats['sentiment_scores']
            min_sent = min(sent_scores) if sent_scores else None
            max_sent = max(sent_scores) if sent_scores else None

            prev_peak = last.peak_concurrent_viewers if last else peak_viewers
            day_over_day_peak_change = peak_viewers - prev_peak

            daily = DailyStats(
                stream_name               = chan,
                stream_date               = stats["stream_date"],
                day_of_week               = stats["stream_date"].strftime("%A"),
                is_weekend                = stats["stream_date"].weekday() >= 5,
                is_holiday                = stats["stream_date"] in US_HOLIDAYS,
                stream_start_time         = stats["start_time"].time(),
                days_since_previous_stream= days_since,
                stream_duration           = int(duration_min),
                avg_concurrent_viewers    = avg_viewers,
                peak_concurrent_viewers   = peak_viewers,
                unique_viewers            = uniq_viewers,
                viewer_growth_rate        = stats["viewer_growth_rate"],
                total_num_chats           = stats["total_num_chats"],
                total_chatters            = uniq_viewers,
                chat_msgs_per_minute      = stats["chat_msgs_per_minute"],
                total_emotes_used         = sum(len(e.split(":")) for e in stats["emote_set"]),
                unique_emotes_used        = len(stats["emote_set"]),
                followers_start           = stats["followers_start"],
                followers_end             = stats["followers_end"],
                net_follower_change       = stats["net_follower_change"],
                total_subscriptions       = total_subs,
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
                avg_sentiment_score       = stats["avg_sentiment_score"],
                min_sentiment_score       = min_sent,
                max_sentiment_score       = max_sent,
                positive_negative_ratio   = stats["positive_negative_ratio"],
                subs_per_avg_viewer       = stats["subs_per_avg_viewer"],
                chat_msgs_per_viewer      = stats["chat_msgs_per_viewer"],
                subs_7d_moving_avg        = float(avg_subs_7),
                subs_3d_moving_avg        = float(avg_subs_3),
                viewers_3d_moving_avg     = None,
                day_over_day_peak_change  = day_over_day_peak_change,
                gift_subs_bool            = stats["gift_subs_bool"],
            )
            db.session.add(daily)
            db.session.commit()
            print(f"[{chan}] stats committed to DB")

        # clean-up
        self.stats_by_channel.pop(chan, None)
        self._last_sent_at.pop(chan, None)
        self.live_channels.discard(chan)



    # ─────────────────────────  LIVE STREAM  ───────────────────────────────
    async def live_stream_data(self, chan: str):
        stats = self.stats_by_channel.get(chan)
        if not stats:
            return

        # ── 1) Throttle to one run per interval ──────────────────────────
        now = datetime.utcnow()
        now_est = now.replace(tzinfo=pytz.utc).astimezone(EST)

        last = self._last_sent_at.get(chan)
        # print()
        # print('last', last)
        # print('now', now)
        # print
        if last is not None and (now - last) < self.SENTIMENT_INTERVAL:
            return
        
        self._last_sent_at[chan] = now

        # ── 2) Define our look-back window ──────────────────────────────
        interval  = self.SENTIMENT_INTERVAL
        threshold = now_est - interval

        # ── 3) VIEWER METRICS over window ───────────────────────────────
        samples = stats.get("viewer_counts", [])
        # how many 20s-ticks fit in the window?
        step   = max(1, int(interval.total_seconds() // 20))
        window = samples[-step:]
        avg_v  = sum(window) / len(window) if window else 0.0
        peak_v = max(window) if window else 0

        # ── 4) CHAT METRICS over window ────────────────────────────────
        recent_msgs = [
            m for m in self.conversation_history_metadata
            if m.get("role")=="user"
               and m.get("channel_name")==chan
               and datetime.fromisoformat(m["timestamp"])\
                     .astimezone(EST) >= threshold
        ]
        chat_count   = len(recent_msgs)
        unique_chat  = len({m["name"] for m in recent_msgs})
        chats_per_min= chat_count / (interval.total_seconds()/60 or 1)

        # ── 5) EMOTE METRICS over window ────────────────────────────────
        emotes = set()
        for m in recent_msgs:
            for p in m.get("tags","").split("/"):
                if p: emotes.add(p.split(":")[0])
        total_emotes  = sum(tag.count(":") for tag in emotes)
        unique_emotes = len(emotes)

        # ── 6) DELTA helper for all cumulative counters ────────────────
        def delta(key):
            now_val = stats.get(key, 0)
            prev    = stats.get(f"last_{key}", 0)
            stats[f"last_{key}"] = now_val
            return now_val - prev

        # subscriptions
        new_t1   = delta("new_subscriptions_t1")
        new_t23  = delta("new_subscriptions_t2_t3")
        resub    = delta("resubscriptions")
        gift_rec = delta("gifted_subs_received")
        gift_giv = delta("gifted_subs_given")
        cancels  = delta("subscription_cancellations")
        subs_int = (new_t1 + new_t23 + resub + gift_rec - gift_giv - cancels)

        # Followers
        net_follow_cg = delta("net_follower_change")

        # bits & donations
        bits_int       = delta("bits_donated")
        donate_cnt_int = delta("donation_events_count")
        donate_amt_int = delta("total_donation_amount")

        # raids
        raids_int        = delta("raids_received")
        raid_viewers_int = delta("raid_viewers_received")

        # polls & predictions
        polls_int        = delta("polls_run")
        poll_part_int    = delta("poll_participation")
        preds_int        = delta("predictions_run")
        pred_part_int    = delta("prediction_participants")

        # misc counters
        catchg_int       = delta("category_changes")
        titlelen_int     = delta("title_length")
        modacts_int      = delta("moderation_actions")
        delmsgs_int      = delta("messages_deleted")
        tosbans_int      = delta("timeouts_bans")

        # sentiment (live)
        try:
            stats['avg_sentiment_score'] = await self.calculate_avg_sentiment_score(
                stats, chan, live=True
            )
            stats['sentiment_scores'].append(stats['avg_sentiment_score'])
        except BadRequestError:
            stats['avg_sentiment_score'] = 0.5
            stats['sentiment_scores'].append(0.5)
        sentiment_score = stats['avg_sentiment_score']
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
                stream_duration           = int(interval.total_seconds()/60),

                avg_concurrent_viewers    = avg_v,
                peak_concurrent_viewers   = peak_v,
                unique_viewers            = unique_chat,
                viewer_growth_rate        = ((peak_v - (window[0] if window else 0))
                                             / (window[0] or 1)),

                total_num_chats           = chat_count,
                total_chatters            = unique_chat,
                chat_msgs_per_minute      = chats_per_min,

                total_emotes_used         = total_emotes,
                unique_emotes_used        = unique_emotes,

                followers_start           = None,
                followers_end             = None,
                net_follower_change       = net_follow_cg,

                total_subscriptions       = subs_int,
                new_subscriptions_t1      = new_t1,
                new_subscriptions_t2_t3   = new_t23,
                resubscriptions           = resub,
                gifted_subs_received      = gift_rec,
                gifted_subs_given         = gift_giv,
                subscription_cancellations= cancels,

                bits_donated              = bits_int,
                donation_events_count     = donate_cnt_int,
                total_donation_amount     = donate_amt_int,

                raids_received            = raids_int,
                raid_viewers_received     = raid_viewers_int,

                polls_run                 = polls_int,
                poll_participation        = poll_part_int,
                predictions_run           = preds_int,
                prediction_participants   = pred_part_int,

                game_category             = stats["game_category"] or "Unknown",
                category_changes          = catchg_int,
                title_length              = titlelen_int,
                has_giveaway              = stats["has_giveaway"],
                has_qna                   = stats["has_qna"],
                tags                      = stats["tags"],

                moderation_actions        = modacts_int,
                messages_deleted          = delmsgs_int,
                timeouts_bans             = tosbans_int,

                avg_sentiment_score       = sentiment_score,
                positive_negative_ratio   = pos_neg_ratio,

                subs_per_avg_viewer       = (subs_int / (avg_v or 1)),
                chat_msgs_per_viewer      = (chat_count / (unique_chat or 1)),

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
        print(f"{author_name}: {message.content} ({chan})")

        if author_name != 'nightbot' and author_name != 'wizebot':
            self.conversation_history.append({
                'role': 'user',
                'content': message.content,
                'name': author_name
            })
            # print(f"[DEBUG➜APPEND] raw message.channel = {message.channel!r}")
            # print(f"[DEBUG➜APPEND] channel_name var = {channel_name!r}")
            self.conversation_history_metadata.append({
                'role': 'user',
                'content': message.content,
                'name': author_name,
                'timestamp': datetime.now(EST).isoformat(),
                'channel_name': chan.lower()
            })

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
                