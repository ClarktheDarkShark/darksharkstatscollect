# dashboard.py – multi-channel version (updated)

from datetime import datetime                    # ⬅ NEW
from flask import Blueprint, jsonify, render_template_string, request
from sqlalchemy import func
from models import DailyStats

dash = Blueprint("dash", __name__)

# ─── SAME KEYS LIST AS BEFORE ──────────────────────────────────────────────────
KEYS = [
    "stream_date","stream_start_time",
    "stream_duration","avg_concurrent_viewers","peak_concurrent_viewers",
    "unique_viewers","viewer_growth_rate",
    "total_num_chats","total_chatters","chat_msgs_per_minute",
    "total_emotes_used","unique_emotes_used",
    "followers_start","followers_end","net_follower_change",
    "total_subscriptions","new_subscriptions_t1","new_subscriptions_t2_t3",
    "resubscriptions","gifted_subs_received","gifted_subs_given",
    "subscription_cancellations",
    "bits_donated","donation_events_count","total_donation_amount",
    "raids_received","raid_viewers_received",
    "polls_run","poll_participation",
    "predictions_run","prediction_participants",
    "game_category","category_changes","title_length",
    "has_giveaway","has_qna","tags",
    "moderation_actions","messages_deleted","timeouts_bans",
    "avg_sentiment_score","positive_negative_ratio",
    "subs_per_avg_viewer","chat_msgs_per_viewer",
    "subs_7d_moving_avg","viewers_3d_moving_avg",
    "day_over_day_peak_change","gift_subs_bool",
]

def dump_stats(row: "DailyStats") -> dict:
    d = {k: getattr(row, k) for k in KEYS}
    d["stream_date"]       = str(d["stream_date"])
    d["stream_start_time"] = d["stream_start_time"].strftime("%H:%M")
    d["stream_name"]       = row.stream_name
    return d

# ───────────────────────────────────────────────────────────────────────────────
#  A. Channels list (unchanged)
# ───────────────────────────────────────────────────────────────────────────────
@dash.route("/api/channels")
def api_channels():
    from main import _bot_holder
    from constants import TEST_USERS

    bot = _bot_holder.get("bot")
    if not bot:
        return jsonify([]), 204

    # 1) Primary bot’s channels
    chans = { ch.name.lower() for ch in bot.connected_channels if ch }

    
    chans |= {u.lower() for u in TEST_USERS}
    
    ordered = sorted(
        chans,
        key=lambda c: (c != "thelegendyagami", c)   # False < True
    )

    return jsonify(ordered)


# ───────────────────────────────────────────────────────────────────────────────
#  B. Live-stats endpoint – now uses refactored collector
# ───────────────────────────────────────────────────────────────────────────────
@dash.route("/api/live")
def api_live():
    from main import _bot_holder
    def _serialisable(obj):
        """Convert anything that JSON doesn’t understand."""
        from datetime import datetime, date, time
        if isinstance(obj, (set, frozenset)):
            # you probably only need the count – adjust if you’d rather
            return len(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, time):
            return obj.strftime("%H:%M:%S")
        return obj     

    channel = request.args.get("channel", "").strip().lower()
    live_only = request.args.get("liveOnly") == "1"   # NEW    
    # print(f">>> [dashboard] api_live called for channel='{channel}'")
    # ─── 1️⃣ In-memory live stats ─────────────────────────────────────────

    # chat_bot  = _bot_holder.get("bot")        # your original chat bot
    stats_bot = _bot_holder.get("stats_bot")  # the new StatsBot instance  🔸

    raw_map1 = {}


    # pull directly from StatsBot, not from a cog                     🔸
    raw_map2 = getattr(stats_bot, "stats_by_channel", {}) if stats_bot else {}

    live_map = {
        **{k.lower(): v for k, v in raw_map1.items()},
        **{k.lower(): v for k, v in raw_map2.items()},
    }

    stats = live_map.get(channel)
    if stats:
        payload = {}
        for k in KEYS:
            if k == "stream_start_time":
                # refactor stores it as 'start_time' (datetime) — convert here
                t = stats.get("stream_start_time") or stats.get("start_time")
                if isinstance(t, datetime):
                    t = t.strftime("%H:%M")
                payload[k] = _serialisable(t)
            else:
                payload[k] = _serialisable(stats.get(k))
        payload["stream_name"] = channel
        return jsonify(payload)

    if live_only:
      return jsonify({"error": "offline"}), 404     # <-- NEW
  
    # 2️⃣ Fall back to the latest DB row if nothing live
    row = (
        DailyStats.query
        .filter(func.lower(DailyStats.stream_name) == channel)
        .order_by(DailyStats.stream_date.desc(),
                  DailyStats.stream_start_time.desc())
        .first()
    )
    if not row:
        return jsonify({"error": "no data yet"}), 404
    return jsonify(dump_stats(row))

# ───────────────────────────────────────────────────────────────────────────────
#  C. Dashboard HTML (unchanged)
# ───────────────────────────────────────────────────────────────────────────────
TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Twitch Bot – Live Dashboard</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Rubik:wght@400;600&display=swap" rel="stylesheet" />
  <style>
    :root {
      --bg: #111827;          /* slate-900             */
      --card: #1f2937;        /* slate-800             */
      --text: #f3f4f6;        /* slate-100             */
      --accent-blue: #3b82f6; /* blue-500   viewers    */
      --accent-green: #10b981;/* emerald-500 followers */
      --accent-purple: #a855f7;/* violet-500  subs     */
      --accent-yellow: #f59e0b;/* amber-500   chats    */
      --accent-pink: #ec4899; /* fuchsia-500 misc      */
      --accent-sentiment: #f472b6; /* pink-600 sentiment */
    }
    *{box-sizing:border-box;margin:0;padding:0}
    body{
      font-family:"Rubik",system-ui,sans-serif;
      background:var(--bg);
      color:var(--text);
      padding:1rem 1.5rem;
      line-height:1.35;
    }
    h1{font-size:1.75rem;font-weight:600;margin-bottom:1rem;text-align:center}

    /* simple flex wrapper for the selector */
    .selector-wrapper{display:flex;justify-content:center;margin-bottom:1rem}
    #channel_select{
      background:var(--card);
      color:var(--text);
      padding:0.5rem 0.75rem;
      font-size:1rem;
      border-radius:0.5rem;
      border:none;
      box-shadow:0 4px 6px rgba(0,0,0,.25);
      cursor:pointer;
    }

    .sticky-header{display:flex;justify-content:center;gap:2rem;flex-wrap:wrap;margin-bottom:1.5rem}
    .sticky-header .box{
      background:var(--card);
      padding:0.6rem 1rem;
      border-radius:0.75rem;
      box-shadow:0 4px 6px rgba(0,0,0,.25);
      font-weight:600;font-size:1.1rem
    }
    .sentiment-box{background:var(--accent-sentiment);color:#fff}

    .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:1rem}
    .card{background:var(--card);border-radius:0.75rem;padding:0.9rem 1rem;box-shadow:0 4px 6px rgba(0,0,0,.3);display:flex;flex-direction:column;justify-content:space-between;transition:transform .2s ease}
    .card:hover{transform:translateY(-4px)}
    .label{font-size:0.8rem;opacity:.75;font-weight:500}
    .value{font-size:1.35rem;font-weight:600;margin-top:0.35rem;word-break:break-word}
    .ring{height:0.35rem;width:100%;border-radius:0.75rem 0.75rem 0 0;margin:-0.9rem -1rem 0.6rem}
    .blue{background:var(--accent-blue)}
    .green{background:var(--accent-green)}
    .purple{background:var(--accent-purple)}
    .yellow{background:var(--accent-yellow)}
    .pink{background:var(--accent-pink)}
    .row{
      display: grid;                                    /* grid instead of flex   */
      grid-template-columns: repeat( auto-fill,
                                    minmax(220px, 1fr) ); /* grow to full width  */
      gap: 1rem;                                        /* keep your existing gap */
      margin-bottom: 1rem;
    }
  </style>
</head>
<body>
  <h1>🎛️ Twitch Bot Live Dashboard</h1>

  <!-- channel picker -->
  <div class="selector-wrapper">
    <select id="channel_select"></select>
  </div>

  <!-- current date + start time + channel + sentiment -->
  <div class="sticky-header">
    <div class="box" id="stream_name">…</div>
    <div class="box" id="stream_date">…</div>
    <div class="box" id="stream_start_time">…</div>
    <div class="box sentiment-box" id="avg_sentiment_score">…</div>
  </div>

  <div id="row_blue"   class="row"></div>
  <div id="row_purple" class="row"></div>
  <div id="row_yellow" class="row"></div>
  <div id="row_pink"   class="row"></div>
  <div id="row_green"  class="row"></div>

  <script>
    const ENDPOINT = "/api/live";
    let currentChannel = "";
    let channels = [];

    async function checkLiveStatus() {
      const checks = await Promise.all(
        channels.map(async ch => {
          try {
            const r = await fetch(
              `${ENDPOINT}?channel=${encodeURIComponent(ch)}&liveOnly=1&t=${Date.now()}`
            );
            return { name: ch, live: r.ok };   // r.ok === true only when 200
          } catch {
            return { name: ch, live: false };
          }
        })
      );

      const sel = document.getElementById("channel_select");
      
      sel.innerHTML = "";  // clear out old <option>s
      for (const {name, live} of checks) {
        const opt = document.createElement("option");
        opt.value = name;
        opt.textContent = live ? `${name} 🔴 LIVE` : name;
        sel.appendChild(opt);
      }
      // restore selection if you’d switched channels
      sel.value = currentChannel;
    }

    // field‑to‑label mapping
    const features = {
      game_category:{l:"Game Category",c:"blue"},
      avg_concurrent_viewers:{l:"Avg Viewers",c:"blue"},
      peak_concurrent_viewers:{l:"Peak Viewers",c:"blue"},
      unique_viewers:{l:"Unique Viewers",c:"blue"},
      viewer_growth_rate:{l:"Viewer Growth %",c:"blue"},

      total_subscriptions:{l:"Total Subs",c:"purple"},
      new_subscriptions_t1:{l:"New Tier‑1 Subs",c:"purple"},
      resubscriptions:{l:"Resubs",c:"purple"},
      gifted_subs_received:{l:"Gifted Subs Recv",c:"purple"},
      gifted_subs_given:{l:"Gifted Subs Given",c:"purple"},

      total_num_chats:{l:"Chat Messages",c:"yellow"},
      chat_msgs_per_minute:{l:"Chats/min",c:"yellow"},
      total_chatters:{l:"Unique Chatters",c:"yellow"},
      total_emotes_used:{l:"Total Emotes",c:"yellow"},
      unique_emotes_used:{l:"Unique Emotes",c:"yellow"},

      bits_donated:{l:"Bits Donated",c:"pink"},
      donation_events_count:{l:"Donation Events",c:"pink"},
      raids_received:{l:"Raids",c:"pink"},
      raid_viewers_received:{l:"Raid Viewers",c:"pink"},
      messages_deleted:{l:"Messages Deleted",c:"pink"},
      timeouts_bans:{l:"Timeouts/Bans",c:"pink"},
      positive_negative_ratio:{l:"Pos/Neg Ratio",c:"pink"},

      followers_start:{l:"Followers Start",c:"green"},
      followers_end:{l:"Followers End",c:"green"},
      net_follower_change:{l:"Follower Δ",c:"green"}
    };

    const grid = document.getElementById("grid");

    // create card shells once
    for (const [key,{l,c}] of Object.entries(features)){
      const card=document.createElement("div");
      card.className="card";
      card.id="card_"+key;
      card.innerHTML=
        `<div class="ring ${c}"></div>
        <div class="label">${l}</div>
        <div class="value" id="${key}">…</div>`;
      document.getElementById(`row_${c}`).appendChild(card);  // ⬅️ send to row
    }

    
    // ──────── clear stale UI ────────
    function clearCards() {
      // header
      document.getElementById("stream_name").textContent = '…';
      document.getElementById("stream_date").textContent = '…';
      document.getElementById("stream_start_time").textContent = '…';
      document.getElementById("avg_sentiment_score").textContent = '…';
      // all metrics
      for (const k in features) {
        document.getElementById(k).textContent = '…';
      }
    }


    // ────────── API helpers ──────────
    async function fetchChannels(){
      const res=await fetch("/api/channels");
      return res.json();
    }

    async function fetchStats(){
      const res = await fetch(`${ENDPOINT}?channel=${encodeURIComponent(currentChannel)}&t=${Date.now()}`);
      if(!res.ok) throw new Error("Network");
      return res.json();
    }

    // ────────── rendering ──────────
    function updateCards(d){
      // sentiment card
      document.getElementById("avg_sentiment_score").textContent =
          `😊 ${(d.avg_sentiment_score ?? 0).toFixed(2)}`;

      // update every card – clear stale data first
      for (const k in features){
        const el = document.getElementById(k);
        const val = d[k];
        if (val === undefined || val === null){
          el.textContent = '–';            // blank when no data
        } else {
          el.textContent = (typeof val === 'number')
                          ? val.toLocaleString()
                          : val;
        }
      }
    }


    async function loadAndRender(){

      try{
        const d = await fetchStats();
        document.getElementById("stream_name").textContent=`📺 ${currentChannel}`;
        document.getElementById("stream_date").textContent=`📅 ${d.stream_date}`;
        document.getElementById("stream_start_time").textContent=`⏱️ ${d.stream_start_time}`;
        updateCards(d);
      }catch(err){console.error(err);}
    }

    /* ── bootstrap ──────────────────────────────────────────────────────── */
    (async () => {
      const sel = document.getElementById("channel_select");
      // ① safely get channels (handles 204 No-Content)
      const chResp = await fetch("/api/channels");
      channels     = chResp.status === 204 ? [] : await chResp.json();

      await checkLiveStatus();                  // first build of the menu
      currentChannel = channels[0] || "";       // pick first channel
      sel.value = currentChannel;

      sel.addEventListener("change", () => {
        currentChannel = sel.value;
        loadAndRender();
      });

      await loadAndRender();

      /* ② run BOTH updates every 20 s */
      setInterval(async () => {
        await checkLiveStatus();   // update 🔴 badges
        await loadAndRender();     // update metrics cards
      }, 20_000);

    })();
  </script>
</body>
</html>
"""

@dash.route("/dashboard")
def dashboard():
    return render_template_string(TEMPLATE)
