# dashboard_predictions.py

from flask import Blueprint, render_template_string, request

# pull trained artifacts from the predictor cog
from archive.predictor import (
    get_predictor_artifacts,
    _infer_grid_for_game,   # internal helper; fine to import
)

dash_preds = Blueprint('dash_preds', __name__, url_prefix='/predictions')

TEMPLATE = """
<!doctype html>
<html>
  <head>
    <meta charset="utf-8">
    <title>Stream Predictions</title>
    <style>
      table { border-collapse: collapse; width: 60%; max-width: 600px; }
      th, td { border: 1px solid #ccc; padding: 6px 10px; text-align: center; }
      th { background: #f5f5f5; }
      body { font-family: sans-serif; margin: 2rem; }
      form { margin-bottom: 1rem; }
      input { padding: 4px; }
    </style>
  </head>
  <body>
    <h1>Top {{ top_n }} Predictions{% if game %} for “{{ game }}”{% endif %}</h1>
    <form method="get">
      <label>Stream (channel): <input name="stream" value="{{ stream }}"></label>
      <label>Game: <input name="game" value="{{ game }}"></label>
      <label>Top N: <input name="top_n" type="number" value="{{ top_n }}" min="1" max="50" style="width:4em;"></label>
      <button type="submit">Go</button>
    </form>
    {% if not ready %}
      <p>Model not trained yet. Try again soon.</p>
    {% else %}
    <table>
      <thead>
        <tr>
          <th>Start Time</th>
          <th>Duration (hrs)</th>
          <th>Expected Subs</th>
        </tr>
      </thead>
      <tbody>
      {% for row in predictions %}
        <tr>
          <td>{{ row.Time }}</td>
          <td>{{ row.Duration }}</td>
          <td>{{ row.Expected_Subs }}</td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
    {% endif %}
  </body>
</html>
"""

@dash_preds.route('/', methods=['GET'])
def show_predictions():
    # fetch trained artifacts from predictor cog
    pipe, df_for_inf, features, cat_opts, start_opts, dur_opts, metrics = get_predictor_artifacts()
    ready = pipe is not None and df_for_inf is not None

    # query params
    stream = request.args.get('stream', 'thelegendyagami')  # default: your main channel
    game   = request.args.get('game', None)                  # optional override
    top_n  = int(request.args.get('top_n', 10))

    if not ready:
        return render_template_string(
            TEMPLATE,
            ready=False,
            stream=stream,
            game=game or "",
            top_n=top_n,
            predictions=[],
        )

    # if caller didn't supply a game, use the last recorded game_category for the stream
    if game is None or game == "":
        try:
            game = df_for_inf[df_for_inf["stream_name"] == stream].iloc[-1]["game_category"]
        except IndexError:
            game = ""

    # run inference grid (uses last row history for `stream`)
    top_df = _infer_grid_for_game(
        pipe,
        df_for_inf,
        features,
        stream_name=stream,
        start_times=start_opts,
        durations=dur_opts,
        category_options=[game] if game else cat_opts,  # restrict if user selected game
        top_n=top_n,
        unique_scores=True,
    )

    # If user specified a game explicitly, be extra sure we filter to it
    if game:
        top_df = top_df[top_df['game_category'] == game]

    # format for template
    disp = top_df.copy()
    disp['Time']          = disp['start_time_hour'].astype(int).map(lambda h: f"{h:02d}:00")
    disp['Duration']      = disp['stream_duration'].astype(int)
    disp['Expected_Subs'] = disp['y_pred'].round().astype(int)

    return render_template_string(
        TEMPLATE,
        ready=True,
        stream=stream,
        game=game,
        top_n=top_n,
        predictions=disp[['Time','Duration','Expected_Subs']].to_dict(orient='records'),
    )
