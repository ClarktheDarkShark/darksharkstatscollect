# constants.py


INTERVAL = 600
IM_TIME_LIMIT = 180  # in seconds
CHANNEL_NAME = 'thelegendyagami'
CHANNEL_NAME_TEST = 'darksharkai'

MAIN_CHANNELS = ['thelegendyagami', 'clarkthedarkshark']
TEST_USERS = [
    'smallant', 
    'alaskabel', 
    'smallantmom', 
    'necros', 
    'ari_hokage', 
    'obsydyon', 
    'naadofett', 
    'ellie_minibot',
    'clauvio',
    'luthimir',
    'tinysnub',
    'alinity',
    'peach',
    'boymetgirl',
    'boesschen',
    'l3layze',
    'strikerhutassa',
    'chainsawchody',
    'Barbarousking',
    'shroud',
    'raysfire',
    'player5',
    'beardandthehair',
    'KPVI0L3t',
    'caseoh_',
    'rdcmysticlee',
    'jynxzi',
    'agent00',
    'burtonttv']

DEFAULT_PROMPT = [
                {
                    "role": "system",
                    "content": f"Always give an actual answer and be confident. \
                    Never say you cannot answer a question. Response will always be extremely short, no more than 50 words.\
                    Please keep responses very short, concise, and to the point. Always give the ###User Prompt significantly more weight than the ###Internet Content.\
                    Never ask questions, and do not be overly optimistic. Even if you do not know the answer, feel free to make something up, but always give a decisive\
                    response."
                }
            ]
DEFAULT_IM_PROMPT = [
                {
                    "role": "system",
                    "content": f"You generate prompts for generating images. Be detailed enough to produce the requested image."
                }
            ]
AI_WEREFOLF_DECIDER = [
                {
                    "role": "system",
                    "content": (
                        "Respond ONLY with 'YES' or 'NO'. Your purpose is to decide if you should respond in the chat based on the full conversation history. "
                        "Do not respond if you are the only one speaking repeatedly without recent user input. If you were the last to speak, respond with NO."
                        "ALWAYS respond if someone addresses you directly with '@darksharkAI'. "
                        "Do NOT respond if there are multiple consecutive messages from you without user input between them. "
                        "Be reserved with how much you talk. Be more of a fly on the wall unless someone speaks to you directly."
                        "Do NOT respond if you were the last one to talk. "
                        "If the history suggests you're dominating the conversation, respond with 'NO'."
                        "If you believe this is a good time to interact based on recent user messages, respond only with 'YES'. Otherwise, respond only with 'NO'."
                    )
                }
            ]


EXAMPLE_MESSAGES = [
                { "role": "assistant", "content": "theleg259Burn" },
                { "role": "assistant", "content": "theleg259Burn" },
                { "role": "assistant", "content": "clarkthedarkshark theleg259Heyguys" },
                { "role": "assistant", "content": "Chewsday innit?" },
                { "role": "assistant", "content": "Games loud" },
                { "role": "assistant", "content": "name fillyWaves" },
                { "role": "assistant", "content": "name.... barbGabnon" },
                { "role": "assistant", "content": "Me oldy" },
                { "role": "assistant", "content": "hahah" },
                { "role": "assistant", "content": "chillin man, finished this weeks exam so im free, now im brain rotting to dragon quest 3" },
                { "role": "usassistanter", "content": "obsydyHi Name" },
                { "role": "assistant", "content": "hey name dorugoAmetHeart" },
                { "role": "assistant", "content": "theleg259Heyguys clarkthedarkshark" },
                { "role": "assistant", "content": "theleg259Heyguys" },
                { "role": "assistant", "content": "theleg259Heyguys" },
                { "role": "usassistanter", "content": "easy 1 bby" },
                { "role": "assistant", "content": "A" },
                { "role": "assistant", "content": "I also think we have next week off for school" },
                { "role": "assistant", "content": "so im super chill mode now" },
                { "role": "assistant", "content": "i'll be workin" },
                { "role": "assistant", "content": "gettin that MONEYYYYY" },
                { "role": "assistant", "content": "I told my wife there was a limit of 4 people in our house theleg259LUL" },
                { "role": "usassistanter", "content": "I wish we had that limit at our house" },
                { "role": "assistant", "content": "I have a huge family..." },
                { "role": "assistant", "content": "if its a quiet evening ill join them" },
                { "role": "assistant", "content": "volumes off, FYI" },
                { "role": "assistant", "content": "my volume" },
                { "role": "assistant", "content": "doing bedtime with the kids" },
                { "role": "assistant", "content": "Name, how big is your family?" },
                { "role": "assistant", "content": "awww nostalgia from GPW2" },
                { "role": "assistant", "content": "We made crockpot brisket" },
                { "role": "assistant", "content": "And it was amazing Lewd" }

            ]

SLANG_TERMS = [
        "lit",
        "stoked",
        "amped",
        "buzzing",
        "pumped",
        "jacked",
        "fired",
        "send it",
        "full send",
        "no cap",
        "huck",
        "shred",
        "bombing",
        "steezy",
        "gnar",
        "sketch",
        "crushing",
        "rip",
        "fearless",
        "yolo",
        "wild",
        "clutch",
        "legendary",
        "next level",
        "savage",
        "beast mode",
        "certified",
        "flex",
        "vibing",
        "goated",
        "bussin",
        "fire",
        "dope",
        "hard",
        "clean",
        "icy",
        "slaps",
        "chill",
        "grind",
        "drip",
        "heat",
        "wavy",
        "pog",
        "cracked",
        "smurfing",
        "slaying",
        "riding the edge",
        "ragged edge",
        "adrenaline junkie",
        "risk it",
        "catching air",
        "dialed in",
        "bomb the hill",
        "hype",
        "thrash",
        "big air",
        "rip it",
        "low key",
        "fire up",
        "hype up",
        "go big or go home",
        "risk it for the biscuit",
        "live fast die young",
        "hold my beer",
        "let's get wild",
        "chasing the dragon"
    ]


# SEED_HISTORY = [
#     {
#         "role": "user",
#         "content": "Good evening ClarktheDarkShark"
#     },
#     {
#         "role": "user",
#         "content": "@DarkSharkAI is a wicked aswesome AI bot"
#     },
#     {
#         "role": "user",
#         "content": "Fullstack dev is dope"
#     },
#     {
#         "role": "user",
#         "content": "Lets go!"
#     },
#     {
#         "role": "user",
#         "content": "Who's ready for AI chating with AI?"
#     }
# ]

SEED_HISTORY = []















































