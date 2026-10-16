# SMB35 Server Bot Guide

This document describes the server-side opponent implemented in this repository. It is written for operators and contributors who want to run, tune, or extend the bot in a community-maintained version of the server.

> [!IMPORTANT]
> The bot is **off by default**. It only runs in game modes explicitly listed in `source/bot_modes.py`. The current implementation is deliberately limited to **one bot per match**. Do not increase `bot_count` above `1` without implementing and testing multi-bot support first.

## What the bot is

The bot is a local, server-owned participant. It does **not** start an emulator or simulate a full Super Mario Bros. game. Instead, it follows the same two network lifecycles a retail player uses:

1. It signs into the local NEX server, registers a principal ID, and joins the matchmaking session through the normal auto-matchmake request.
2. When a round is started, it receives the normal round notification, requests the round parameters, connects to Eagle over a loopback TLS WebSocket, and joins the Eagle match as a peer.
3. During play, it sends controller input (Eagle RPC 31) and attacks (RPC 33). It watches real-player attack packets to drive its synthetic health model.

This approach matters: to real clients, the bot looks like an Eagle participant rather than a server-side shortcut. The server does not have enough SMB35 game-state information to make it a level-aware, collision-aware AI.

## Repository map

| File | Responsibility |
| --- | --- |
| `source/bot_modes.py` | Enables bot modes and sets per-mode name, count, and input interval. |
| `source/bot_runtime.py` | Bot connection lifecycle, inputs, course selection, attacks, health, TTL, and retirement. Most tuning happens here. |
| `source/main.py` | Matchmaking integration, pre-round real-player cutoff, and round setup. |
| `source/eagle.py` | Eagle relay, loopback bot token, bot-peer registration, and node removal. |
| `source/config.py` | Server integer settings and optional relay trace logging. |

## Enabling the bot

Edit `source/bot_modes.py` and add the desired game mode to `BOT_MODES`. The known classic 35-Player Battle mode is already named for you:

```python
BOT_MODES = {
    CLASSIC_BATTLE_GAME_MODE: BotMode("classic-fallback", bot_count=1),
}
```

`BotMode` has these fields:

| Setting | Default | Meaning |
| --- | ---: | --- |
| `name` | required | Operator-facing mode name written to logs. |
| `bot_count` | `1` | Requested number of bots. The runtime currently raises `NotImplementedError` unless this remains exactly `1`. |
| `input_interval` | `0.8` seconds | Delay between each 30-frame input packet. It also determines the normal attack-loop cadence. |

Restart the server after changing Python source. A mode lookup happens when a matchmaking session is created, so changing the file does not alter an already-created session.

## Matchmaking and player-count behavior

When the first real player joins an enabled session, the server authenticates a bot principal and has that principal make a normal auto-matchmake request. The bot is then included in the round's player ID list and takes the first player slot after all real players. Human players use slots `0..n-1`; the one bot uses slot `n`.

### Pre-match cutoff

`BOT_PREMATCH_EXIT_REAL_PLAYER_COUNT` in `source/main.py` controls when the bot withdraws before a round starts.

```python
BOT_PREMATCH_EXIT_REAL_PLAYER_COUNT = 3
```

With the current value:

| Real humans in lobby | Bot behavior |
| ---: | --- |
| 1–2 | The bot remains eligible, joins the round, and plays. |
| 3 or more | The bot leaves the lobby, its NEX/Eagle work is stopped, and the human-only match continues. |

The cutoff is checked on each lobby join, before `StartRound`. It is intentionally not applied during round construction: removing a player after clients build their round state can invalidate that state. Once suppressed for a lobby, the bot is not re-added to that session.

Changing the value to `1` effectively disables bot play whenever a human is present. Set it to a value greater than the practical lobby size only if you deliberately want the bot retained in every match.

## Start-of-round lifecycle

The order below is intentional and should be preserved when modifying the bot:

```text
real players matchmake
        |
bot NEX principal authenticates and auto-matchmakes
        |
StartRound locks lobby and appends bot PID
        |
bot receives notification 116000 and fetches StartRound parameters
        |
real Eagle peers become ready and claim their normal slots
        |
bot connects to Eagle, increments server counter 0, waits for RPC 16
        |
bot sends JoinMatch (RPC 26) and waits for host StartMatch (RPC 28)
        |
bot input, attack, health, and lifetime loops begin
```

The bot waits for every non-host real player to send `JoinMatch` before it joins. This prevents it from taking a slot that a real peer should own. It also waits for the retail-style counter acknowledgement before `JoinMatch`.

## Course selection and random levels

Course selection is configured in `source/bot_runtime.py`:

```python
BOT_REQUESTED_LEVEL_OVERRIDE = None
BOT_RANDOMIZE_REQUESTED_LEVEL = True
SMB35_COURSE_COUNT = 32
```

SMB35 course IDs are zero-based:

| Course ID range | Displayed courses |
| --- | --- |
| `0–3` | 1-1 through 1-4 |
| `4–7` | 2-1 through 2-4 |
| `...` | `...` |
| `28–31` | 8-1 through 8-4 |

The bot chooses one requested course once per bot runtime/session.

| Configuration | Result |
| --- | --- |
| `BOT_REQUESTED_LEVEL_OVERRIDE = 12` | Always requests course ID 12 (`4-1`). The override takes precedence over every other course option. |
| `BOT_REQUESTED_LEVEL_OVERRIDE = None`, `BOT_RANDOMIZE_REQUESTED_LEVEL = True` | Randomly requests one ID from `0` through `31`. This is the current default. |
| `BOT_REQUESTED_LEVEL_OVERRIDE = None`, `BOT_RANDOMIZE_REQUESTED_LEVEL = False` | Starts at `0` (`1-1`), then mirrors the first real player's requested course when that player joins. |

The host's selected course is not announced early enough for normal mirroring while randomization is active, which is why random mode intentionally selects its own fresh course. The bot log prints both the numeric ID and `world-course` label when it joins.

## Movement and controller input

Each input packet contains exactly 30 six-bit controller frames. At the default `0.8`-second interval, the bot sends a packet about every 0.8 seconds.

Normal movement is deliberately simple:

- Every frame holds **Left** (`0x04`).
- For each 30-frame packet, there is a `45%` chance of a jump pulse.
- A jump pulse begins at a random valid frame and is held for a random `4–10` frames.
- The defined run bit (`0x08`) and right bit (`0x20`) are not used by the current movement routine.

The relevant settings are:

```python
LEFT_JUMP_CHANCE_PER_PACKET = 0.45
LEFT_JUMP_MIN_HELD_FRAMES = 4
LEFT_JUMP_MAX_HELD_FRAMES = 10
```

Keep `0 <= chance <= 1`, keep the minimum positive, and keep the maximum at least the minimum and no more than 30. The input routine needs a 30-frame packet and will fail if it is asked to place an impossible jump span.

## Attacks and spawn table

The bot sends only verified, observed `(event, variant)` pairs. It does not generate arbitrary event values, because those values may affect game state in ways other than spawning an enemy.

After a random movement phase of `4–10` input packets, it opens an attack window of one, three, or seven attacks. Those window sizes have weights `60`, `33`, and `7`, respectively. Attacks are emitted one per later input loop. Once a window ends, a new movement phase is selected.

At the default interval, movement phases last about `3.2–8.0` seconds before the next attack window begins.

### Normal enemy distribution

`ATTACK_ENEMY_WEIGHTS` is both the complete normal spawn table and its selection weights. The values total 100.

| Enemy | Event | Variant | Chance |
| --- | ---: | ---: | ---: |
| Goomba | 3 | 2 | 29% |
| Koopa | 5 | 29 | 18% |
| Green Koopa Paratroopa | 8 | 1 | 10% |
| Red Koopa Paratroopa | 10 | 6 | 8% |
| Piranha Plant | 12 | 23 | 7% |
| Buzzy Beetle | 13 | 3 | 5% |
| Bullet Bill | 14 | 5 | 4% |
| Blooper | 15 | 4 | 3% |
| Lakitu | 16 | 13 | 3% |
| Hammer Bro | 17 | 4 | 3% |
| Cheep Cheep | 20 | 28 | 3% |
| Spiny Ball | 11 | 20 | 2% |
| Podoboo | 18 | 13 | 2% |
| Flying Cheep Cheep | 19 | 4 | 2% |
| Bowser | 24 | 0 | 0.5% |
| Hammer Bowser | 25 | 13 | 0.5% |

To tune frequencies, edit only the last number in an existing row and update the table/comments in the code so the sum remains meaningful. Do **not** add an event/variant pair simply because it looks plausible; capture and validate it first against a retail client.

### Attack targets

The bot targets only real, ready Eagle peers. Server bots and virtual clients are excluded. For two or more eligible humans, it builds a shuffled target queue and serves every player once before reshuffling. It also avoids making the final target of the old queue the first target of the new queue when possible. This means a multi-player match gets fair target rotation rather than repeated random focus.

### Discovery/probe mode

`ATTACK_PROBE_EVENT_VALUES` is empty by default, so it is disabled.

```python
ATTACK_PROBE_EVENT_VALUES = ()
ATTACK_PROBE_VARIANT = 2
ATTACK_PROBE_INTERVAL_SECONDS = 3.0
```

If deliberately testing a **previously captured** event list, the bot sends each listed event in order, one every configured interval. While a probe is active, normal health retirement is paused so the run is not cut short; when the last probe is sent, health and lifetime are reset and normal attack cadence resumes. This is a development/capture tool, not a live-community feature.

## Synthetic health, TTL, and retirement

The bot has no full in-game collision or lives simulation. Instead, it has synthetic health measured in seconds, plus a separate master TTL. Both begin when the host starts the match.

### Initial health

One value is sampled uniformly from the band matching the real-player count:

| Real players | Initial health range | Health cap |
| ---: | ---: | ---: |
| 1 | 45–75 seconds | 75 seconds |
| 2 | 60–90 seconds | 90 seconds |
| 3 or more | 75–120 seconds | 120 seconds |

The constants are `HEALTH_SECONDS_BY_REAL_PLAYER_COUNT`. Counts above three use the `3` entry.

Health drains continuously with real elapsed monotonic time. It is not tied to frames, score, position, or actual enemy collisions.

### Attack effects on health

| Event | Default effect |
| --- | ---: |
| Bot sends a successful attack | `+1.0` second, capped at the current health cap. |
| Real player sends an attack addressed to the bot | `-1.0` second. |
| An attack is not addressed to the bot | No health effect. |

Adjust `HEALTH_SECONDS_PER_SENT_ATTACK` and `HEALTH_SECONDS_PER_RECEIVED_ATTACK` to rebalance this. The source records the most recent real attacker that successfully targeted the bot; this is retained for the limited 1v1 KO attribution path, but it does not grant real collision damage.

### Master lifetime (TTL)

Each round also samples an independent master lifetime from:

```python
LIFETIME_MIN_SECONDS = 180.0
LIFETIME_MAX_SECONDS = 300.0
```

Bot attacks can add health but **cannot** extend this TTL. The first condition reached ends the bot's participation:

1. master lifetime expires; or
2. synthetic health reaches zero.

Set `BOT_NEVER_RETIRE = True` only for controlled capture/training sessions. It disables both health- and TTL-based retirement and can leave a bot in a round indefinitely. It should remain `False` in public play.

### How the bot dies

On health depletion or TTL expiry, the bot stops its loop and closes its Eagle peer WebSocket. Eagle then removes the bot node and broadcasts `NodeRemoved` to real clients. This is the actual retirement mechanism.

The repository contains captured player-loss templates, including a verified 1v1 player-0-kills-player-1 record, but a synthetic bot-sent player-loss packet was ignored by retail clients. For that reason, the current runtime does **not** rely on RPC 38 to eliminate the bot. Treat node removal as the authoritative observed outcome of synthetic retirement.

## Network and deployment settings

The bot uses the server's private TLS certificate and intentionally disables verification only for its same-host NEX/Eagle connections. Network settings are intentionally generic in the tracked source:

| Setting | Default | Used for |
| --- | --- | --- |
| `EAGLE_INTERNAL_HOST` | `127.0.0.1` | Address passed to Eagle manager and used by the bot's NEX connection. |
| `EAGLE_PUBLIC_HOST` | `localhost` | Host name in the Eagle URL sent to real game clients. |
| `EAGLE_PORT` | `20001` | TCP port used for Eagle WebSocket connections. |

For a deployment, create an untracked `source/local_config.py` containing only your overrides, for example:

```python
EAGLE_INTERNAL_HOST = "server-lan-address"
EAGLE_PUBLIC_HOST = "server-public-hostname"
```

Do not publish `source/local_config.py`, `source/resources/privkey.pem`, or any real certificate/private key. Generate and deploy your own certificate material, and keep all host-specific values out of Git.

The bot token lasts five minutes. The normal player Eagle token lasts three hours. Those expiration values are protocol/session plumbing, not the gameplay TTL described above.

## Logging and diagnosis

Set the following in `source/config.py` when diagnosing a protocol problem:

```python
EAGLE_TRACE = True
EAGLE_TRACE_HEX_BYTES = 96
```

This logs a limited hexadecimal preview of relayed Eagle RPCs and decoded `JoinMatch` traffic. Disable it during routine public play: it is diagnostic output, not a gameplay feature.

Useful log markers include:

| Log text | Meaning |
| --- | --- |
| `Created match ... bot_mode=...` | Confirms the session matched an enabled bot mode. |
| `Bot NEX registered` / `auto-matchmade` | Confirms the bot completed NEX setup and joined the lobby. |
| `BOT_PREMATCH_EXIT` | Humans reached the pre-round cutoff; bot was removed intentionally. |
| `Bot runtime joined ... requested_level=... course=...` | Confirms Eagle join and shows the selected course. |
| `Bot runtime started` | Host sent StartMatch and the play loop began. |
| `Bot attack window` | Shows the number of attacks in the current burst. |
| `Bot health ...` | Shows each health adjustment and why it occurred. |
| `Bot health depleted ... closing ... Eagle peer(s)` | Bot retired because of health or master TTL. |
| `EAGLE_NODE_REMOVED ... server_bot=True` | Confirms client-visible bot removal. |

## Safe modification checklist

Before changing bot behavior, keep these invariants intact:

- Keep `bot_count=1` unless multi-bot NEX principals, Eagle peers, IDs, target behavior, and tests are implemented together.
- Do not let a bot claim an Eagle player slot before real players have finished their join setup.
- Keep bot player IDs after the real-player count; Eagle node IDs are one-based while SMB35 player IDs are zero-based.
- Preserve the counter acknowledgement before `JoinMatch`.
- Use only captured and verified attack event/variant pairs.
- Leave `BOT_NEVER_RETIRE` off for community play.
- Change health and TTL with both short and long public-match tests; the bot's health is synthetic and not an approximation of actual collision state.
- Treat `NodeRemoved`, not the unused player-loss template, as the current reliable retirement behavior.
- Replace private host names, IP addresses, TLS files, and any local credentials before publishing.

## Known limits and future work

- The bot has one fixed left-walk-and-jump movement pattern; it is not level-aware.
- It does not read the real game's collision state, score, power-ups, timer, or actual life state.
- It supports exactly one server-owned bot at a time.
- The requested course can differ from the host course while randomization is enabled.
- Death attribution is only documented by a captured 1v1 record; bot retirement intentionally uses disconnect/node removal instead.
- Attack packets are restricted to verified values by design. New enemy types require capture and validation, not guessed event IDs.

These constraints are useful guardrails for a public fork: the server stays transparent about what the bot does, avoids pretending to simulate game state it cannot observe, and limits modifications to behaviors that have an observed protocol basis.
