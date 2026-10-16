"""Configuration for server-side Super Mario Bros. 35 bot modes.

Each bot is a local NEX and Eagle client owned by the server. It is not an
emulator process. Keeping the mode selection in this small module lets normal
35-player matches keep using the unmodified path.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class BotMode:
	"""Configuration for one server-side Eagle bot mode."""

	name: str
	bot_count: int = 1
	input_interval: float = 0.8


# The normal 35-Player Battle game_mode observed on this server.
CLASSIC_BATTLE_GAME_MODE = 70010000


# Only modes listed here receive server-owned NEX participants and Eagle
# peers. Keep this empty while deploying ordinary server changes. The first
# bot experiment should use one bot, then increase only after it stays stable.
#
# BOT_MODES = {
# 	CLASSIC_BATTLE_GAME_MODE: BotMode("classic-fallback", bot_count=1),
# }
BOT_MODES = {}


def for_game_mode(game_mode):
	"""Return the explicitly enabled bot mode for a matchmaking game mode."""

	return BOT_MODES.get(game_mode)
