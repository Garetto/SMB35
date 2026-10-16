"""Wire-level server bots for Super Mario Bros. 35 Eagle sessions.

The retail client owns its local simulation.  A server bot therefore acts as
an Eagle peer: it joins the match, sends regular InputKey packets, and can
later emit game actions such as Attack.  The packets in this module are based
on the documented SMB35 Eagle RPC layouts, rather than recorded byte strings.
"""

from dataclasses import dataclass

import anyio
from anynet import streams, tls, websocket
from nintendo.nex import backend, kerberos, matchmaking_eagle as matchmaking, \
	messaging, notification, rmc, secure

import contextlib
import logging
import random
import time
import config
logger = logging.getLogger(__name__)


RPC_JOIN_MATCH = 26
RPC_INCREASE_SERVER_COUNTER = 19
RPC_START_MATCH = 28
RPC_INPUT_KEY = 31
RPC_ATTACK = 33
RPC_PLAYER_LOSS = 38

# Normal attacks are selected from observed event/variant pairs only.  The
# weights approximate how often each enemy appears in SMB1: early, ordinary
# enemies are common; castle-only enemies are intentionally very rare.  Values
# total 100, which makes this table both the source of the selection weights and
# the operator-facing percentage grid.
#
# Do not add arbitrary event values here.  Event/variant pairs can affect more
# than spawning, so every entry must first be captured in the attack register.
ATTACK_ENEMY_WEIGHTS = (
	# event, variant, readable name, percentage
	(3, 2, "Goomba", 29.0),
	(5, 29, "Koopa", 18.0),
	(8, 1, "Green Koopa Paratroopa", 10.0),
	(10, 6, "Red Koopa Paratroopa", 8.0),
	(12, 23, "Piranha Plant", 7.0),
	(13, 3, "Buzzy Beetle", 5.0),
	(14, 5, "Bullet Bill", 4.0),
	(15, 4, "Blooper", 3.0),
	(16, 13, "Lakitu", 3.0),
	(17, 4, "Hammer Bro", 3.0),
	# Ordinary Cheep Cheep; distinct from the Flying Cheep Cheep below.
	(20, 28, "Cheep Cheep", 3.0),
	(11, 20, "Spiny Ball", 2.0),
	(18, 13, "Podoboo", 2.0),
	(19, 4, "Flying Cheep Cheep", 2.0),
	(24, 0, "Bowser", 0.5),
	(25, 13, "Hammer Bowser", 0.5),
)
ATTACK_ENEMY_SELECTION_WEIGHTS = tuple(entry[3] for entry in ATTACK_ENEMY_WEIGHTS)
# Disabled: arbitrary event values can alter gameplay state rather than spawn
# an enemy, so only captured/verified Attack pairs may be used in normal play.
ATTACK_PROBE_EVENT_VALUES = ()
ATTACK_PROBE_VARIANT = 2
ATTACK_PROBE_INTERVAL_SECONDS = 3.0
# Start each attack window only after a longer movement phase.  The selected
# window then sends one, three, or seven attacks on successive input packets.
MOVE_BURST_MIN_INPUT_PACKETS = 4
MOVE_BURST_MAX_INPUT_PACKETS = 10
ATTACK_BURST_SIZES = (1, 3, 7)
ATTACK_BURST_WEIGHTS = (60, 33, 7)
# Left-running test behavior: each 30-frame input record has a chance to carry
# one short jump pulse at a random point in the record.
LEFT_JUMP_CHANCE_PER_PACKET = 0.45
LEFT_JUMP_MIN_HELD_FRAMES = 4
LEFT_JUMP_MAX_HELD_FRAMES = 10

# The bot's health is measured in seconds. It drains continuously, earns a
# small amount by sending an attack, and loses substantially more when a real
# player sends an attack addressed to it. The bands reflect the amount of
# incoming pressure from the real-player count without pretending the server
# has a full SMB35 collision simulation.
HEALTH_SECONDS_BY_REAL_PLAYER_COUNT = {
	1: (45.0, 75.0),
	2: (60.0, 90.0),
	3: (75.0, 120.0), # Three or more real players use this band.
}
HEALTH_SECONDS_PER_SENT_ATTACK = 1.0
HEALTH_SECONDS_PER_RECEIVED_ATTACK = 1.0
# Normal bot rounds use synthetic health and lifetime retirement.  This must
# only be enabled during deliberate long-lived capture/training sessions.
BOT_NEVER_RETIRE = False
# Health gains cannot keep the bot in a round indefinitely. This independent
# timer is selected once at round start and is never adjusted by attack events.
LIFETIME_MIN_SECONDS = 180.0
LIFETIME_MAX_SECONDS = 300.0

# Exact 1v1 records captured from two retail clients. The KO record is player
# 1 dying to player 0, which is the same player ordering as the configured
# one-human/one-bot session. Its player-0 client then emitted RPC 39 (Feat).
#
# Kinnay's SMB35 RPC reference documents Dead's field widths but not the
# semantics of its trailing fields. We therefore replay the real 1v1 record
# only for this matching topology; do not treat it as a general loss schema.
KO_DEATH_TEMPLATE = bytes.fromhex("020000000000000120")
# A clean real timeout from player 0. It carries the observed no-attacker
# sentinel and did not trigger RPC 39 (Feat), so it is used when the bot's
# independent lifetime expires.
TIMEOUT_DEATH_TEMPLATE = bytes.fromhex("01fc000000000000b9")

# Controller masks decoded from the retail-player Eagle trace.  The live bot
# test establishes that 0x20 is right; use the opposite directional bit 0x04
# for this left-walk experiment. Jump and run remain 0x10 and 0x08.
# SMB35 encodes 30 six-bit controller states in each RPC 31 packet.
INPUT_JUMP = 0x10
INPUT_RUN = 0x08
INPUT_LEFT = 0x04
INPUT_RIGHT = 0x20

EAGLE_VERSION = 3
APP_VERSION = 0
DDL_HASH = 0
VERSION_STRING = "2.0.4"
NODE_BITS = 11
ALL_NODES = 1025
# Give a real host time to process the bot's NodeAdded notice before the bot
# sends its counter and JoinMatch. A loopback peer otherwise reaches JoinMatch
# in the same scheduler turn as ClientReady, unlike a retail remote peer.
EAGLE_PEER_SETTLE_SECONDS = 0.25
# In the verified two-retail-player trace, player 2 waits between receiving
# the RPC 16 reply to its counter increment and sending RPC 26 JoinMatch.
# The previous settle delay was before the counter request, so it did not
# reproduce this protocol boundary.
EAGLE_COUNTER_TO_JOIN_SECONDS = 1.0

PACKET_ACCEPTED = 0
PACKET_LOGIN_REQUEST = 1
PACKET_LOGIN_RESULT = 2
PACKET_CLIENT_READY = 3
PACKET_PING = 4
PACKET_PONG = 5
PACKET_DISCONNECTED = 9

PLAYER_NAME_BYTES = 33
PLAYERS_PER_MATCH = 35

# Exact values from the verified second SMB35 player that reached RPC 28 in
# classic mode. These fields are part of Eagle JoinMatch and are not cosmetic:
# the host compares the peer profile during its pre-start validation.
DEFAULT_COINS = 3711
DEFAULT_COIN_LEVEL = 27
# The current successful two-human reference uses these undocumented JoinMatch
# values. They must remain aligned with the retail client rather than treated
# as a server-side ready flag.
DEFAULT_JOIN_UNKNOWN16 = 0
DEFAULT_JOIN_UNKNOWN1 = True
# SMB35 course ids are zero-based: 0 is 1-1 and 31 is 8-4. Select a fresh
# course for each bot match until host-course mirroring is decoded.
BOT_REQUESTED_LEVEL_OVERRIDE = None
BOT_RANDOMIZE_REQUESTED_LEVEL = True
SMB35_COURSE_COUNT = 32


@dataclass(frozen=True)
class BotPlayer:
	"""The two identities SMB35 needs for one server-side bot."""

	player_id: int
	node_id: int
	principal_id: int
	name: str


class BotNotificationServer(notification.NotificationServer):
	"""Receive the same round-start notice that a retail client receives."""

	def __init__(self, runtime):
		super().__init__()
		self.runtime = runtime

	async def process_notification_event(self, client, event):
		if event.type == 116000:
			self.runtime.note_round_notification(event.param1)


class BotMessageDeliveryServer(messaging.MessageDeliveryServer):
	"""Accept server-delivered mailbox traffic while the bot is registered."""

	def __init__(self, runtime):
		super().__init__()
		self.runtime = runtime

	async def deliver_message(self, client, message):
		logger.info(
			"Bot NEX received message session=%i message=%r",
			self.runtime.server.session_id, message
		)


class BotBackEndClient(backend.BackEndClient):
	"""Local NEX client whose two loopback TLS connections trust this server."""

	@contextlib.asynccontextmanager
	async def login(self, username, password=None, auth_info=None, servers=[]):
		# SMB35 uses the NX login-with-param route (NEX 4.6). The local
		# server's certificate is intentionally private, so the bot must trust it
		# just as its loopback Eagle peer does.
		result = await self._login_with_param(username, auth_info)
		secure_station = result.secure_station
		kerberos_key = result.source_key
		if not kerberos_key:
			raise RuntimeError("The local bot login did not return a source key")

		ticket = kerberos.ClientTicket.decrypt(
			result.ticket, kerberos_key, self._settings
		)
		if ticket.target != secure_station["PID"]:
			response = await self._auth_proto_nx.request_ticket(
				result.pid, secure_station["PID"]
			)
			response.result.raise_if_error()
			ticket = kerberos.ClientTicket.decrypt(
				response.ticket, kerberos_key, self._settings
			)

		credentials = kerberos.Credentials(
			ticket, result.pid, secure_station["CID"]
		)
		host, port = secure_station["address"], secure_station["port"]
		if host == "0.0.0.1":
			host, port = self._auth_host, self._auth_port
		context = tls.TLSContext()
		context.disable_verification()
		async with rmc.connect(
			self._settings, host, port, secure_station["sid"], context,
			credentials, servers
		) as client:
			yield client


@contextlib.asynccontextmanager
async def connect_backend(settings, host, port):
	"""Connect to the local NEX server over its private TLS certificate."""

	context = tls.TLSContext()
	context.disable_verification()
	async with rmc.connect(settings, host, port, context=context) as client:
		yield BotBackEndClient(settings, client, host, port)


class EagleBotPeer:
	"""A real loopback Eagle client for one server-owned bot."""

	def __init__(self, client):
		self.client = client

	async def recv_packet(self):
		stream = streams.BitStreamIn(await self.client.recv(), ">")
		relay_type = stream.bits(2)
		packet_type = stream.bits(8)
		source_id = stream.bits(NODE_BITS)
		if relay_type == 1:
			stream.bits(NODE_BITS)
		elif relay_type == 2:
			for _ in range(1024):
				stream.bit()
		stream.bytealign()
		return packet_type, source_id, stream

	async def send_packet(self, packet_type, payload=b""):
		stream = streams.BitStreamOut(">")
		stream.bits(0, 2)
		stream.bits(packet_type, 8)
		stream.bits(0, NODE_BITS)
		stream.bytealign()
		stream.write(payload)
		await self.client.send(stream.get())

	async def login(self, token):
		packet_type, _, stream = await self.recv_packet()
		if packet_type != PACKET_ACCEPTED:
			raise RuntimeError("Bot peer did not receive Eagle Accepted")
		node_id = stream.bits(16)
		stream.bits(64)

		handshake = streams.BitStreamOut(">")
		handshake.bits(0, 7)
		handshake.bit(False)
		handshake.bits(0, 8)
		handshake.bits(EAGLE_VERSION, 32)
		handshake.bits(APP_VERSION, 64)
		handshake.bits(DDL_HASH, 32)
		handshake.bits(len(VERSION_STRING), 8)
		handshake.ascii(VERSION_STRING)
		await self.send_packet(PACKET_LOGIN_REQUEST, handshake.get())

		for offset in range(0, len(token), 255):
			fragment = token[offset:offset + 255]
			login = streams.BitStreamOut(">")
			login.bits(1, 7)
			login.bit(offset + len(fragment) == len(token))
			login.bits(len(fragment), 8)
			login.ascii(fragment)
			await self.send_packet(PACKET_LOGIN_REQUEST, login.get())

		packet_type, _, _ = await self.recv_packet()
		if packet_type != PACKET_LOGIN_RESULT:
			raise RuntimeError("Bot peer did not receive Eagle Login Result")
		await self.send_packet(PACKET_CLIENT_READY)
		return node_id

	async def send_pong(self, client_time):
		stream = streams.BitStreamOut(">")
		stream.bits(int(time.monotonic() * 1000), 64)
		stream.bits(client_time, 64)
		await self.send_packet(PACKET_PONG, stream.get())

	async def wait_for_counter(self, index):
		"""Wait for Eagle RPC 16 after incrementing a server counter.

		Retail SMB35 peers do not announce JoinMatch until this acknowledgement
		has arrived. The loop also consumes the node notices sent just after
		ClientReady and services an Eagle ping if one arrives first.
		"""
		with anyio.fail_after(5):
			while True:
				packet_type, source_id, stream = await self.recv_packet()
				if packet_type == PACKET_DISCONNECTED:
					raise RuntimeError("Bot peer disconnected while waiting for counter")
				if packet_type == PACKET_PING:
					await self.send_pong(stream.bits(64))
					continue
				if packet_type != 16 or source_id != 0:
					continue
				stream.bits(64)  # Eagle server timestamp
				if stream.bits(8) == index:
					return

	async def send_rpc(self, targets, rpc_id, payload):
		stream = streams.BitStreamOut(">")
		if not targets:
			stream.bits(0, 2)
		elif len(targets) == 1:
			stream.bits(1, 2)
		else:
			stream.bits(2, 2)
		stream.bits(rpc_id, 8)
		stream.bits(0, NODE_BITS)
		if len(targets) == 1:
			stream.bits(targets[0], NODE_BITS)
		elif len(targets) > 1:
			for node_id in range(1024):
				stream.bit(node_id in targets)
		stream.bytealign()
		stream.bits(int(time.monotonic() * 1000), 64)
		stream.write(payload)
		await self.client.send(stream.get())

	async def run(self):
		while True:
			packet_type, _, stream = await self.recv_packet()
			if packet_type == PACKET_DISCONNECTED:
				return
			if packet_type == PACKET_PING:
				await self.send_pong(stream.bits(64))

	async def close(self):
		await self.client.close()


def build_join_match(bot, requested_level):
	"""Encode Eagle RPC 26 (JoinMatch) for one bot."""

	stream = streams.BitStreamOut(">")
	stream.bits(bot.player_id, 7)
	stream.bits(requested_level, 8)
	stream.bits(DEFAULT_COINS, 32)
	stream.bit(False)   # is Luigi
	stream.bits(bot.principal_id, 64)

	name = bot.name.encode("ascii", "replace")[:PLAYER_NAME_BYTES]
	stream.write(name.ljust(PLAYER_NAME_BYTES, b"\0"))
	stream.bits(DEFAULT_COIN_LEVEL, 16)
	stream.bits(DEFAULT_JOIN_UNKNOWN16, 16)
	stream.bits(0, 2)   # item
	stream.bit(DEFAULT_JOIN_UNKNOWN1)
	return stream.get()


def describe_course(requested_level):
	"""Format a zero-based SMB35 course id as its world-course label."""
	return "%i-%i" %(requested_level // 4 + 1, requested_level % 4 + 1)


def build_input_key(bot, input_masks):
	"""Encode one Eagle RPC 31 (InputKey) covering 30 input frames."""

	if len(input_masks) != 30:
		raise ValueError("SMB35 InputKey packets must contain exactly 30 frames")

	stream = streams.BitStreamOut(">")
	stream.bits(bot.player_id, 7)
	stream.bits(len(input_masks), 5)
	for input_mask in input_masks:
		if not 0 <= input_mask < 64:
			raise ValueError("SMB35 input mask is outside its six-bit range")
		stream.bits(input_mask, 6)
	return stream.get()


def build_increase_server_counter():
	"""Encode Eagle RPC 19 as sent by a newly-ready SMB35 peer.

	The retail second peer increments counter 0 before it sends JoinMatch.  The
	server handles this packet locally; it is not gameplay data for the other
	peers, but the host waits for that setup sequence before starting the round.
	"""

	stream = streams.BitStreamOut(">")
	stream.bits(0, 8)   # counter index
	stream.bits(1, 64)  # increment
	stream.bits(0, 8)   # unknown
	return stream.get()


def build_attack(bot, target_player_id, event_value, variant):
	"""Encode Eagle RPC 33 (Attack).

	``variant`` is the trailing five-bit value observed in retail attack packets.
	It is kept separate from the fixed 32-bit event value so the bot can replay
	only traffic patterns that have already been accepted by a retail client.
	"""

	if not 0 <= target_player_id < PLAYERS_PER_MATCH:
		raise ValueError("Target player id is out of range")
	if not 0 <= event_value < 1 << 32:
		raise ValueError("Attack event value is outside its 32-bit range")
	if not 0 <= variant < 1 << 5:
		raise ValueError("Attack variant is outside its five-bit range")

	stream = streams.BitStreamOut(">")
	stream.bits(bot.player_id, 7)
	stream.bits(1 << target_player_id, PLAYERS_PER_MATCH)
	stream.bits(event_value, 32)
	stream.bits(variant, 5)
	return stream.get()


def parse_attack(payload):
	"""Decode the documented fields of a ten-byte Eagle RPC 33 payload."""

	if len(payload) != 10:
		return None
	stream = streams.BitStreamIn(payload, ">")
	player_id = stream.bits(7)
	targets = stream.bits(PLAYERS_PER_MATCH)
	event_value = stream.bits(32)
	variant = stream.bits(5)
	return player_id, targets, event_value, variant


def build_player_loss(bot, killer_player_id=None):
	"""Encode the captured Dead RPC for an attributed or timeout bot death.

	``killer_player_id=0`` replays the verified player-0-kills-player-1 record.
	It is intentionally restricted to the one-human/one-bot ordering captured in
	the trace. Other player topologies retain an uncredited timeout-style record
	until their Dead fields are independently documented.
	"""

	if killer_player_id == 0 and bot.player_id == 1:
		template = streams.BitStreamIn(KO_DEATH_TEMPLATE, ">")
	else:
		template = streams.BitStreamIn(TIMEOUT_DEATH_TEMPLATE, ">")
	template.bits(7)  # Replace the captured victim id with this bot's id.
	stream = streams.BitStreamOut(">")
	stream.bits(bot.player_id, 7)
	for _ in range(65):
		stream.bit(template.bit())
	return stream.get()


class BotRuntime:
	"""Runs the NEX and Eagle sides of the configured bots for one session."""

	def __init__(self, server, mode, nex_settings, nex_host, nex_port=20000):
		self.server = server
		self.mode = mode
		self.nex_settings = nex_settings
		self.nex_host = nex_host
		self.nex_port = nex_port
		# SMB35 maps Eagle player ids to the ordering in which Eagle nodes join.
		# The bot is placed after the already-ready real nodes.
		self.player_offset = 1
		self.real_player_count = 1
		self.players = []
		# Keep the NEX ordering in logs; it is useful diagnostics but does not
		# assign an Eagle player slot.
		self.round_pid_indexes = []
		# The host does not announce its selected course before this server-side
		# peer must JoinMatch. Pick a fresh course for each bot match unless a
		# specific override is configured for controlled testing.
		self.requested_level = (
			BOT_REQUESTED_LEVEL_OVERRIDE
			if BOT_REQUESTED_LEVEL_OVERRIDE is not None else (
				random.randrange(SMB35_COURSE_COUNT)
				if BOT_RANDOMIZE_REQUESTED_LEVEL else 0
			)
		)
		self.join_sent = False
		# Activation waits for replies from the Eagle server.  It must never run
		# inline with relay_rpc(), or the real host's packet handler is held while
		# the loopback peer waits for its counter acknowledgement.
		self.activation_started = False
		# The Eagle socket has one consumer.  Do not enter its normal receive loop
		# until the activation task has consumed the RPC 16 counter reply.
		self.join_complete = anyio.Event()
		# A real host increments the server counter before the second peer joins
		# the game.  Do not let the loopback peer race the host through that
		# startup boundary: the retail client changes role if it sees JoinMatch
		# first and then behaves as player 1 instead of emitting the host setup.
		self.real_counter_seen = False
		# In a multi-human round the bot must not claim its player slot until the
		# other real Eagle peers have sent JoinMatch. Announcing it immediately
		# after the host counter makes the retail second peer choose the bot's
		# slot, leaving two clients with the same SMB35 player id.
		self.real_join_count = 0
		self.match_started = False
		self.stopped = False
		self.peer_started = False
		self.peers = {}
		self.principal_ids = []
		self.nex_client = None
		self.nex_started = False
		self.nex_error = None
		self.nex_ready = anyio.Event()
		self.nex_stop = anyio.Event()
		self.round_notification = anyio.Event()
		self.notified_round_id = None
		self.round_ready = False
		self.real_eagle_ready = False
		# Each session has an independent, intentionally variable input stream.
		# This is a visible movement baseline, not a level-aware game simulation.
		self.input_rng = random.Random()
		self.health_deadline = None
		self.health_cap = None
		self.lifetime_deadline = None
		self.health_depleted = False
		self.last_attacker_id = None
		# Targets are served in shuffled rounds so a multi-player bot cannot keep
		# selecting one person merely because random choice happened to repeat.
		self.attack_target_queue = []
		self.last_attack_target = None
		self.attack_probe_index = 0
		self.next_attack_probe_at = None
		self.move_packets_until_attack = 0
		self.attack_attempts_remaining = 0
		self.opening_clear_jump_packets = 0
		self.opening_control_complete = True
		self.sent_attack_count = 0

	async def prepare_nex(self):
		"""Create real NEX principals before exposing bots to matchmaking."""

		if self.mode.bot_count != 1:
			raise NotImplementedError("Only the controlled one-bot mode is implemented")
		if not self.nex_started:
			self.nex_started = True
			self.server.task_group.start_soon(self.connect_nex)
		await self.nex_ready.wait()
		if self.nex_error:
			raise RuntimeError("Bot NEX login failed") from self.nex_error
		return list(self.principal_ids)

	async def begin_matchmaking(self, param):
		"""Join through the normal MatchmakeExtension client RPC."""

		await self.nex_ready.wait()
		if self.nex_error:
			raise RuntimeError("Bot NEX client is unavailable") from self.nex_error
		self.server.task_group.start_soon(self.join_matchmaking, param)

	async def join_matchmaking(self, param):
		try:
			gathering = await matchmaking.MatchmakeExtensionClient(
				self.nex_client
			).auto_matchmake_with_param_postpone(param)
			if gathering.id != self.server.session_id:
				raise RuntimeError(
					"Bot joined session %i instead of %i" %(
						gathering.id, self.server.session_id
					)
				)
			logger.info(
				"Bot NEX auto-matchmade session=%i pid=%i",
				self.server.session_id, self.principal_ids[0]
			)
		except Exception:
			logger.exception(
				"Bot NEX auto-matchmake failed session=%i", self.server.session_id
			)

	async def connect_nex(self):
		try:
			await self._connect_nex()
		except Exception as e:
			self.nex_error = e
			logger.exception("Bot NEX client ended session=%i", self.server.session_id)
		finally:
			self.nex_ready.set()

	async def _connect_nex(self):
		handlers = [BotNotificationServer(self), BotMessageDeliveryServer(self)]
		username = "smb35-bot-%i" %self.server.session_id
		async with connect_backend(
			self.nex_settings, self.nex_host, self.nex_port
		) as nex:
			async with nex.login(username, servers=handlers) as client:
				response = await secure.SecureConnectionClient(client).register([])
				response.result.raise_if_error()
				self.nex_client = client
				self.principal_ids = [client.pid()]
				logger.info(
					"Bot NEX registered session=%i pids=%s",
					self.server.session_id, self.principal_ids
				)
				self.nex_ready.set()
				await self.nex_stop.wait()

	def note_round_notification(self, round_id):
		self.notified_round_id = round_id
		self.round_notification.set()
		logger.info(
			"Bot NEX round notification session=%i round=%i",
			self.server.session_id, round_id
		)

	def note_real_eagle_ready(self):
		"""Reserve node 1 for the real host before the bot connects."""

		self.real_eagle_ready = True
		self.start_peer()

	async def prepare_round(self, round_id):
		"""Complete the bot's round-start RMC call before Eagle is allowed."""

		await self.nex_ready.wait()
		if self.nex_error:
			raise RuntimeError("Bot NEX client is unavailable") from self.nex_error
		with anyio.fail_after(5):
			await self.round_notification.wait()
		if self.notified_round_id != round_id:
			raise RuntimeError(
				"Bot received round %s while waiting for round %s" %(
					self.notified_round_id, round_id
				)
			)
		param = await matchmaking.MatchmakeRefereeClient(
			self.nex_client
		).get_start_round_param(round_id)
		self.round_pid_indexes = []
		for principal_id in self.principal_ids:
			if principal_id not in param.pids:
				raise RuntimeError("Bot PID is absent from its StartRoundParam")
			self.round_pid_indexes.append(list(param.pids).index(principal_id))
		if len(set(self.round_pid_indexes)) != len(self.round_pid_indexes):
			raise RuntimeError("Bot StartRoundParam PID indexes are not unique")
		self.round_ready = True
		logger.info(
			"Bot NEX round ready session=%i round=%i pids=%s pid_indexes=%s",
			self.server.session_id, round_id, list(param.pids), self.round_pid_indexes
		)
		self.start_peer()

	def set_player_offset(self, real_player_count):
		"""Reserve lower Eagle player ids for real session participants."""

		if self.join_sent:
			raise RuntimeError("Cannot change bot player ids after JoinMatch")
		if not 1 <= real_player_count < PLAYERS_PER_MATCH:
			raise ValueError("Invalid real player count for bot session")
		if real_player_count + self.mode.bot_count > PLAYERS_PER_MATCH:
			raise ValueError("Too many real players and bots for one SMB35 match")
		self.real_player_count = real_player_count
		self.player_offset = real_player_count

	def add_node(self, node_id, index):
		player_id = self.player_offset + index
		self.players.append(BotPlayer(
			player_id=player_id,
			node_id=node_id,
			principal_id=self.principal_ids[index],
			name="SMB35 Bot %i" %(index + 1)
		))

	def start_peer(self):
		"""Start the one-bot loopback Eagle connection exactly once."""

		if self.peer_started or not self.round_ready or not self.real_eagle_ready:
			return
		self.peer_started = True
		self.server.task_group.start_soon(self.connect_peer, 0)

	async def connect_peer(self, index):
		try:
			await self._connect_peer(index)
		except Exception:
			logger.exception("Bot Eagle peer ended session=%i", self.server.session_id)

	async def _connect_peer(self, index):
		principal_id = self.principal_ids[index]
		# The Eagle listener is bound to the server's LAN address and requires
		# TLS. This is a same-host connection to the server's private certificate.
		context = tls.TLSContext()
		context.disable_verification()
		url = "wss://%s:%i/%i" %(
			config.EAGLE_INTERNAL_HOST, config.EAGLE_PORT, self.server.session_id
		)
		async with websocket.connect(url, context) as client:
			peer = EagleBotPeer(client)
			node_id = await peer.login(self.server.make_bot_token(principal_id))
			bot = BotPlayer(
				player_id=self.player_offset + index,
				node_id=node_id,
				principal_id=principal_id,
				name="SMB35 Bot %i" %(index + 1)
			)
			self.players.append(bot)
			self.peers[node_id] = peer
			await anyio.sleep(EAGLE_PEER_SETTLE_SECONDS)
			self.start_activation()
			await self.join_complete.wait()
			await peer.run()

	def start_activation(self):
		"""Schedule the counter/JoinMatch exchange outside Eagle relay_rpc()."""

		required_real_joins = self.real_player_count - 1
		if (
			self.join_sent or self.activation_started or not self.players
			or not self.real_counter_seen
			or self.real_join_count < required_real_joins
		):
			return
		self.activation_started = True
		self.server.task_group.start_soon(self.activate)

	async def activate(self):
		"""Join after every non-host real Eagle peer has announced itself."""

		required_real_joins = self.real_player_count - 1
		if (
			self.join_sent or not self.players or not self.real_counter_seen
			or self.real_join_count < required_real_joins
		):
			return

		try:
			for bot in self.players:
				# Match the observed startup order for a real second Eagle peer:
				# RPC 19 addressed to the server, its RPC 16 acknowledgement, then
				# the player's JoinMatch.
				peer = self.peers[bot.node_id]
				await peer.send_rpc([0], RPC_INCREASE_SERVER_COUNTER, build_increase_server_counter())
				await peer.wait_for_counter(0)
				logger.info(
					"Bot Eagle counter acknowledged session=%i node=%i",
					self.server.session_id, bot.node_id
				)
				await anyio.sleep(EAGLE_COUNTER_TO_JOIN_SECONDS)
				await peer.send_rpc([ALL_NODES], RPC_JOIN_MATCH, build_join_match(bot, self.requested_level))
			self.join_sent = True
			self.join_complete.set()
			logger.info(
				"Bot runtime joined session=%i bots=%i requested_level=%i course=%s",
				self.server.session_id,
				len(self.players),
				self.requested_level,
				describe_course(self.requested_level)
			)
		except Exception:
			logger.exception("Bot Eagle activation failed session=%i", self.server.session_id)
		finally:
			if not self.join_sent:
				self.activation_started = False

	async def observe_real_rpc(self, rpc_id, payload):
		"""Follow the real client's setup order before emitting bot gameplay."""

		# A working SMB35 host first sends RPC 19. Each other real peer then
		# sends its own counter and JoinMatch. Waiting for those joins prevents
		# the bot from changing a real peer's player-slot selection.
		if rpc_id == RPC_INCREASE_SERVER_COUNTER:
			self.real_counter_seen = True
			self.start_activation()
		elif rpc_id == RPC_JOIN_MATCH and len(payload) >= 2:
			self.real_join_count += 1
			if (
				BOT_REQUESTED_LEVEL_OVERRIDE is None
				and not BOT_RANDOMIZE_REQUESTED_LEVEL
			):
				bits = streams.BitStreamIn(payload, ">")
				bits.bits(7)
				self.requested_level = bits.bits(8)
			self.start_activation()
		elif rpc_id == RPC_START_MATCH and self.join_sent and not self.match_started:
			self.match_started = True
			self.server.task_group.start_soon(self.run)
			logger.info("Bot runtime started session=%i", self.server.session_id)
		elif rpc_id == RPC_ATTACK:
			attack = parse_attack(payload)
			if attack:
				attacker_id, targets, event_value, variant = attack
				bot_ids = {bot.player_id for bot in self.players}
				if any(targets & (1 << bot_id) for bot_id in bot_ids):
					# This is the human who most recently applied the bot's health
					# penalty. In the verified 1v1 layout player 0 can receive the
					# captured KO attribution when that hit ultimately depletes health.
					self.last_attacker_id = attacker_id
					self.adjust_health(
						-HEALTH_SECONDS_PER_RECEIVED_ATTACK,
						"received attack from player %i (event=%i variant=%i)" %(
							attacker_id, event_value, variant
						)
					)

	async def run(self):
		"""Move in bursts and send either discovery probes or normal attacks."""

		self.start_health()
		if ATTACK_PROBE_EVENT_VALUES:
			self.next_attack_probe_at = time.monotonic()
			logger.info(
				"Bot attack probe started session=%i events=%s variant=%i interval=%.1f",
				self.server.session_id, ATTACK_PROBE_EVENT_VALUES,
				ATTACK_PROBE_VARIANT, ATTACK_PROBE_INTERVAL_SECONDS
			)
		while not self.stopped:
			probe_active = self.attack_probe_index < len(ATTACK_PROBE_EVENT_VALUES)
			# Do not terminate a discovery run halfway through. start_health() resets
			# the normal synthetic-health clock once the fixed probe sequence ends.
			if not probe_active:
				await self.check_health()
			if self.health_depleted:
				await anyio.sleep(self.mode.input_interval)
				continue
			for bot in self.players:
				await self.peers[bot.node_id].send_rpc(
					[ALL_NODES], RPC_INPUT_KEY,
					build_input_key(bot, self.next_input_frames())
				)
				if probe_active:
					if time.monotonic() >= self.next_attack_probe_at:
						event_value = ATTACK_PROBE_EVENT_VALUES[self.attack_probe_index]
						await self.send_attack(
							bot, event_value, ATTACK_PROBE_VARIANT,
							"probe event %i (%i/%i)" %(
								event_value, self.attack_probe_index + 1,
								len(ATTACK_PROBE_EVENT_VALUES)
							)
						)
						self.attack_probe_index += 1
						self.next_attack_probe_at = (
							time.monotonic() + ATTACK_PROBE_INTERVAL_SECONDS
						)
						if self.attack_probe_index == len(ATTACK_PROBE_EVENT_VALUES):
							self.start_health()
							logger.info(
								"Bot attack probe complete session=%i; normal cadence resumed",
								self.server.session_id
							)
					continue
				if not self.opening_control_complete:
					continue
				if self.move_packets_until_attack:
					self.move_packets_until_attack -= 1
					if not self.move_packets_until_attack:
						self.attack_attempts_remaining = self.input_rng.choices(
							ATTACK_BURST_SIZES, weights=ATTACK_BURST_WEIGHTS
						)[0]
						logger.info(
							"Bot attack window session=%i attempts=%i",
							self.server.session_id, self.attack_attempts_remaining
						)
				elif self.attack_attempts_remaining:
					await self.send_attack(bot)
					self.attack_attempts_remaining -= 1
					if not self.attack_attempts_remaining:
						self.move_packets_until_attack = self.input_rng.randint(
							MOVE_BURST_MIN_INPUT_PACKETS,
							MOVE_BURST_MAX_INPUT_PACKETS
						)
			await anyio.sleep(self.mode.input_interval)

	def start_health(self):
		player_band = HEALTH_SECONDS_BY_REAL_PLAYER_COUNT[
			min(self.real_player_count, 3)
		]
		initial_health = self.input_rng.uniform(*player_band)
		lifetime = self.input_rng.uniform(LIFETIME_MIN_SECONDS, LIFETIME_MAX_SECONDS)
		self.health_deadline = time.monotonic() + initial_health
		self.health_cap = player_band[1]
		self.lifetime_deadline = time.monotonic() + lifetime
		self.health_depleted = False
		self.last_attacker_id = None
		self.move_packets_until_attack = self.input_rng.randint(
			MOVE_BURST_MIN_INPUT_PACKETS, MOVE_BURST_MAX_INPUT_PACKETS
		)
		self.attack_attempts_remaining = 0
		self.opening_clear_jump_packets = 0
		self.opening_control_complete = True
		self.sent_attack_count = 0
		self.attack_target_queue = []
		self.last_attack_target = None
		logger.info(
			"Bot health started session=%i players=%i seconds=%.1f cap=%.1f master_ttl=%.1f",
			self.server.session_id, self.real_player_count, initial_health,
			self.health_cap, lifetime
		)

	def remaining_health(self):
		if self.health_deadline is None:
			return None
		return max(0.0, self.health_deadline - time.monotonic())

	def remaining_lifetime(self):
		if self.lifetime_deadline is None:
			return None
		return max(0.0, self.lifetime_deadline - time.monotonic())

	def adjust_health(self, seconds, reason):
		if self.health_deadline is None or self.health_depleted:
			return
		remaining = min(
			self.health_cap, max(0.0, self.remaining_health() + seconds)
		)
		self.health_deadline = time.monotonic() + remaining
		logger.info(
			"Bot health session=%i delta=%+.1f remaining=%.1f reason=%s",
			self.server.session_id, seconds, remaining, reason
		)

	async def check_health(self):
		if BOT_NEVER_RETIRE:
			return
		if self.health_depleted:
			return
		if self.remaining_lifetime() <= 0:
			reason = "master lifetime expired"
			killer_player_id = None
		elif self.remaining_health() <= 0:
			reason = "health depleted"
			killer_player_id = self.last_attacker_id
		else:
			return
		self.health_depleted = True
		# Eagle's player-loss RPC is peer gameplay state, not a server-authoritative
		# elimination command.  The retail client ignored even a byte-identical
		# captured loss record when this synthetic bot sent it.  Retire the bot as
		# an Eagle participant instead: closing the socket makes EagleServer remove
		# its node and broadcast NodeRemoved to the real client, so it cannot remain
		# a live participant and take first place after its synthetic health expires.
		self.stopped = True
		logger.info(
			"Bot health depleted session=%i reason=%s killer_player=%s; "
			"closing %i Eagle peer(s) for NodeRemoved",
			self.server.session_id, reason, killer_player_id, len(self.players)
		)
		for bot in self.players:
			await self.peers[bot.node_id].close()

	def next_attack_target(self, bot):
		"""Return one real player from a shuffled, no-repeat target round."""

		targets = sorted(
			node_id - 1
			for node_id, client in self.server.clients.items()
			if node_id != bot.node_id
			and not getattr(client, "is_server_bot", False)
			and not getattr(client, "is_virtual", False)
		)
		if not targets:
			return None

		target_set = set(targets)
		self.attack_target_queue = [
			target for target in self.attack_target_queue if target in target_set
		]
		if not self.attack_target_queue:
			self.attack_target_queue = list(targets)
			self.input_rng.shuffle(self.attack_target_queue)
			if (
				len(self.attack_target_queue) > 1
				and self.attack_target_queue[0] == self.last_attack_target
			):
				self.attack_target_queue[0], self.attack_target_queue[1] = (
					self.attack_target_queue[1], self.attack_target_queue[0]
				)

		target = self.attack_target_queue.pop(0)
		self.last_attack_target = target
		return target

	async def send_attack(self, bot, event_value=None, variant=None, attack_name=None):
		"""Send an attack pattern to the next real-player target."""

		target = self.next_attack_target(bot)
		if target is None:
			return
		self.sent_attack_count += 1
		if event_value is None:
			event_value, variant, attack_name, _percentage = self.input_rng.choices(
				ATTACK_ENEMY_WEIGHTS, weights=ATTACK_ENEMY_SELECTION_WEIGHTS
			)[0]
		await self.peers[bot.node_id].send_rpc(
			[ALL_NODES], RPC_ATTACK,
			build_attack(bot, target, event_value, variant)
		)
		self.adjust_health(
			HEALTH_SECONDS_PER_SENT_ATTACK,
			"sent %s attack to player %i (event=%i variant=%i count=%i)" %(
				attack_name, target, event_value, variant, self.sent_attack_count
			)
		)

	def next_input_frames(self):
		"""Return continuous left-walk input with occasional randomized jump pulses."""

		frames = [INPUT_LEFT] * 30
		if self.input_rng.random() < LEFT_JUMP_CHANCE_PER_PACKET:
			held_frames = self.input_rng.randint(
				LEFT_JUMP_MIN_HELD_FRAMES,
				LEFT_JUMP_MAX_HELD_FRAMES
			)
			start = self.input_rng.randint(0, len(frames) - held_frames)
			for index in range(start, start + held_frames):
				frames[index] |= INPUT_JUMP
		return frames

	async def stop(self):
		self.stopped = True
		self.nex_stop.set()
		for peer in list(self.peers.values()):
			await peer.close()
