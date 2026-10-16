
INTEGER_SETTINGS1 = {
	0: 60,
	1: 30,
	2: 30,
	3: 1,
	4: 0,
	5: 0,
	6: 0,
	7: 0,
	8: 0,
	9: 0,
	10: 5,
	11: 3,
	12: 1,
	13: 30,
	14: 30,
	15: 180,
	16: 0
}

# What integers should we use here?
INTEGER_SETTINGS2 = {}

# Diagnostic-only Eagle relay logging. Leave this disabled during ordinary play.
# When enabled, the server logs the first bytes of each client-to-client RPC;
# this is enough to identify the packet shape without filling the journal.
EAGLE_TRACE = False
EAGLE_TRACE_HEX_BYTES = 96

# Eagle network settings. The defaults are for a server and bot running on the
# same machine. Deployments should keep their host-specific values in the
# ignored local_config.py file, not in version control.
EAGLE_INTERNAL_HOST = "127.0.0.1"
EAGLE_PUBLIC_HOST = "localhost"
EAGLE_PORT = 20001

try:
	from local_config import *
except ModuleNotFoundError:
	pass
