"""Decode sample_events.hex with msgspec exactly as the Python consumer does (array_like StreamEvent)."""

import sys
from pathlib import Path

import msgspec


class StreamEvent(msgspec.Struct, array_like=True):
    event_id: str
    user_id: str
    session_id: str
    type: str
    client_ts: int
    props: dict[str, str | int | float | bool] = {}


dec = msgspec.msgpack.Decoder(StreamEvent)
path = Path(sys.argv[1])
for line in path.read_text().split():
    ev = dec.decode(bytes.fromhex(line))
    # re-encoding the astuple must give back the identical bytes (msgspec's own encoding of the same event)
    same = msgspec.msgpack.encode(msgspec.structs.astuple(ev)) == bytes.fromhex(line)
    print(ev, "reencode_identical=", same)
