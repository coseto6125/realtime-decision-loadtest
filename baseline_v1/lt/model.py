"""lt-model: System One mock. Jev-style shape in (state, question, options), probs and latency_ms out.

Probabilities are a deterministic hash of the request, so runs are reproducible. Latency is lognormal
(median 40 ms, p95 120 ms). TIMEOUT_RATE and ERROR_RATE come from env or POST /admin/faults.
"""

import asyncio
import random

import msgspec
from sanic import HTTPResponse, Request

from lt.common import envf, json_resp, make_app, serve
from lt.logic import JudgeIn, JudgeOut, mock_latency_ms, mock_probs


class Faults(msgspec.Struct):
    timeout_rate: float = 0.0
    error_rate: float = 0.0
    hang_s: float = 2.0


app, m = make_app("lt-model")
FAULTS = Faults(timeout_rate=envf("TIMEOUT_RATE", 0.0), error_rate=envf("ERROR_RATE", 0.0))
_in_dec = msgspec.json.Decoder(JudgeIn)
_faults_dec = msgspec.json.Decoder(Faults)
_enc = msgspec.json.Encoder()
_rng = random.Random()


@app.post("/judge")
async def judge(request: Request) -> HTTPResponse:
    try:
        req = _in_dec.decode(request.body)
    except msgspec.DecodeError as e:
        return json_resp({"error": str(e)}, 400)
    if not req.options:
        return json_resp({"error": "options must not be empty"}, 400)
    u = _rng.random()
    if u < FAULTS.error_rate:
        m.inc("injected_error")
        return json_resp({"error": "injected"}, 500)
    if u < FAULTS.error_rate + FAULTS.timeout_rate:
        m.inc("injected_timeout")
        await asyncio.sleep(FAULTS.hang_s)
    lat = mock_latency_ms(_rng.gauss())
    await asyncio.sleep(lat / 1000)
    m.inc("served")
    m.hists["mock_latency_drawn_ms"].observe(lat)
    return HTTPResponse(_enc.encode(JudgeOut(probs=mock_probs(request.body, len(req.options)),
                                             latency_ms=round(lat, 1))), content_type="application/json")


@app.post("/admin/faults")
async def set_faults(request: Request) -> HTTPResponse:
    global FAULTS
    try:
        FAULTS = _faults_dec.decode(request.body)
    except msgspec.DecodeError as e:
        return json_resp({"error": str(e)}, 400)
    return json_resp(FAULTS)


@app.get("/admin/faults")
async def get_faults(_: Request) -> HTTPResponse:
    return json_resp(FAULTS)


if __name__ == "__main__":
    serve(app)
