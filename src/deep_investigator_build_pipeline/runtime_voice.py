"""Runtime voice operations layer.

Serves a browser console (mic + speaker) over WebSocket + HTTP on 8767.

Pipeline:
  browser mic -> WS (PCM/WAV) -> faster-whisper STT -> Operator Agent (GLM,
  sees the live cortex snapshot + recent narration + control tools) -> reply
  -> edge-tts -> stream audio back; the browser plays it while it generates.

Narration (proactive) and operator Q&A share one speech queue so narration
yields to the human the moment they speak.
"""
from __future__ import annotations

import argparse
import asyncio
import io
import json
import os
import queue
import tempfile
import threading
import time
import wave
from pathlib import Path

import edge_tts
import websockets

from deep_investigator_build_pipeline import runtime_cortex as cortex
from deep_investigator_build_pipeline import runtime_controls as controls
from deep_investigator_build_pipeline import runtime_narrator as narrator_mod

PORT = int(os.getenv("VOICE_HTTP_PORT", "8767"))
WS_PORT = int(os.getenv("VOICE_WS_PORT", "8768"))
MODEL = os.getenv("RUNTIME_VOICE_MODEL", "zai-org/GLM-5.3-Flash")
API_BASE = os.getenv("RUNTIME_VOICE_BASE", os.getenv("OPENAI_API_BASE", "https://api.featherless.ai/v1"))
API_KEY = os.getenv("RUNTIME_VOICE_KEY", os.getenv("OPENAI_API_KEY", ""))
VOICE = os.getenv("RUNTIME_VOICE_TTS_VOICE", "en-US-ChristopherNeural")
STT_MODEL = os.getenv("RUNTIME_STT_MODEL", "base.en")

OPERATOR_SYSTEM = (
    "You are the operator's voice assistant for a live autonomous research-and-build "
    "pipeline. You speak in a friendly, concise, human way. Use the LIVE STATE and "
    "recent narration below to ground every answer. You can answer questions about "
    "progress, evidence, blockers, phase, and artifacts. You can also act on the "
    "pipeline using the control commands available to you.\n"
    "Action model: directives for the NEXT floor (e.g. 'make sure X happens next') "
    "are applied immediately and are not destructive. Destructive actions — halt, "
    "restart, reverse, rollback a file, edit the prompt — are handled by a structured "
    "confirm flow: propose briefly, ask the operator to say 'confirm', then report the "
    "result. Never say you applied a destructive command yourself; just report what the "
    "system will do. Every intervention is recorded automatically.\n"
    "Keep replies short (under 90 words) and conversational. No markdown."
)

# Structured-confirm state: a proposed destructive action waits for a verbal confirm.
_pending: dict[str, Any] = {"action": None, "detail": {}, "ts": 0.0}
_CONFIRM_WINDOW = float(os.getenv("RUNTIME_CONTROL_CONFIRM_SECONDS", "75"))
_PENDING_RESET: dict[str, Any] = {"action": None, "detail": {}, "ts": 0.0}


def _transcript(role: str, text: str) -> None:
    """Transcript-first: everything spoken (and heard) is logged before TTS."""
    try:
        d = cortex.STATE_DIR
        d.mkdir(parents=True, exist_ok=True)
        rec = {"role": role, "text": text, "t": time.time()}
        with (d / "transcript.jsonl").open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _audit(kind: str, detail: dict[str, Any]) -> None:
    """Audited intervention: append into the gateway event stream."""
    try:
        d = cortex.STATE_DIR
        d.mkdir(parents=True, exist_ok=True)
        rec = {"kind": "owner_intervention", "type": kind, "ts": time.time(),
               "channel": "voice", **detail}
        with (d / "events.jsonl").open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _selected_context(out: dict[str, Any]) -> str:
    """Context for the 'explain this component' question: the beacon selection."""
    try:
        sel = Path(cortex.ROOT_DIR) / ".beacon/selected.json"
        if not sel.exists():
            return ""
        data = json.loads(sel.read_text(encoding="utf-8"))
        name = data.get("name") or ""
        if not name or time.time() - data.get("ts", 0) > 60:
            return ""
        for e in out.get("entities", []):
            if e["name"].startswith(name) or name.startswith(e["name"]):
                return (f"selected component '{name}': materialized {e['M']:.2%}, "
                        f"evidence {e['E']:.0%}, conflict {e['K']:.0f}, freshness {e['F']:.2%}")
    except Exception:
        pass
    return ""

# faster-whisper model (lazy, CPU).
_stt_model = None
_stt_lock = threading.Lock()


def _get_stt():
    global _stt_model
    if _stt_model is not None:
        return _stt_model
    with _stt_lock:
        if _stt_model is None:
            from faster_whisper import WhisperModel
            _stt_model = WhisperModel(STT_MODEL, device="cpu", compute_type="int8")
    return _stt_model


def _llm(prompt: str, system: str, max_tokens: int = 220, temperature: float = 0.7) -> str:
    if not API_KEY:
        return "I cannot reach the language model right now."
    body = json.dumps({
        "model": MODEL,
        "messages": [{"role": "system", "content": system},
                     {"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "temperature": temperature,
    }).encode("utf-8")
    import urllib.request
    req = urllib.request.Request(
        f"{API_BASE}/chat/completions", data=body,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {API_KEY}"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        data = json.loads(r.read().decode("utf-8"))
    return data["choices"][0]["message"]["content"] or ""


def transcribe_wav(data: bytes) -> str:
    """Decode arbitrary audio bytes (opencore/media bytes) to WAV PCM and transcribe.

    The browser sends WebM/Opus (MediaRecorder). faster-whisper requires a
    decodable audio stream, so we decode to 16-bit 16 kHz mono WAV via ffmpeg
    when the input is not already a riff/wav file.
    """
    import subprocess

    model = _get_stt()
    is_wav = data[:4] == b"RIFF" and data[8:12] == b"WAVE"
    if is_wav:
        stream = io.BytesIO(data)
    else:
        proc = subprocess.run(
            ["ffmpeg", "-hide_banner", "-loglevel", "error",
             "-i", "pipe:0", "-f", "wav", "-ar", "16000", "-ac", "1",
             "-c:a", "pcm_s16le", "pipe:1"],
            input=data, capture_output=True, timeout=30,
        )
        if proc.returncode != 0:
            raise RuntimeError(f"ffmpeg decode failed: {proc.stderr[:200]!r}")
        stream = io.BytesIO(proc.stdout)
    segments, _ = model.transcribe(stream, language="en")
    return " ".join(s.text for s in segments).strip()


async def _tts_mp3(text: str) -> bytes | None:
    try:
        communicate = edge_tts.Communicate(text, VOICE)
        buf = io.BytesIO()
        async for chunk in communicate.stream():
            if chunk["type"] == "audio":
                buf.write(chunk["data"])
        data = buf.getvalue()
        return data if data else None
    except Exception:
        return None


# Shared speech queue: priority {0: narration, 1: operator reply}
_speech_q: "queue.PriorityQueue[tuple[int, str]]" = queue.PriorityQueue()
_operator_typing = threading.Event()


def narrate_callback(line: str) -> None:
    _transcript("narrator", line)
    _speech_q.put((0, line))


def _operator_turn(user_text: str, ws_context: dict) -> str:
    out = cortex.snapshot(force=False)
    live = cortex.describe(out)
    ev = out.get("recent_errors", [])
    recent = "\n".join(ev[-3:]) or "none"
    sel = _selected_context(out)
    prompt = (
        f"LIVE STATE: {live}\n"
        f"recent errors: {recent}\n"
        f"directive pending: {json.dumps(controls.read_control().get('directive'))}\n"
        + (f"selected component: {sel}\n" if sel else "")
        + f"\nOperator said: {user_text}\n"
        "Respond as the voice operator. If they request a control action, "
        "propose it and ask for confirmation. If they give an instruction for "
        "the next floor, confirm you will apply it."
    )
    reply = _llm(prompt, OPERATOR_SYSTEM)
    return _apply_intent(reply, user_text)


def _match_command(text: str) -> tuple[str | None, dict[str, Any]]:
    """Deterministic structured-intent extraction (before any LLM reply)."""
    import re
    low = text.lower()
    if any(k in low for k in ("halt", "stop the run", "stop it now", "abort the run", "kill the run")):
        return "halt", {}
    if re.search(r"\b(restart|reboot)\b", low):
        return "restart", {}
    if re.search(r"\b(reverse|back to floor|checkpoint)\b", low):
        m = re.search(r"(?:floor|step|checkpoint)\s*(\d+)", low)
        return "reverse", {"floor": m.group(1) if m else None}
    if re.search(r"\b(rollback|undo|roll back)\b", low):
        m = re.search(r"([\w./-]+\.(?:py|json|toml|md|txt|yaml|yml))", low)
        return "rollback", {"file": m.group(1) if m else None}
    if re.search(r"\b(edit|change|rewrite|update) the? prompt\b", low):
        rest = re.sub(r"^(edit|change|rewrite|update)\s+(the\s+)?prompt\s*[:,]?\s*", text, flags=re.I).strip()
        return "edit_prompt", {"prompt": rest or text}
    return None, {}


def _execute(act: str, det: dict[str, Any]) -> str:
    result: dict[str, Any]
    if act == "halt":
        result = controls.command_halt()
    elif act == "restart":
        result = controls.command_restart()
    elif act == "reverse":
        result = controls.command_reverse(det.get("floor"))
    elif act == "rollback":
        result = controls.rollback_file(det.get("file"))
    elif act == "edit_prompt":
        result = controls.command_edit_prompt(det.get("prompt"))
    else:
        result = {"type": "error", "text": f"Unknown action {act}."}
    _audit("applied", {"action": act, **det, "result": str(result.get("text", ""))[:120]})
    return result.get("text", f"{act} applied.")


def _apply_intent(reply: str, user_text: str) -> str:
    """Structured command pipeline: intent -> confirm-if-destructive -> audited apply."""
    import re
    low = user_text.lower()
    now = time.time()

    if _pending["action"]:
        if now - _pending["ts"] > _CONFIRM_WINDOW:
            _pending.update(_PENDING_RESET)
        elif any(k in low for k in ("confirm", "confirmed", "go ahead", "do it", "approved")):
            act, det = _pending["action"], _pending["detail"]
            _pending.update(_PENDING_RESET)
            return _execute(act, det)
        elif any(k in low for k in ("cancel", "never mind", "forget it", "don't", "no don't")):
            _pending.update(_PENDING_RESET)
            return "Cancelled — nothing was changed."

    act, det = _match_command(user_text)
    if act:
        _pending.update({"action": act, "detail": det, "ts": now})
        _audit("proposed", {"action": act, **det})
        target = ""
        if det.get("floor"):
            target = f" (back to floor {det['floor']})"
        elif det.get("file"):
            target = f" (file {det['file']})"
        return f"You're asking to {act.replace('_', ' ')}{target} — this changes the run. Say confirm to apply it."

    # Directive injection: non-destructive -> immediate + audited.
    if any(k in low for k in ("add", "inject", "make sure", "tell the next", "next floor",
                              "please next", "from now on", "please ensure", "for the next")):
        candidates = [s for s in reply.split("\n") if s.strip()]
        directive = candidates[-1] if candidates else user_text
        controls.set_directive(directive)
        _audit("directive", {"text": directive[:200]})
        return f"Understood — I'll pass that to the next floor: “{directive}”"
    return reply


# ---------------------------------------------------------------------------

async def _handle_ws(websocket):
    try:
        # Initial greeting (operator joins).
        await websocket.send(json.dumps({"type": "speak", "text": "Operator online. I can see the pipeline live and ready."}))
        while True:
            try:
                msg = await asyncio.wait_for(websocket.recv(), timeout=20)
            except asyncio.TimeoutError:
                await websocket.send(json.dumps({"type": "ping"}))
                continue
            data = json.loads(msg)
            if data.get("type") == "audio":
                # Base64 (possibly a data URL) audio -> decode -> transcribe
                import base64
                payload = data["data"]
                if payload.startswith("data:"):
                    payload = payload.split(",", 1)[1]
                raw = base64.b64decode(payload)
                try:
                    text = await asyncio.to_thread(transcribe_wav, raw)
                except Exception as e:
                    text = ""
                if not text.strip():
                    continue
                _operator_typing.set()
                try:
                    reply = await asyncio.to_thread(_operator_turn, text, {})
                finally:
                    _operator_typing.clear()
                _transcript("operator", reply)
                mp3 = await _tts_mp3(reply)
                if mp3:
                    await websocket.send(json.dumps({"type": "audio", "data": base64.b64encode(mp3).decode()}))
                else:
                    await websocket.send(json.dumps({"type": "speak", "text": reply}))
    except Exception:
        pass


def _speech_loop():
    """Dequeue narration + operator replies and enqueue TTS frames to a sink."""
    while True:
        prio, text = _speech_q.get()
        # produced by narrate_callback or operator; here we just run TTS async
        if _operator_typing.is_set() and prio == 0:
            # yield to operator; drop/skip narration while operator speaks
            _speech_q.task_done()
            continue
        async def _go(t):
            mp3 = await _tts_mp3(t)
            return mp3
        try:
            mp3 = asyncio.run(_go(text))
        except Exception:
            mp3 = None
        if mp3:
            # push to all clients via a broadcast
            _broadcast(mp3)
        _speech_q.task_done()


_clients = set()
_client_lock = threading.Lock()


def _broadcast(mp3: bytes):
    import base64
    data = base64.b64encode(mp3).decode()
    loop = _loop
    with _client_lock:
        for ws in list(_clients):
            try:
                if loop is None or loop.is_closed():
                    continue
                async def _send(ws=ws):
                    try:
                        await ws.send(json.dumps({"type": "audio", "data": data}))
                    except Exception:
                        _clients.discard(ws)
                asyncio.run_coroutine_threadsafe(_send(), loop)
            except Exception:
                pass


_loop: asyncio.AbstractEventLoop | None = None


async def _ws_handler(ws):
    _clients.add(ws)
    try:
        await _handle_ws(ws)
    finally:
        _clients.discard(ws)


def _http_job() -> None:
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    import os

    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path.split("?", 1)[0] in ("/", "/console"):
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(_CONSOLE_PAGE.replace("PLACEHOLDER_WS", str(WS_PORT)).encode())
            else:
                self.send_response(404); self.end_headers()
        def log_message(self, *a): pass

    srv = ThreadingHTTPServer(("127.0.0.1", PORT), H)
    print(f"Voice console: http://127.0.0.1:{PORT}  (WebSocket: ws://127.0.0.1:{WS_PORT})", flush=True)
    srv.serve_forever()


_CONSOLE_PAGE = """<!doctype html><html><head><meta charset="utf-8"><title>Voice Ops</title>
<style>body{background:#070a10;color:#dbe4f0;font-family:ui-monospace,monospace;padding:24px}
button{background:#1a2440;color:#cfe4ff;border:1px solid #2a3a66;padding:10px 16px;border-radius:8px;cursor:pointer;margin-right:8px}
#log{white-space:pre-wrap;font-size:13px;color:#9fb2c8;max-height:50vh;overflow:auto;border-top:1px solid #1c2440;padding-top:10px;margin-top:16px}
h1{font-size:15px;letter-spacing:2px;color:#8fd0ff}</style></head><body>
<h1>■ VOICE OPERATIONS</h1>
<button id="mic">Enable Mic</button><button id="stop">Stop</button>
<p style="font-size:12px;color:#7b8aa0">Speak to query or command the pipeline. Hold to talk, release to send. Destructive actions ask you to confirm.</p>
<div id="log">Waiting…</div>
<audio id="ao" autoplay controls style="width:100%;margin-top:14px"></audio>
<script>
const log=document.getElementById('log');const ao=document.getElementById('ao');
const ws=new WebSocket('ws://'+location.hostname+':'+PLACEHOLDER_WS);
let mediaRec=null;let audioCtx=null;let recBlobs=[];
function say(t){log.textContent+='\\n[you] '+t}
function speak(t){log.textContent+='\\n[ops] '+t;
  ws.send(JSON.stringify({type:'speak_out',text:t}));}
ws.onmessage=(ev)=>{const m=JSON.parse(ev.data);
  if(m.type==='audio'){const blob=new Blob([Uint8Array.from(atob(m.data),c=>c.charCodeAt(0))],{type:'audio/mpeg'});
    ao.src=URL.createObjectURL(blob);ao.play();}
  else if(m.type==='speak'){log.textContent+='\\n[ops] '+m.text;}
  else if(m.type==='ping'){}};
async function enableMic(){
  const stream=await navigator.mediaDevices.getUserMedia({audio:true});
  audioCtx=new AudioContext();const src=audioCtx.createMediaStreamSource(stream);
  const dest=audioCtx.createMediaStreamDestination();src.connect(dest);
  mediaRec=new MediaRecorder(dest.stream);
  mediaRec.ondataavailable=e=>recBlobs.push(e.data);
  mediaRec.onstop=()=>{const blob=new Blob(recBlobs,{type:'audio/webm'});
    recBlobs=[];const fr=new FileReader();
    fr.onload=()=>ws.send(JSON.stringify({type:'audio',data:fr.result}));
    fr.readAsDataURL(blob);};
  const down=document.getElementById('mic');
  // push to talk
  down.addEventListener('mousedown',()=>{recBlobs=[];mediaRec.start();});
  down.addEventListener('mouseup',()=>{mediaRec.stop();});
  down.textContent='Hold to talk';
}
document.getElementById('mic').onclick=enableMic;
document.getElementById('stop').onclick=()=>{if(mediaRec)mediaRec.stop();};
</script></body></html>"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.getcwd())
    args = ap.parse_args()
    root = Path(args.root)
    if root != Path.cwd():
        os.chdir(root)
    # point cortex/controls at the right paths
    cortex.ROOT_DIR = root
    cortex.STATE_DIR = Path(os.getenv("DUAL_LOBE_STATE_DIR", root / ".dual_lobe"))
    cortex.RUN_LOG = Path(os.getenv("RUNTIME_RUN_LOG", root / "output/run.log"))
    controls.ROOT = root

    global _loop
    _loop = asyncio.new_event_loop()

    # narrator thread -> proactive speech
    stop = threading.Event()
    narrator = narrator_mod.Narrator(narrate_callback)
    threading.Thread(target=narrator.run, args=(stop,), daemon=True).start()

    # speech/tts loop thread
    threading.Thread(target=_speech_loop, daemon=True).start()

    # http console
    threading.Thread(target=_http_job, daemon=True).start()

    # websocket server
    async def serve():
        async with websockets.serve(_ws_handler, "127.0.0.1", WS_PORT):
            await asyncio.Future()  # run forever
    try:
        _loop.run_until_complete(serve())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
