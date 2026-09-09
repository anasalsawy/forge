"""Runtime Beacon - a live visual state reflector for the build pipeline.

Not a decorative toy. The beacon renders the *project's own subject* as it is
actually being built:

- Research phase  -> an unbuilt outline + a lattice of knowledge nodes (leads,
  confirmed facts) as glowing dots, colored cool cyan.
- Build phase     -> the subject's parts are FILLED IN in the exact proportion
  of real build artifacts on disk (warm amber).
- Sharpness       -> solid, validated work reads sharp / clean-edged.
- Fuzz / cracks   -> material defects (Lobe-B HIGH/CRITICAL severity, analyst
  CORRECTION REQUIRED) blur the affected region.
- Glitch squalls  -> blockers / credit errors produce static bands.
- A caption strip  always carries the *raw truth* (numbers) so the art can
  never outrun reality.

Cadence is fixed (default 30s) with a local procedural fallback render so the
feed never goes stale even if the image API is slow or unavailable.

Server: port 8766 serves the live page (latest.png + a filmstrip + state.json).
"""
from __future__ import annotations

import argparse
import json
import os
import threading
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path

try:
    from PIL import Image, ImageDraw, ImageFont
    HAVE_PIL = True
except Exception:
    HAVE_PIL = False

from deep_investigator_build_pipeline import runtime_cortex as cortex

PORT = int(os.getenv("RUNTIME_BEACON_PORT", "8766"))
INTERVAL = float(os.getenv("RUNTIME_BEACON_INTERVAL", "30"))
TRUTH_INTERVAL = float(os.getenv("RUNTIME_BEACON_TRUTH_INTERVAL", "10"))
PROVIDER = os.getenv("RUNTIME_BEACON_PROVIDER", "pollinations")
MODEL = os.getenv("RUNTIME_BEACON_MODEL", "zai-org/GLM-5.3-Flash")
FRAMES_DIR = Path(os.getenv("RUNTIME_BEACON_FRAMES", ".beacon/frames"))
STATE_JSON = Path(os.getenv("RUNTIME_BEACON_STATE", ".beacon/state.json"))
LATEST = Path(os.getenv("RUNTIME_BEACON_LATEST", ".beacon/latest.png"))
SELECTED_JSON = Path(os.getenv("RUNTIME_BEACON_SELECTED", ".beacon/selected.json"))
REVISIONS_DIR = Path(os.getenv("RUNTIME_BEACON_REVISIONS", ".beacon/revisions"))
GEN_THRESHOLD = float(os.getenv("RUNTIME_BEACON_GEN_DELTA", "0.15"))
GEN_MIN_INTERVAL = float(os.getenv("RUNTIME_BEACON_GEN_MIN_SECONDS", "20"))
GEN_MAX_AGE = float(os.getenv("RUNTIME_BEACON_GEN_MAX_AGE_SECONDS", "180"))
API_BASE = os.getenv("RUNTIME_BEACON_BASE", os.getenv("OPENAI_API_BASE", "https://api.featherless.ai/v1"))
API_KEY = os.getenv("RUNTIME_BEACON_KEY", os.getenv("OPENAI_API_KEY", ""))

FRAMES_DIR.mkdir(parents=True, exist_ok=True)
STATE_JSON.parent.mkdir(parents=True, exist_ok=True)
REVISIONS_DIR.mkdir(parents=True, exist_ok=True)

_lock = threading.Lock()
_last_caption = "Pipeline warming up..."


def _llm(prompt: str, max_tokens: int = 300, temperature: float = 1.0) -> str:
    """Tiny scene-director LLM call (Featherless GLM). Fail-open -> empty."""
    if not API_KEY:
        return ""
    body = json.dumps({
        "model": MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "temperature": temperature,
    }).encode("utf-8")
    try:
        req = urllib.request.Request(
            f"{API_BASE}/chat/completions", data=body,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {API_KEY}"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=45) as r:
            data = json.loads(r.read().decode("utf-8"))
        return data["choices"][0]["message"]["content"] or ""
    except Exception:
        return ""


def _director_prompt(out: dict[str, Any], root: Path) -> str:
    """Translate live state into an artistic prompt for the subject.

    Returns a tuple implicitly via ':=' : (prompt, caption).
    """
    subject = _subject_from_state(out, root)
    phase = out.get("phase") or "research"
    sev = out.get("severity") or "LOW"
    calls = out.get("worker_calls", 0)
    findings = out.get("n_findings", 0)
    blocks = out.get("n_blockers", 0)
    credit = out.get("credit_blocked", False)
    artifacts = out.get("artifacts_count", 0)
    tops = ", ".join(out.get("artifacts_tops", [])) or "nothing yet"
    m_to = int(round((out.get("implementation_completeness") or 0.0) * 100))
    e_to = int(round((out.get("evidence_confidence") or 0.0) * 100))
    k_n = out.get("n_conflicts", 0)

    lane = "cool cyan, blueprint energy" if phase == "research" else "warm amber, illuminated construction"
    crisp = "sharp, crisp, clean vector edges" if sev in ("LOW", "MEDIUM") else "partly blurred, hairline cracks, desaturated near the affected region"
    squall = ", scattered static glitch bands and a storm squall across the sky" if (credit or blocks > 0) else ""
    fill = "empty blueprint outline with a sparse lattice of glowing knowledge nodes" if phase == "research" else f"partially filled-in structure, {m_to}% of the subject realized on disk"

    prompt = (
        "You are the visual conscience of an autonomous software/research build pipeline. "
        f"The project under construction is: {subject}. "
        f"Current state: phase={phase}, lobe-B severity={sev}, {calls} worker calls, "
        f"{findings} confirmed findings, {blocks} blockers, {artifacts} files on disk: {tops}. "
        f"Truth: materialization {m_to}%, evidence confidence {e_to}%, active conflicts {k_n}. "
        "Render ONE vivid, minimalist diorama. "
        f"Lane: {lane}. Sharpness: {crisp}. {squall} "
        f"Subject: {fill}. "
        "RENDER CONSTRAINTS (mandatory): Do NOT invent components, connections, capabilities, "
        "or text that are not present in the supplied scene state. Render NO text, words, "
        "numbers, labels, or captions anywhere — all labels are drawn programmatically on top "
        "of the image afterward. Do NOT imply completion or validation for structure that is "
        "not materially built: unfilled regions must read as unbuilt. "
        "Keep it abstract but clearly legible as the named subject taking shape. "
        "A calm, high-contrast, cinematic composition. No text, no words, no captions in the image."
    )
    caption = (
        f"phase {phase} · floor {out.get('floor') or '—'} · {calls} worker calls "
        f"· severity {sev} · implementation {m_to}% · evidence {e_to}%"
        + (f" · conflict {k_n}" if k_n else "")
        + f" · {findings} findings · {blocks} blockers · {artifacts} files ({tops})"
        + (" · ⚠ CREDITS BLOCKED" if credit else "")
        + (f" · FINAL {out['final']}" if out.get("final") else "")
    )
    return prompt, caption


def _subject_from_state(out: dict[str, Any], root: Path) -> str:
    """Derive the project's subject noun phrase (best-effort).

    Prefer the flow's state.prompt if a run has published it; else infer from
    the first user `topic_hint` captured in the latest worker event.
    """
    latest = out.get("latest_worker") or {}
    hint = latest.get("topic_hint") or ""
    if hint:
        hint = " ".join(hint.split())[:120]
    if not hint:
        # Fallback: prompt in gateway state dir or flow state is unavailable to
        # the beacon (flow state lives in the runner), so use a generic label.
        hint = "the project"
    return hint or "the project"


def _render_from_pollinations(prompt: str) -> bytes | None:
    q = urllib.parse.quote(prompt)
    url = f"https://image.pollinations.ai/prompt/{q}?width=1024&height=768&nologo=true&model=flux"
    try:
        with urllib.request.urlopen(url, timeout=90) as r:
            return r.read()
    except Exception:
        return None


def _render_procedural(out: dict[str, Any], caption: str, seq: int) -> bytes | None:
    """Local fallback: build a PIL diorama even if the image API is down."""
    if not HAVE_PIL:
        return None
    w, h = 1024, 768
    img = Image.new("RGB", (w, h), (10, 14, 24))
    d = ImageDraw.Draw(img)
    sev = out.get("severity") or "LOW"
    phase = out.get("phase") or "research"
    lane = (120, 200, 255) if phase == "research" else (255, 190, 90)
    black = (8, 8, 12)

    # Horizon + subject block.
    d.rectangle((0, h - 90, w, h), fill=(18, 24, 40))
    block_w = int(w * 0.5)
    block_h = int(h * (0.3 + 0.45 * min(1.0, out.get("artifacts_count", 0) / 20.0)))
    x0 = (w - block_w) // 2
    y0 = h - 90 - block_h
    d.rectangle((x0, y0, x0 + block_w, h - 90), fill=lane, outline=(255, 255, 255))
    # Per-entity truth treatments: K conflicts hatch, low-E materialized blurs,
    # stale transient evidence flickers. Deterministic, data-driven.
    ents = out.get("entities", [])
    k_conflict = any(e.get("K", 0) > 0 for e in ents)
    low_materialized = any(e.get("M", 0) == 1 and e.get("E", 1.0) < 0.4 for e in ents)
    stale_transient = any(e.get("F", 1.0) < 0.8 and e.get("M", 0) == 0 for e in ents)
    if k_conflict:
        for i in range(6):
            d.line((x0, y0 + i * (block_h // 6), x0 + block_w, y0 + i * (block_h // 6)),
                   fill=(90, 90, 110), width=3)
    if low_materialized:
        for i in range(8):  # desaturation smear over the realized block
            d.line((x0, y0, x0 + block_w, y0 + block_h),
                   fill=(70, 70, 90), width=2)
    if stale_transient:
        for sx in range(x0, x0 + block_w, 32):
            for sy in range(y0, h - 90, 32):
                d.ellipse((sx, sy, sx + 3, sy + 3), fill=(150, 150, 170))
    if out.get("credit_blocked"):
        for sx in range(0, w, 24):
            d.rectangle((sx, 0, sx + 6, h), fill=(200, 200, 210))
    # Lattice of knowledge nodes during research.
    if phase == "research":
        for i in range(min(10, out.get("n_findings", 0) + 2)):
            cxp = 60 + (i * (w - 120) // 9)
            cyp = 140 + (i % 3) * 40
            d.ellipse((cxp - 12, cyp - 12, cxp + 12, cyp + 12), fill=(120, 200, 255), outline=(255, 255, 255))
    # Caption strip.
    d.rectangle((0, h - 40, w, h), fill=black)
    _text(d, caption, (w // 2, h - 20), fill=(230, 230, 235), anchor="mm", maxw=w - 20)
    buf = BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _text(d, text, xy, **kw):
    size = 22
    try:
        font = ImageFont.truetype("DejaVuSans.ttf", size)
    except Exception:
        font = ImageFont.load_default()
    d.text(xy, text, font=font, **kw)


_RUN_LOG = Path(os.getenv("RUNTIME_RUN_LOG", "output/run.log"))
_frame_kind = "none"


def _truth_line(out: dict[str, Any]) -> str:
    """Human-readable (M,E,K,F) line appended to the run output inline."""
    m_impl = int(round((out.get("implementation_completeness") or 0.0) * 100))
    m_ev = int(round((out.get("evidence_confidence") or 0.0) * 100))
    m_rc = int(round((out.get("research_completeness") or 0.0) * 100))
    m_ic = int(round((out.get("integrity_conflict") or 0.0) * 100))
    parts = [f"[mirror] {time.strftime('%H:%M:%S')} phase {out.get('phase') or 'research'} "
             f"floor {out.get('floor') or '—'} · implementation {m_impl}% · evidence {m_ev}% "
             f"· research {m_rc}% · conflict {m_ic}%"]
    if out.get("n_blockers"):
        parts.append(f" · {out['n_blockers']} blockers")
    if out.get("credit_blocked"):
        parts.append(" · ⚠ CREDITS BLOCKED")
    if out.get("completion_blocked") and not out.get("credit_blocked"):
        parts.append(" · ⚠ GATE BLOCKED")
    if out.get("final"):
        parts.append(f" · FINAL {out['final']}")
    return "".join(parts) + "\n"


def _write_state_json(out: dict[str, Any], caption: str, seq: int,
                      frame_kind: str, prompt: str = "") -> None:
    ents = [{ "name": e["name"], "M": e["M"], "E": e["E"], "K": e["K"], "F": e["F"],
              "kind": e.get("kind"), "status": e.get("status") }
            for e in out.get("entities", [])]
    try:
        STATE_JSON.write_text(json.dumps({
            "caption": caption, "seq": seq, "prompt": prompt[:400], "frame_kind": frame_kind,
            "time": time.time(), "updated": time.strftime("%H:%M:%S"),
            "metrics": {
                "implementation_completeness": out.get("implementation_completeness"),
                "evidence_confidence": out.get("evidence_confidence"),
                "research_completeness": out.get("research_completeness"),
                "integrity_conflict": out.get("integrity_conflict"),
                "n_conflicts": out.get("n_conflicts"),
            },
            "stage": {
                "phase": out.get("phase"), "floor": out.get("floor"),
                "final": out.get("final"), "credit_blocked": out.get("credit_blocked"),
                "completion_blocked": out.get("completion_blocked"),
                "severity": out.get("severity"), "worker_calls": out.get("worker_calls"),
            },
            "entities": ents,
            "artifacts_tops": out.get("artifacts_tops", []),
            "claims": out.get("claims", [])[-8:],
            "recent_errors": out.get("recent_errors", [])[-3:],
        }, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def _loop(root: Path) -> None:
    global _last_caption, _frame_kind
    seq = 0
    last_gen = 0.0
    last_visual_digest = ""
    while True:
        out = cortex.snapshot(force=True)
        prompt, caption = _director_prompt(out, root)
        _last_caption = caption
        # --- Truth pass: always current, no generative call, no image needed. ---
        try:
            with _RUN_LOG.open("a", encoding="utf-8") as fh:
                fh.write(_truth_line(out))
        except Exception:
            pass
        # Semantic delta against the last recorded revision decides a render.
        revs = cortex.read_revisions(REVISIONS_DIR, 2)
        prev = revs[-1] if revs else None
        cur_rev = cortex.make_revision(out)
        diff = cortex.diff_revisions(prev, cur_rev)
        now = time.time()
        delta_trigger = prev is None or diff["delta"] >= GEN_THRESHOLD
        age_trigger = (now - last_gen) >= GEN_MAX_AGE
        png: bytes | None = None
        kind = _frame_kind
        if (delta_trigger or age_trigger) and (now - last_gen) >= GEN_MIN_INTERVAL:
            seq += 1
            if PROVIDER == "pollinations":
                png = _render_from_pollinations(prompt)
                kind = "generative" if png else "procedural"
            if png is None:
                png = _render_procedural(out, caption, seq)
                kind = "procedural"
            if png:
                try:
                    frame = FRAMES_DIR / f"frame_{seq:06d}.png"
                    frame.write_bytes(png)
                    LATEST.write_bytes(png)
                    last_gen = now
                    _frame_kind = kind
                except Exception:
                    pass
            else:
                kind = _frame_kind
        _write_state_json(out, caption, seq, kind, prompt)
        cortex.save_revision(REVISIONS_DIR, cur_rev)
        try:
            time.sleep(TRUTH_INTERVAL)
        except KeyboardInterrupt:
            break


class Handler(BaseHTTPRequestHandler):
    def _show(self, data: bytes, ctype: str) -> None:
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        if path == "/":
            html = _PAGE
            self._show(html.encode("utf-8"), "text/html; charset=utf-8")
        elif path == "/latest.png":
            if LATEST.exists():
                self._show(LATEST.read_bytes(), "image/png")
            else:
                self.send_response(503); self.end_headers()
        elif path == "/state.json":
            data = STATE_JSON.read_text(encoding="utf-8") if STATE_JSON.exists() else "{}"
            self._show(data.encode("utf-8"), "application/json")
        elif path == "/diff":
            revs = cortex.read_revisions(REVISIONS_DIR, 2)
            cur = cortex.make_revision(cortex.snapshot(force=True))
            diff = cortex.diff_revisions(revs[-1] if revs else None, cur)
            self._show(json.dumps(diff, ensure_ascii=False).encode(), "application/json")
        elif path == "/selected":
            data = SELECTED_JSON.read_text(encoding="utf-8") if SELECTED_JSON.exists() else "{}"
            self._show(data.encode("utf-8"), "application/json")
        elif path == "/filmlist":
            frames = sorted(FRAMES_DIR.glob("frame_*.png"))
            self._show(json.dumps([f.name for f in frames][-24:]).encode(), "application/json")
        elif path.startswith("/frame/"):
            name = path.split("/")[-1]
            f = FRAMES_DIR / name
            if f.exists():
                self._show(f.read_bytes(), "image/png")
            else:
                self.send_response(404); self.end_headers()
        else:
            self.send_response(404); self.end_headers()

    def do_POST(self) -> None:
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        if path in ("/select", "/clear"):
            name = ""
            if path == "/select":
                try:
                    ln = int(self.headers.get("Content-Length", "0") or 0)
                    if ln:
                        name = self.rfile.read(ln).decode("utf-8", "ignore").strip()
                except Exception:
                    name = ""
            try:
                SELECTED_JSON.parent.mkdir(parents=True, exist_ok=True)
                SELECTED_JSON.write_text(json.dumps(
                    {"name": name, "ts": time.time()}, ensure_ascii=False), encoding="utf-8")
                self._show(b"ok", "text/plain")
            except Exception:
                self.send_response(500); self.end_headers()
        else:
            self.send_response(404); self.end_headers()

    def log_message(self, fmt, *args):
        pass


_PAGE = """<!doctype html><html><head><meta charset="utf-8"><title>Pipeline Beacon</title>
<style>
 body{background:#070a10;color:#dbe4f0;font-family:ui-monospace,Menlo,monospace;margin:0;padding:24px}
 h1{font-size:16px;letter-spacing:2px;color:#8fd0ff}
 .cap{font-size:13px;margin:8px 0 14px;color:#aeb9c8;max-width:1100px}
 img.live{width:100%;max-width:1100px;border:1px solid #223;border-radius:8px;background:#000}
 .strip{display:flex;gap:8px;overflow-x:auto;margin-top:14px;padding-bottom:8px}
 .strip img{height:110px;border-radius:6px;border:1px solid #223;cursor:pointer}
 .facts{display:flex;gap:18px;flex-wrap:wrap;margin-top:14px;font-size:12px;color:#9fb2c8}
 .b{color:#8fd0ff}
 .spin{display:inline-block;animation:sp 1.2s linear infinite}
 @keyframes sp{to{transform:rotate(360deg)}}
 .modes{margin:10px 0}
 .modes button{background:#10182c;color:#cfe4ff;border:1px solid #2a3a66;padding:6px 12px;border-radius:6px;cursor:pointer;margin-right:6px;font-family:inherit}
 .modes button.on{background:#1a3a66;border-color:#4a7bc0}
 .badge{display:inline-block;background:#3a2f00;color:#ffd970;border:1px solid #6a5a10;padding:2px 8px;border-radius:5px;font-size:11px;vertical-align:middle}
 table.ent{margin-top:12px;border-collapse:collapse;font-size:12px;width:100%;max-width:900px}
 table.ent td,table.ent th{border-bottom:1px solid #1a2440;padding:3px 10px;text-align:left}
 table.ent tr.sel td{background:#14224a}
 .k{width:52px}
 .lbl{font-family:inherit;border:none;background:transparent;color:inherit;text-align:left;cursor:pointer;font-size:12px;padding:0}
 .k1{color:#ff6b6b} .k0{color:#7fd78f}
</style></head><body>
<h1>■ PIPELINE BEACON — live state reflector</h1>
<p class="cap"><span class="spin">⟳</span> <span id="cap">…</span></p>
<div class="modes">
 <button data-m="concept">Concept</button>
 <button data-m="truth" class="on">Truth</button>
 <button data-m="diff">Diff</button>
 <button data-m="artifact">Artifact</button>
</div>
<img id="live" class="live" src="/latest.png?t=0" alt="live">
<div class="facts" id="facts"></div>
<table class="ent" id="ents" style="display:none"><thead><tr>
<th>component</th><th class="k">M</th><th class="k">E</th><th class="k">K</th><th class="k">F</th></tr></thead>
<tbody id="entrows"></tbody></table>
<div id="diffbox" style="display:none" class="facts" style="max-width:900px"></div>
<div class="strip" id="strip"></div>
<script>
let t=0,mode='truth';
const modes=document.querySelectorAll('.modes button');
modes.forEach(b=>b.onclick=()=>{modes.forEach(x=>x.classList.remove('on'));b.classList.add('on');mode=b.dataset.m;
 document.getElementById('live').style.filter=(mode==='concept'?'saturate(0.4)':'none');
 document.getElementById('diffbox').style.display=(mode==='diff'?'block':'none');
 document.getElementById('ents').style.display=(mode==='diff'?'none':'table');
 refresh();});
async function refresh(){
  try{
    const r=await fetch('/state.json?t='+(++t));const s=await r.json();
    document.getElementById('cap').textContent=(s.caption||'…')+' — updated '+s.updated
      + (s.metrics?('  [impl '+(s.metrics.implementation_completeness*100).toFixed(0)+'% · ev '+(s.metrics.evidence_confidence*100).toFixed(0)+'% · conflict '+(s.metrics.integrity_conflict*100).toFixed(0)+'%]'):'')
      + (s.frame_kind==='generative'?' <span class="badge">ILLUSTRATIVE</span>':'');
    document.getElementById('live').src='/latest.png?t='+(++t);
    const f=document.getElementById('facts');
    const st=s.stage||{}; f.innerHTML=(st.phase||'')+' · floor '+(st.floor||'—')
      +' · severity '+(st.severity||'')+' · '+(st.worker_calls||0)+' calls'
      +(st.credit_blocked?' <b style="color:#ff6b6b">· ⚠ CREDITS BLOCKED</b>':'')
      +(st.final?' · <b>FINAL '+st.final+'</b>':'');
    if(mode==='truth'||mode==='artifact'){
      const tb=document.getElementById('entrows');tb.innerHTML='';
      const rows=(mode==='artifact'?(s.artifacts_tops||[]).map(n=>({name:n,M:1,E:0,K:0,F:1})):(s.entities||[]));
      rows.sort((a,b)=>b.K-a.K||a.E-b.E).forEach(e=>{
        const tr=document.createElement('tr');const b1=document.createElement('button');
        b1.className='lbl';b1.textContent=e.name.length>46?e.name.slice(0,46)+'…':e.name;
        b1.onclick=()=>fetch('/select',{method:'POST',body:e.name});
        tr.append(b1,td(e.M.toFixed?e.M.toFixed(2):'1.00'),td(e.E.toFixed?e.E.toFixed(2):'0.15'),
          td((e.K||0).toFixed(1),true),td(e.F.toFixed?e.F.toFixed(2):'1.00'));
        tb.append(tr);
      });
      document.getElementById('ents').style.display='table';
    }
    if(mode==='diff'){
      const dr=await fetch('/diff');const d=await dr.json();
      let h='<b>SEMANTIC DIFF</b> D\u0394 '+d.delta.toFixed(2);
      if(d.new)h+=' — baseline established';
      if((d.added||[]).length)h+='<br>+ added: '+d.added.join(', ');
      if((d.removed||[]).length)h+='<br>\u2212 removed: '+d.removed.join(', ');
      if((d.conflict_changed||[]).length)h+='<br>! conflict: '+d.conflict_changed.join(', ');
      if((d.gate_events||[]).length)h+='<br>\u26a1 gate: '+d.gate_events.join(', ');
      document.getElementById('diffbox').innerHTML=h;
    }
    const rl=await fetch('/filmlist');const names=await rl.json();
    const st2=document.getElementById('strip');st2.innerHTML='';
    names.slice().reverse().forEach(n=>{const im=document.createElement('img');
      im.src='/frame/'+n;im.onclick=()=>document.getElementById('live').src=im.src;st2.appendChild(im);});
  }catch(e){}
  setTimeout(refresh,5000);
}
function td(txt,cls){const c=document.createElement('td');c.textContent=txt;
  if(cls){c.className='k'+(Number(txt)>0?'1':'0');}return c;}
refresh();
</script></body></html>"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.getcwd())
    args = ap.parse_args()
    root = Path(args.root)
    cortex.ROOT_DIR = root
    cortex.STATE_DIR = Path(os.getenv("DUAL_LOBE_STATE_DIR", root / ".dual_lobe"))
    global REVISIONS_DIR, _RUN_LOG
    REVISIONS_DIR = Path(os.getenv("RUNTIME_BEACON_REVISIONS", root / ".beacon/revisions"))
    _RUN_LOG = Path(os.getenv("RUNTIME_RUN_LOG", root / "output/run.log"))
    th = threading.Thread(target=_loop, args=(root,), daemon=True)
    th.start()
    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    print(f"Beacon on http://127.0.0.1:{PORT}  (frames: {FRAMES_DIR})", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
