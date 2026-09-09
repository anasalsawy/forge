"""Launch the dual-lobe gateway (+ beacon/voice), load .env + settings.env, then run the CrewAI flow."""
import os
import signal
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

try:
    from dotenv import load_dotenv
    root = Path(__file__).resolve().parent
    load_dotenv(root / ".env")
    # settings.env overrides .env (provider/model/toggles live here).
    load_dotenv(root / "settings.env", override=True)
except Exception:
    pass

ROOT = Path(__file__).resolve().parent
ENV = os.environ.copy()
PROCS: list[subprocess.Popen] = []

pkg = "deep_investigator_build_pipeline.dual_lobe_gateway"


def _healthy(url: str, proc: subprocess.Popen) -> bool:
    for _ in range(60):
        try:
            urllib.request.urlopen(url, timeout=0.4).read()
            return True
        except Exception:
            if proc.poll() is not None:
                return False
            time.sleep(0.15)
    return proc.poll() is None


def _env_flag(name: str, default: str) -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "on")


def _resolve_layers(args, raw_flag: bool):
    """Resolve whether to launch gateway/image/voice from CLI flags + env + --raw."""
    if raw_flag:
        return {"gateway": True, "image": False, "voice": False}
    return {
        "gateway": args.gateway_on if args.gateway_on is not None else _env_flag("RUNTIME_GATEWAY", "1"),
        "image": args.image_on if args.image_on is not None else _env_flag("RUNTIME_BEACON", "1"),
        "voice": args.voice_on if args.voice_on is not None else _env_flag("RUNTIME_VOICE", "1"),
    }


def _add_options(ap) -> None:
    ap.add_argument("--with-voice", "--voice", action="store_true", dest="voice_on",
                    help="Launch the voice layer (WebSocket console).")
    ap.add_argument("--without-voice", "--no-voice", action="store_false", dest="voice_on",
                    help="Do not launch the voice layer.")
    ap.add_argument("--with-image", "--image", action="store_true", dest="image_on",
                    help="Launch the image/beacon layer (visual reflector).")
    ap.add_argument("--without-image", "--no-image", action="store_false", dest="image_on",
                    help="Do not launch the image/beacon layer.")
    ap.add_argument("--with-gateway", action="store_true", dest="gateway_on",
                    help="Launch the dual-lobe LLM gateway.")
    ap.add_argument("--without-gateway", "--no-gateway", action="store_false", dest="gateway_on",
                    help="Do not launch the gateway (expect one already running).")
    ap.add_argument("--no-layers", "--raw", action="store_true",
                    help="Run raw (gateway only, no voice/image layers).")
    ap.set_defaults(voice_on=None, image_on=None, gateway_on=None)


def main(argv: list[str]) -> int:
    import argparse
    ap = argparse.ArgumentParser(description="Dual-lobe pipeline launcher")
    _add_options(ap)
    # Unknown args pass through to `crewai run`.
    args, rest = ap.parse_known_args(argv)

    layers = _resolve_layers(args, args.no_layers)
    voice_enabled = layers["voice"]
    image_enabled = layers["image"]
    gateway_enabled = layers["gateway"]

    inputs = rest
    if gateway_enabled:
        gateway = subprocess.Popen([sys.executable, "-m", pkg], env=ENV)
        PROCS.append(gateway)

    # Optional image/beacon layer (visual state reflector on 8766).
    beacon = None
    if image_enabled:
        try:
            p = "deep_investigator_build_pipeline.runtime_beacon"
            beacon = subprocess.Popen([sys.executable, "-m", p, "--root", str(ROOT)], env=ENV)
            PROCS.append(beacon)
        except Exception:
            beacon = None

    # Optional voice layer (WebSocket console on 8767 + WS on 8768).
    voice = None
    if voice_enabled:
        try:
            v = "deep_investigator_build_pipeline.runtime_voice"
            voice = subprocess.Popen([sys.executable, "-m", v, "--root", str(ROOT)], env=ENV)
            PROCS.append(voice)
        except Exception:
            voice = None

    if gateway_enabled:
        health = f"http://{os.getenv('DUAL_LOBE_HOST','127.0.0.1')}:{os.getenv('DUAL_LOBE_PORT','8765')}/health"
        if not _healthy(health, gateway):
            print("Gateway failed to start; aborting.", file=sys.stderr)
            return 1

    print(f"[run_dual_lobe] gateway={gateway_enabled} image={image_enabled} voice={voice_enabled}", flush=True)

    # The flow is the canonical clean direct variant: workers call the upstream
    # model directly (no shadow lobe). Dual-lobe is implemented at the LLM-call
    # layer, not as a flow variant.
    flow_file = ROOT / "src/deep_investigator_build_pipeline/flow_direct.json"

    cmd = ["crewai", "run", "--definition", str(flow_file)] + inputs

    # Merge any pending operator directive from output/control.json into the
    # flow's initial state (state.directive). Newest directive wins; consume it.
    try:
        control_file = ROOT / "output" / "control.json"
        if control_file.exists():
            import json as _json
            ctrl = _json.loads(control_file.read_text(encoding="utf-8"))
            directive = ctrl.get("directive") if isinstance(ctrl, dict) else None
            if isinstance(directive, dict) and directive.get("text"):
                flow_inputs = {}
                rest_inputs = inputs
                # Parse an existing --inputs JSON value from the CLI args.
                for i, a in enumerate(inputs):
                    if a == "--inputs" and i + 1 < len(inputs):
                        try:
                            flow_inputs = _json.loads(inputs[i + 1])
                        except Exception:
                            flow_inputs = {}
                        rest_inputs = inputs[:i] + inputs[i + 2:]
                        break
                flow_inputs.setdefault("prompt", os.getenv("RUNTIME_RUN_PROMPT", ""))
                flow_inputs["directive"] = directive["text"]
                cmd = (["crewai", "run", "--definition", str(flow_file)]
                       + rest_inputs + ["--inputs", _json.dumps(flow_inputs)])
                # Consume the directive once applied.
                ctrl["directive"] = None
                control_file.write_text(_json.dumps(ctrl, ensure_ascii=False, indent=2), encoding="utf-8")
                print("[run_dual_lobe] injected operator directive into next kickoff.", flush=True)
    except Exception as e:
        print(f"[run_dual_lobe] directive injection skipped: {e}", flush=True)

    # Tee crewai output to output/run.log so the beacon can parse floor/phase.
    (ROOT / "output").mkdir(exist_ok=True)
    log_path = ROOT / "output" / "run.log"
    os.environ["RUNTIME_RUN_LOG"] = str(log_path)
    with log_path.open("ab") as logf:
        p = subprocess.Popen(cmd, env=ENV, cwd=ROOT, stdout=logf, stderr=subprocess.STDOUT)
        PROCS.append(p)
        rc = p.wait()
    return rc


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    finally:
        for p in PROCS:
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
        for p in PROCS:
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()
