#!/usr/bin/env python3
"""Spark Hub fetcher.

Collects everything a single DGX Spark owner wants on one page and writes
data.js (window.SPARK_DATA = {...}) next to index.html.

    Sources                         Section in index.html
    ------------------------------  ---------------------------
    NVIDIA/dgx-spark-playbooks      Playbooks, Supported models
    NVIDIA/local-ai-inference-...   Supported models (recipes)
    Hugging Face Hub API            Models, Quantizations
    Artificial Analysis API (opt.)  Ratings
    forums.developer.nvidia.com     Community tips

    fetch_*()  ->  build_*()  ->  data.js  ->  index.html

Standard library only. Each source is isolated: one failing source keeps
its previous data and the rest still refresh.

Env vars (all optional):
    HF_TOKEN        raises Hugging Face rate limits
    GITHUB_TOKEN    raises GitHub API rate limit (only 2 API calls used)
    AA_API_KEY      Artificial Analysis key, enables benchmark ratings
"""

import argparse
import concurrent.futures as cf
import datetime as dt
import html
import json
import math
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCHEMA_VERSION = 1
USER_AGENT = "spark-hub/1.0 (+local dashboard for DGX Spark)"
HTTP_TIMEOUT_S = 30
HTTP_RETRIES = 3
RETRY_BACKOFF_S = 2.0
WORKERS = 6

# DGX Spark hardware (GB10, 128 GB LPDDR5x unified memory).
SPARK_MEM_GB = 128
SPARK_USABLE_GB = 115          # left after DGX OS + desktop
SPARK_FIT_OK_GB = 100          # weights below this leave room for KV cache
SPARK_BW_GBS = 273             # memory bandwidth
DECODE_EFFICIENCY = 0.6        # share of bandwidth real decoders reach

GH_API = "https://api.github.com"
GH_RAW = "https://raw.githubusercontent.com"
PLAYBOOK_REPO = "NVIDIA/dgx-spark-playbooks"
RECIPE_REPO = "NVIDIA/local-ai-inference-recipes"
PLAYBOOK_WEB = "https://build.nvidia.com/spark"
RECIPE_WEB = "https://nvidia.github.io/local-ai-inference-recipes/hardware/dgx-spark/index.html"

HF_API = "https://huggingface.co/api"
HF_WEB = "https://huggingface.co"
HF_LIST_LIMIT = 100
HF_NEW_DAYS = 90
HF_NEW_MIN_LIKES = 15
HF_MAX_BASE_MODELS = 70
HF_MAX_QUANT_REPOS = 14
HF_MAX_TREE_PER_BASE = 6
HF_MAX_PARAMS_B = 400          # bigger never fits one Spark even at 2-bit

AA_URL = "https://artificialanalysis.ai/api/v2/data/llms/models"

FORUM = "https://forums.developer.nvidia.com"
FORUM_CAT_MAIN = 719           # DGX Spark / GB10 User Forum
FORUM_CAT_ANNOUNCE = 722
FORUM_LATEST_PAGES = 3
FORUM_MAX_EXCERPTS = 60
FORUM_SLEEP_S = 0.4

GiB = 1024 ** 3
GB = 1000 ** 3

# Pipelines scanned on Hugging Face -> page category.
PIPELINES = {
    "text-generation": "llm",
    "image-text-to-text": "vision",
    "any-to-any": "omni",
    "text-to-image": "image-gen",
    "text-to-video": "video-gen",
    "image-to-video": "video-gen",
    "automatic-speech-recognition": "speech",
    "text-to-speech": "speech",
    "feature-extraction": "embedding",
    "sentence-similarity": "embedding",
}

# Orgs whose repos are treated as "official" releases.
OFFICIAL_ORGS = {
    "nvidia", "qwen", "google", "meta-llama", "deepseek-ai", "mistralai",
    "openai", "microsoft", "zai-org", "thudm", "moonshotai", "minimaxai",
    "ibm-granite", "allenai", "black-forest-labs", "stabilityai",
    "tencent", "baidu", "xiaomimimo", "lgai-exaone", "huggingfacetb",
    "ai21labs", "cohereforai", "coherelabs", "bytedance-seed", "wan-ai",
    "inclusionai", "stepfun-ai", "meituan-longcat", "liquidai",
}

# Well-known quantizers; their repos win ties when choosing variants.
TRUSTED_QUANTIZERS = {
    "nvidia", "unsloth", "bartowski", "lmstudio-community", "redhatai",
    "qwen", "google", "mistralai", "intel", "cyankiwi", "ggml-org",
    "mlx-community", "turboderp",
}

# Quant format -> runtimes that load it on a Spark.
FORMAT_RUNTIMES = {
    "nvfp4": ["vLLM", "TensorRT-LLM", "SGLang"],
    "mxfp4": ["vLLM", "TensorRT-LLM", "llama.cpp"],
    "fp8": ["vLLM", "TensorRT-LLM", "SGLang"],
    "awq": ["vLLM", "SGLang"],
    "gptq": ["vLLM", "SGLang"],
    "int8": ["vLLM"],
    "bnb": ["Transformers"],
    "gguf": ["llama.cpp", "Ollama", "LM Studio"],
    "exl": ["ExLlama / TabbyAPI"],
    "mlx": ["MLX (Mac only)"],
    "bf16": ["vLLM", "SGLang", "TensorRT-LLM", "Transformers"],
}

# Effective bits per weight, used when real file sizes are unknown.
FORMAT_BITS = {
    "nvfp4": 4.5, "mxfp4": 4.25, "fp8": 8.0, "awq": 4.25, "gptq": 4.25,
    "int8": 8.0, "bnb": 4.5, "exl": 4.0, "mlx": 4.5, "bf16": 16.0,
}

# Order matters: first match wins.
FORMAT_TAG_RULES = [
    ("gguf", ("gguf",)),
    ("mlx", ("mlx",)),
    ("exl", ("exl2", "exl3", "exllamav2", "exllamav3")),
    ("nvfp4", ("nvfp4", "modelopt", "fp4")),
    ("mxfp4", ("mxfp4",)),
    ("awq", ("awq",)),
    ("gptq", ("gptq", "autoround")),
    ("fp8", ("fp8",)),
    ("bnb", ("bitsandbytes", "4-bit")),
    ("int8", ("int8", "w8a8")),
]

GGUF_QUANT_RE = re.compile(
    r"(?i)(?:UD-)?(IQ[1-4]_[A-Z]{1,3}|TQ[12]_0|Q[2-8]_K(?:_[A-Z]{1,2})?|"
    r"Q[4-8]_[01]|MXFP4(?:_MOE)?|BF16|F16|F32)"
)
ACTIVE_PARAMS_RE = re.compile(r"(?i)[-_.]A(\d+(?:\.\d+)?)B(?:[-_.]|$)")
TOTAL_PARAMS_RE = re.compile(r"(?i)(?:^|[-_.])(\d+(?:\.\d+)?)B(?:[-_.]|$)")

# Category keywords (matched on lower-cased repo id).
CATEGORY_KEYWORDS = {
    # Match specialization in repo IDs and HF tags. Keep generic "code" token-aware
    # so e.g. "decode" does not classify a normal model as a coding model.
    "coding": ("coder", "coding", "codegemma", "codestral", "devstral", "starcoder", "wizardcoder", "magicoder", "codeqwen", "codegeex", "granite-code", "stable-code", "deepseek-coder", "kimi-dev", "replit", "swe", "programming", "code-generation", "code-completion", "text-to-code", "text2code", "software-development", "code-instruct", "instruct-code", "codellama", "opencoder", "codegecko"),
    "reasoning": ("reason", "thinking", "-think", "r1", "qwq", "magistral", "phi-4-reasoning"),
}

# Community feed topic tags: tag -> (label, keyword regex).
FORUM_TOPICS = {
    "performance": ("Performance", r"tok/s|tokens?/s|t/s|benchmark|throughput|speed|faster|latency|perf\b|tps\b|prefill|decode"),
    "vllm-trt": ("vLLM / TRT-LLM / SGLang", r"vllm|tensorrt|trt-?llm|trtllm|sglang|\bnim\b|flare"),
    "ollama-llamacpp": ("Ollama / llama.cpp / LM Studio", r"ollama|llama\.?cpp|llama-server|gguf|lm ?studio|exl[23]"),
    "fine-tuning": ("Fine-tuning", r"fine-?tun|lora|qlora|unsloth|training|\bnemo\b|llama-?factory|pretrain|sft\b"),
    "networking": ("Networking / NVIDIA Sync", r"nvidia sync|\bsync\b|ssh|network|remote|tailscale|vnc|rdp|wi-?fi|ethernet|headless|thinlinc|vpn"),
    "thermals": ("Thermals / Stability", r"thermal|temperature|overheat|\bfans?\b|throttl|shutdown|freez|hang|power|reboot|stabil"),
    "multi-spark": ("Pairing Sparks", r"\b[23] ?x ?(dgx )?sparks?|(two|three|multiple) (dgx )?sparks|dual spark|cluster|connectx|qsfp|nccl|multi-?node|tensorfold|ring topology|\btp ?=? ?2\b"),
    "image-video": ("Image / Video gen", r"comfy|flux|\bwan\b|video|image gen|diffusion|sdxl"),
    "hardware": ("Hardware / OS / Drivers", r"warranty|rma|fault|firmware|driver|dgx os|kernel|boot|bios|ssd|usb|display|monitor|update"),
}
FORUM_KIND_TIP = r"how to|guide|tutorial|tip|trick|recipe|setup|set up|walkthrough|benchmark|results|tok/s|works|solved|released|new .* recipe"
FORUM_KIND_ISSUE = r"error|fail|not work|crash|issue|problem|help|cannot|can't|unable|broken|freeze|declined"

MODEL_FAMILY_RE = r"qwen|deepseek|nemotron|glm|gemma|llama|gpt-oss|minimax|mistral|devstral|kimi|phi|granite|flux|wan"

# Strings that look like owner/name but are not Hugging Face repos.
NON_HF_OWNERS = {
    "vllm", "lmsysorg", "eugr", "nvcr.io", "ghcr.io", "docker", "library",
    "ubuntu", "ollama", "localhost", "http:", "https:", "github.com",
    "nvidia.github.io", "build.nvidia.com", "etc", "usr", "home", "dev",
    "tmp", "opt", "var", "mnt", "proc", "sys", "assets", "src", "models",
    "datasets", "docs", "settings", "spaces", "v1", "api",
}
HF_HANDLE_RE = re.compile(r"^[A-Za-z0-9][\w.-]{0,95}/[A-Za-z0-9][\w.-]{0,95}$")
FILE_EXT_RE = re.compile(r"\.(sh|py|md|ya?ml|json|txt|png|jpg|gguf|safetensors|html|toml|cfg|ini|log|tar|gz|zip)$", re.I)

PLAYBOOK_GROUPS = [
    ("Coding & agents", r"coding|claude|vibe|vs ?code|agent|claw|hermes|openshell|chatbot"),
    ("Inference & serving", r"vllm|trt|tensorrt|sglang|llama\.cpp|lm studio|ollama|nim|nemotron|local llm|speculative|multi-modal inference|nvfp4|quantiz|pair"),
    ("Fine-tuning & training", r"fine-?tun|unsloth|nemo|factory|pytorch|nanochat|pretrain|triton|kernel"),
    ("Image, video & vision", r"comfy|flux|image|video|vlm|vision|vss|photo"),
    ("Multi-Spark & networking", r"connect|nccl|cluster|switch|sync|tailscale|brev|cupynumeric|multiple|two |three "),
    ("Data science & apps", r"cuda-x|data science|rapids|single-cell|topic|portfolio|cuopt|txt2kg|rag|jax|cutile|healthcare|home|isaac|gr00t"),
    ("Device & tools", r"dashboard|mig|multi-gpu|webui|open webui"),
]

DEFAULT_OUT = os.path.dirname(os.path.abspath(__file__))


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def log(msg):
    stamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{stamp}] {msg}", flush=True)


def now_iso():
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def parse_iso(value):
    if not value:
        return None
    try:
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def days_since(value):
    stamp = parse_iso(value)
    if stamp is None:
        return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=dt.timezone.utc)
    return (dt.datetime.now(dt.timezone.utc) - stamp).days


def strip_html(text):
    text = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", text or "", flags=re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


HOST_FAILS = {}            # host -> consecutive connection failures
HOST_FAIL_LIMIT = 3       # after this, skip the host for the rest of the run


def http_get(url, headers=None, as_json=True):
    """GET with retries. Returns parsed JSON / text, or raises.

    A host that keeps refusing connections is short-circuited so an
    outage costs seconds, not minutes.
    """
    host = urllib.parse.urlsplit(url).netloc
    if HOST_FAILS.get(host, 0) >= HOST_FAIL_LIMIT:
        raise ConnectionError(f"{host} unreachable earlier in this run")

    hdrs = {"User-Agent": USER_AGENT, "Accept": "application/json" if as_json else "*/*"}
    hdrs.update(headers or {})
    last_err = None

    for attempt in range(HTTP_RETRIES):
        req = urllib.request.Request(url, headers=hdrs)
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as resp:
                body = resp.read().decode("utf-8", errors="replace")
            HOST_FAILS[host] = 0
            return json.loads(body) if as_json else body
        except urllib.error.HTTPError as err:
            last_err = err
            # 4xx other than rate-limit will not get better on retry.
            if err.code in (400, 401, 403, 404, 410):
                raise
            wait = RETRY_BACKOFF_S * (attempt + 1)
            if err.code == 429:
                wait = float(err.headers.get("Retry-After") or wait * 3)
            time.sleep(wait)
        except (urllib.error.URLError, TimeoutError, ConnectionError) as err:
            last_err = err
            HOST_FAILS[host] = HOST_FAILS.get(host, 0) + 1
            if HOST_FAILS[host] >= HOST_FAIL_LIMIT:
                break
            time.sleep(RETRY_BACKOFF_S * (attempt + 1))

    raise last_err


def try_get(url, headers=None, as_json=True, default=None):
    try:
        return http_get(url, headers, as_json)
    except Exception as err:  # noqa: BLE001 - every source must survive
        log(f"  warn: {url[:110]} -> {err}")
        return default


def parallel_map(func, items, workers=WORKERS):
    with cf.ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(func, items))


def gh_headers():
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        return {}
    return {"Authorization": f"Bearer {token}"}


def hf_headers():
    token = os.environ.get("HF_TOKEN")
    if not token:
        return {}
    return {"Authorization": f"Bearer {token}"}


def load_json(path, default):
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return default


def write_atomic(path, text):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(text)
    os.replace(tmp, path)


# Structured refresh progress shared with spark_hub_server.py through
# SPARK_HUB_PROGRESS_FILE. Stage numbers/labels must match the dashboard.
_PROGRESS_STAGES = {
    1: "Initialize",
    2: "Hugging Face",
    3: "NVIDIA Playbooks",
    4: "NVIDIA Recipes",
    5: "Forum Topics",
    6: "Model Ratings",
    7: "Process Data",
    8: "Detect Changes",
    9: "Save Files",
    10: "Finalize",
}


def report_progress(stage, detail, *, state="running", stage_percent=0):
    """Log and write authoritative progress from the collector's execution path."""
    progress_path = os.environ.get("SPARK_HUB_PROGRESS_FILE")
    stage = max(1, min(10, int(stage)))
    stage_percent = max(0, min(100, int(stage_percent)))
    percent = min(100, ((stage - 1) * 10) + (stage_percent // 10))
    label = _PROGRESS_STAGES[stage]
    log(f"[{label}] {detail} ({percent}%)")
    if not progress_path:
        return
    payload = {
        "stage": stage,
        "total": 10,
        "label": label,
        "detail": str(detail)[:240],
        "percent": percent,
        "stage_percent": stage_percent,
        "state": state,
        "updated": now_iso(),
    }
    try:
        write_atomic(progress_path, json.dumps(payload, ensure_ascii=False))
    except OSError as exc:
        log(f"WARN unable to write refresh progress: {exc}")


def tag_list(raw_tags):
    """Discourse returns tags as strings or {name: ...} objects."""
    out = []
    for tag in raw_tags or []:
        if isinstance(tag, dict):
            tag = tag.get("name") or tag.get("id") or ""
        if tag:
            out.append(str(tag))
    return out


# ---------------------------------------------------------------------------
# Source 1: NVIDIA DGX Spark playbooks (GitHub)
# ---------------------------------------------------------------------------

def gh_tree(repo):
    url = f"{GH_API}/repos/{repo}/git/trees/main?recursive=1"
    data = http_get(url, gh_headers())
    return data.get("tree", [])


def parse_playbook_index(readme):
    """Root README lists the current playbooks as '- [Title](nvidia/slug/)'."""
    items = []
    for title, path in re.findall(r"^- \[([^\]]+)\]\((nvidia/[^)/]+)/?\)", readme, re.M):
        items.append({"title": title.strip(), "path": path})
    return items


def md_field(md, label):
    match = re.search(rf"\*\*{label}:\*\*\s*(.+)", md)
    if not match:
        return None
    return match.group(1).strip()


def short_time(text):
    """'30 MIN (including ...)' -> '30 MIN'."""
    if not text:
        return None
    match = re.search(r"\d+\s*(?:[–-]\s*\d+\s*)?(?:MIN|HR|HOUR|H)\w*", text, re.I)
    return match.group(0).upper() if match else text[:30]


def short_risk(text):
    """'**Medium to High**—the agent ...' -> 'Medium-High'."""
    if not text:
        return None
    levels = re.findall(r"\b(Low|Medium|High)\b", text[:60], re.I)
    if not levels:
        return None
    levels = [lvl.capitalize() for lvl in levels[:2]]
    return "-".join(dict.fromkeys(levels))


def parse_last_updated(md):
    """'- **Last Updated:** 10/09/2026' followed by '  - change' lines."""
    match = re.search(r"\*\*Last Updated:\*\*\s*([0-9/.-]+)(.*?)(?:\n(?!\s+-)|\Z)", md, re.S)
    if not match:
        return None, []

    raw = match.group(1).strip()
    iso = None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d.%m.%Y"):
        try:
            iso = dt.datetime.strptime(raw, fmt).date().isoformat()
            break
        except ValueError:
            continue

    notes = [n.strip() for n in re.findall(r"\n\s+-\s+(.+)", match.group(2))]
    return iso, notes[:4]


def extract_hf_handles(md):
    """Find Hugging Face repo ids mentioned in a playbook README.

    strong = from 'hf download', huggingface.co links or table cells next
    to an 'HF / Hugging Face handle' header. weak = any `owner/name` code
    span; kept only after Hub validation.
    """
    strong, weak = set(), set()

    for handle in re.findall(r"hf download\s+([\w.-]+/[\w.-]+)", md):
        strong.add(handle)
    for handle in re.findall(r"huggingface\.co/([\w.-]+/[\w.-]+)", md):
        strong.add(handle)
    for handle in re.findall(r"(?:-hf|--hf-repo)\s+([\w.-]+/[\w.-]+)", md):
        strong.add(handle)
    # Tables whose header names a HF handle / checkpoint column are strong.
    handle_table = False
    for line in md.splitlines():
        if not line.startswith("|"):
            handle_table = False
            continue
        if re.search(r"(?i)hugging ?face|hf handle|checkpoint|model id", line) and "`" not in line:
            handle_table = True
            continue
        spans = re.findall(r"`([^`\s]{3,120})`", line)
        (strong if handle_table else weak).update(spans)

    for span in re.findall(r"`([^`\s]{3,120})`", md):
        weak.add(span)

    def clean(handle):
        handle = handle.split(":")[0].strip().rstrip(".,)")
        if handle.startswith(("http", "~", "/", ".", "$")):
            return None
        if not HF_HANDLE_RE.match(handle) or FILE_EXT_RE.search(handle):
            return None
        owner = handle.split("/")[0].lower()
        if owner in NON_HF_OWNERS or "." in owner and owner.endswith((".io", ".com")):
            return None
        return handle

    strong = {h for h in map(clean, strong) if h}
    weak = {h for h in map(clean, weak) if h} - strong
    return strong, weak


def extract_ollama_tags(md):
    tags = set(re.findall(r"ollama (?:pull|run)\s+([a-z0-9][\w.\-]*(?::[\w.\-]+)?)", md))
    return sorted(t for t in tags if not t.startswith("hf.co"))


def extract_hardware(md):
    """Read the 'Supported hardware platforms' table."""
    section = re.search(r"## Supported hardware platforms(.*?)\n## ", md, re.S)
    if not section:
        return []
    names = re.findall(r"^\|\s*\*\*([^*]+)\*\*", section.group(1), re.M)
    return [n.strip() for n in names]


def classify_playbook(title):
    low = title.lower()
    for group, pattern in PLAYBOOK_GROUPS:
        if re.search(pattern, low):
            return group
    return "Other"


def fetch_playbooks(cache):
    log("Playbooks: GitHub tree + README index")
    tree = gh_tree(PLAYBOOK_REPO)
    sha_by_path = {e["path"]: e.get("sha") for e in tree if e.get("type") == "blob"}

    root = http_get(f"{GH_RAW}/{PLAYBOOK_REPO}/main/README.md", as_json=False)
    index = parse_playbook_index(root)
    readme_cache = cache.setdefault("readmes", {})

    def load(item):
        path = f"{item['path']}/README.md"
        sha = sha_by_path.get(path)
        cached = readme_cache.get(path)
        if cached and cached.get("sha") == sha:
            return item, cached["md"]
        md = try_get(f"{GH_RAW}/{PLAYBOOK_REPO}/main/{path}", as_json=False, default="")
        readme_cache[path] = {"sha": sha, "md": md}
        return item, md

    playbooks = []
    for item, md in parallel_map(load, index):
        if not md or re.search(r"example test playbook", item["title"], re.I):
            continue

        slug = item["path"].split("/", 1)[1]
        web_slug = slug.replace("playbook-", "", 1)
        tagline = re.search(r"^>\s*(.+)$", md, re.M)
        updated, notes = parse_last_updated(md)
        strong, weak = extract_hf_handles(md)
        hardware = extract_hardware(md)
        spark_ok = not hardware or any("spark" in h.lower() for h in hardware)

        playbooks.append({
            "slug": web_slug,
            "title": item["title"],
            "tagline": tagline.group(1).strip() if tagline else "",
            "group": classify_playbook(item["title"]),
            "updated": updated,
            "notes": notes,
            "time": short_time(md_field(md, "Estimated time")),
            "risk": short_risk(md_field(md, "Risk level")),
            "hardware": hardware,
            "spark": spark_ok,
            "multi_spark": bool(re.search(r"two|three|multiple|cluster|switch|across", item["title"], re.I)),
            "models_strong": sorted(strong),
            "models_weak": sorted(weak),
            "ollama": extract_ollama_tags(md),
            "url": f"{PLAYBOOK_WEB}/{web_slug}",
            "github": f"https://github.com/{PLAYBOOK_REPO}/tree/main/{item['path']}",
        })

    # Drop README cache entries for playbooks that no longer exist.
    live = {f"{i['path']}/README.md" for i in index}
    for path in list(readme_cache):
        if path not in live:
            del readme_cache[path]

    playbooks.sort(key=lambda p: p["updated"] or "", reverse=True)
    log(f"  {len(playbooks)} playbooks")
    return playbooks


# ---------------------------------------------------------------------------
# Source 2: NVIDIA local AI inference recipes (catalog.json)
# ---------------------------------------------------------------------------

def fetch_recipes():
    log("Recipes: NVIDIA local-ai-inference-recipes catalog")
    catalog = http_get(f"{GH_RAW}/{RECIPE_REPO}/main/catalog.json")
    recommended = try_get(f"{GH_RAW}/{RECIPE_REPO}/main/recommended_catalog.json", default={}) or {}
    rec_ids = set()
    for item in recommended.get("recommended_recipes", []) or []:
        rec_ids.add(item.get("recipe_id") or item.get("id") if isinstance(item, dict) else item)

    recipes = []
    for model in catalog.get("models", []):
        for rec in model.get("recipes", []):
            hw = rec.get("hardware", "")
            if not hw.startswith("dgx-spark"):
                continue

            ckpt = rec.get("checkpoint") or {}
            serve = rec.get("serve") or {}
            runtime = rec.get("runtime") or {}
            recipes.append({
                "id": rec.get("id"),
                "model_id": model.get("id"),
                "name": model.get("display_name"),
                "family": model.get("family"),
                "builder": model.get("builder"),
                "modalities": model.get("modalities") or [],
                "upstream": model.get("upstream"),
                "hardware": hw,
                "sparks": 2 if hw.endswith("2gpu") else 1,
                "backend": rec.get("backend"),
                "backend_version": rec.get("backend_version"),
                "precision": rec.get("precision"),
                "variant": rec.get("variant"),
                "status": rec.get("status"),
                "min_mem_gb": rec.get("min_vidmem_gb"),
                "checkpoint": ckpt.get("target_label"),
                "checkpoint_url": ckpt.get("target"),
                "draft": ckpt.get("draft_label"),
                "image": runtime.get("image"),
                "setup_cmd": runtime.get("setup_command"),
                "serve_cmd": serve.get("command"),
                "port": serve.get("port"),
                "limitations": rec.get("limitations") or [],
                "recommended": rec.get("id") in rec_ids,
                "url": f"https://github.com/{RECIPE_REPO}/tree/main/models/{rec.get('id')}",
            })

    log(f"  {len(recipes)} DGX Spark recipes")
    return recipes


# ---------------------------------------------------------------------------
# Source 3: Hugging Face Hub
# ---------------------------------------------------------------------------

HF_EXPAND = ["safetensors", "gguf", "createdAt", "likes", "downloads", "tags",
             "pipeline_tag", "trendingScore", "lastModified", "library_name"]


def hf_list(**params):
    query = [(k, v) for k, v in params.items()]
    query += [("expand[]", e) for e in HF_EXPAND]
    url = f"{HF_API}/models?{urllib.parse.urlencode(query)}"
    return try_get(url, hf_headers(), default=[]) or []


def hf_model(repo_id):
    query = urllib.parse.urlencode([("expand[]", e) for e in HF_EXPAND])
    return try_get(f"{HF_API}/models/{repo_id}?{query}", hf_headers())


def hf_tree(repo_id):
    url = f"{HF_API}/models/{repo_id}/tree/main?recursive=true"
    return try_get(url, hf_headers(), default=[]) or []


def hf_exists(repo_id):
    try:
        http_get(f"{HF_API}/models/{repo_id}", hf_headers())
        return True
    except urllib.error.HTTPError as err:
        return False if err.code in (401, 404) else None
    except Exception:  # noqa: BLE001
        return None


def base_of(tags):
    for tag in tags or []:
        if tag.startswith("base_model:quantized:"):
            return tag.split(":", 2)[2], "quantized"
    for tag in tags or []:
        if tag.startswith("base_model:finetune:"):
            return tag.split(":", 2)[2], "finetune"
    return None, None


def license_of(tags):
    for tag in tags or []:
        if tag.startswith("license:"):
            return tag.split(":", 1)[1]
    return None


def format_of(tags, repo_id):
    low = {t.lower() for t in tags or []}
    rid = repo_id.lower()
    for fmt, keys in FORMAT_TAG_RULES:
        if any(k in low for k in keys) or any(re.search(rf"[-_]{re.escape(k)}(\b|[-_])", rid) for k in keys):
            return fmt
    return None


def param_count(info):
    st = info.get("safetensors") or {}
    if st.get("total"):
        return st["total"]
    gg = info.get("gguf") or {}
    if gg.get("total"):
        return gg["total"]
    match = TOTAL_PARAMS_RE.search(info.get("id", "").split("/")[-1])
    return float(match.group(1)) * 1e9 if match else None


def active_params_b(repo_id, total_b):
    match = ACTIVE_PARAMS_RE.search(repo_id.split("/")[-1])
    if match:
        return float(match.group(1))
    return total_b


def categories_of(repo_id, pipeline, tags=None):
    cats = [PIPELINES.get(pipeline, "llm")]
    # Classify from the full repo identifier plus Hugging Face tags. Model IDs
    # often use separators differently (Qwen2.5-Coder, code_generation, etc.).
    parts = [str(repo_id or ""), str(pipeline or "")]
    if isinstance(tags, (list, tuple, set)):
        parts.extend(str(t.get("name", "") if isinstance(t, dict) else t) for t in tags)
    low = re.sub(r"[_./]+", "-", " ".join(parts).lower())
    tokenized = re.sub(r"[^a-z0-9]+", "-", low).strip("-")
    for cat, keys in CATEGORY_KEYWORDS.items():
        if cat == "coding":
            # Code must be a full token or known compound term; coder/codegemma
            # are also caught by explicit specialist terms above.
            has_code_token = bool(re.search(r"(?:^|-)code(?:-|$)", tokenized))
            if has_code_token or any(k in tokenized for k in keys):
                cats.append(cat)
        elif any(k in tokenized for k in keys):
            cats.append(cat)
    if cats[0] == "llm" and "coding" not in cats and "reasoning" not in cats:
        cats.append("general")
    return cats


def fit_of(size_gb):
    if size_gb is None:
        return "unknown"
    if size_gb <= SPARK_FIT_OK_GB:
        return "fits"
    if size_gb <= SPARK_USABLE_GB:
        return "tight"
    return "no"


def est_tok_s(size_gb, total_b, active_b):
    """Decode speed bound by memory bandwidth over active weights."""
    if not size_gb or not total_b or not active_b:
        return None
    active_gb = size_gb * (active_b / total_b)
    if active_gb <= 0:
        return None
    return round(SPARK_BW_GBS * DECODE_EFFICIENCY / active_gb, 1)


def gguf_variants(repo_id, files):
    """Group GGUF files (incl. shards) by quant label."""
    groups = {}
    for f in files:
        path = f.get("path", "")
        if f.get("type") != "file" or not path.lower().endswith(".gguf"):
            continue
        if "mmproj" in path.lower():
            continue
        match = GGUF_QUANT_RE.search(path.split("/")[-1]) or GGUF_QUANT_RE.search(path)
        if not match:
            continue
        label = match.group(0).upper()
        size = (f.get("lfs") or {}).get("size") or f.get("size") or 0
        grp = groups.setdefault(label, {"bytes": 0, "file": path})
        grp["bytes"] += size
        if "00001-of" in path:
            grp["file"] = path

    out = []
    for label, grp in groups.items():
        out.append({
            "repo": repo_id,
            "format": "gguf",
            "quant": label,
            "size_gb": round(grp["bytes"] / GB, 1),
            "file": grp["file"],
        })
    return out


def safetensor_bytes(files):
    total = 0
    for f in files:
        if f.get("type") == "file" and f.get("path", "").endswith(".safetensors"):
            total += (f.get("lfs") or {}).get("size") or f.get("size") or 0
    return total


def run_commands(variant):
    repo = variant["repo"]
    if variant["format"] == "gguf":
        tag = variant["quant"]
        return {
            "llama.cpp": f"llama-server -hf {repo}:{tag} --host 0.0.0.0 --port 8080 -ngl 99 --jinja",
            "Ollama": f"ollama run hf.co/{repo}:{tag}",
        }
    if variant["format"] == "mlx":
        return {"MLX (Mac)": f"mlx_lm.server --model {repo}"}
    if variant["format"] == "exl":
        return {}
    return {
        "vLLM": f"vllm serve {repo} --host 0.0.0.0 --port 8000 --gpu-memory-utilization 0.85",
        "SGLang": f"python -m sglang.launch_server --model-path {repo} --host 0.0.0.0 --port 30000",
    }


def collect_candidates():
    """Trending + recent repos across pipelines."""
    seen = {}
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=HF_NEW_DAYS)

    jobs = []
    for pipe in PIPELINES:
        jobs.append({"pipeline_tag": pipe, "sort": "trendingScore", "limit": HF_LIST_LIMIT})
    for pipe in ("text-generation", "image-text-to-text"):
        jobs.append({"pipeline_tag": pipe, "sort": "likes7d", "limit": HF_LIST_LIMIT})
        jobs.append({"pipeline_tag": pipe, "sort": "createdAt", "limit": HF_LIST_LIMIT * 2})
    jobs.append({"author": "nvidia", "sort": "createdAt", "limit": HF_LIST_LIMIT})
    # Explicit coding searches are not subject to the normal recency filter.
    # Otherwise mature but useful code-specialized repos can be excluded and
    # the UI can show Coding: 0 even though the collector supports the category.
    jobs.append({"pipeline_tag": "text-generation", "search": "coder", "sort": "downloads", "limit": HF_LIST_LIMIT, "_coding_discovery": True})
    for term in ("code generation", "coding", "codestral", "devstral", "starcoder", "codellama"):
        jobs.append({"pipeline_tag": "text-generation", "search": term, "sort": "downloads", "limit": HF_LIST_LIMIT, "_coding_discovery": True})

    # Consume results as they finish instead of waiting for the slowest Hub
    # query. This keeps both the dashboard log and progress bar alive during
    # the long model discovery stage.
    batches = [None] * len(jobs)
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(hf_list, **{k:v for k,v in job.items() if not k.startswith("_")}): index
                   for index, job in enumerate(jobs)}
        completed = 0
        for future in cf.as_completed(futures):
            index = futures[future]
            try:
                batches[index] = future.result() or []
            except Exception as exc:
                log(f"WARN Hugging Face listing failed ({index + 1}/{len(jobs)}): {exc}")
                batches[index] = []
            completed += 1
            log(f"Hugging Face candidate search: {completed}/{len(jobs)} listing batches complete")
            report_progress(2, f"Hugging Face candidate search: {completed}/{len(jobs)} listing batches complete",
                            stage_percent=min(35, int(completed * 35 / max(1, len(jobs)))))
    for job, batch in zip(jobs, batches):
        for info in batch:
            created = parse_iso(info.get("createdAt"))
            is_recent_sort = (info.get("likes") or 0) >= HF_NEW_MIN_LIKES
            if (not job.get("_coding_discovery") and created and created < cutoff
                    and not info.get("trendingScore") and not is_recent_sort):
                continue
            # If a repo was first returned by a general query and then found by
            # a specialist query, retain its metadata but mark it as a coding hit.
            rid = info.get("id")
            if not rid:
                continue
            if job.get("_coding_discovery"):
                info = dict(info)
                tags = list(info.get("tags") or [])
                if not any(str(t.get("name", "") if isinstance(t, dict) else t).lower() == "coding" for t in tags):
                    tags.append("coding")
                info["tags"] = tags
                if rid in seen:
                    existing = dict(seen[rid])
                    existing["tags"] = list(existing.get("tags") or []) + ["coding"]
                    seen[rid] = existing
                else:
                    seen[rid] = info
            else:
                seen.setdefault(rid, info)
    # Hub search can omit older specialist repos. Query a small known set by
    # exact ID only when the searches returned no coding hits, ensuring the
    # Coding category can recover from a search-index miss.
    if not any("coding" in categories_of(info.get("id", ""), info.get("pipeline_tag") or "text-generation", info.get("tags", []))
               for info in seen.values()):
        known_coders = (
            "Qwen/Qwen2.5-Coder-7B-Instruct",
            "deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct",
            "mistralai/Codestral-22B-v0.1",
            "google/codegemma-7b-it",
            "meta-llama/CodeLlama-13b-Instruct-hf",
        )
        log("Hugging Face coding search returned no matches; checking known coding model IDs")
        for rid, info in zip(known_coders, parallel_map(hf_model, known_coders)):
            if info:
                seen[rid] = info
        log(f"Hugging Face direct coding lookups: {sum(rid in seen for rid in known_coders)}/{len(known_coders)} found")
    return seen


def fetch_models(cache):
    log("Models: Hugging Face candidates")
    candidates = collect_candidates()
    log(f"  {len(candidates)} candidate repos")
    if not candidates:
        raise RuntimeError("Hugging Face returned no models (offline or rate-limited)")

    # Resolve each candidate to a base model; quant repos vote for their base.
    bases = {}
    for rid, info in candidates.items():
        base, relation = base_of(info.get("tags"))
        target = base if relation == "quantized" else rid
        entry = bases.setdefault(target, {"id": target, "signals": 0.0, "info": None})
        entry["signals"] += (info.get("trendingScore") or 0) + math.log1p(info.get("likes") or 0) * 5
        if target == rid:
            entry["info"] = info

    missing = [b for b, e in bases.items() if e["info"] is None]
    if missing:
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {pool.submit(hf_model, rid): rid for rid in missing}
            resolved = 0
            for future in cf.as_completed(futures):
                rid = futures[future]
                try:
                    info = future.result()
                except Exception as exc:  # source-level failure is retained/logged
                    log(f"WARN Hugging Face model metadata failed for {rid}: {exc}")
                    info = None
                if info:
                    bases[rid]["info"] = info
                resolved += 1
                report_progress(2, f"Hugging Face metadata resolution: {resolved}/{len(missing)} models checked",
                                stage_percent=35 + int(resolved * 30 / max(1, len(missing))))

    rows = []
    for entry in bases.values():
        info = entry["info"]
        if not info:
            continue
        params = param_count(info)
        if params and params / 1e9 > HF_MAX_PARAMS_B:
            continue
        rows.append((entry["signals"], info, params))

    # Reserve up to 15 slots for code-specialized models, then fill remaining
    # slots by relevance. This guarantees representation without letting coding
    # results crowd out the general trending list.
    rows.sort(key=lambda r: r[0], reverse=True)
    coding_rows = [r for r in rows if "coding" in categories_of(r[1].get("id", ""), r[1].get("pipeline_tag") or "text-generation", r[1].get("tags", []))]
    coding_rows = coding_rows[:min(15, HF_MAX_BASE_MODELS)]
    coding_ids = {r[1].get("id") for r in coding_rows}
    other_rows = [r for r in rows if r[1].get("id") not in coding_ids]
    rows = coding_rows + other_rows[:max(0, HF_MAX_BASE_MODELS-len(coding_rows))]
    rows.sort(key=lambda r: r[0], reverse=True)
    log(f"  {len(rows)} base models kept ({len(coding_rows)} code-specialized), resolving quantizations")

    first_seen = cache.setdefault("first_seen", {})
    today = dt.date.today().isoformat()
    models = []
    if rows:
        with cf.ThreadPoolExecutor(max_workers=4) as pool:
            futures = {pool.submit(build_model, r[1], r[2], r[0]): (index, r) for index, r in enumerate(rows)}
            built = 0
            ordered_models = {}
            for future in cf.as_completed(futures):
                index, row = futures[future]
                try:
                    model = future.result()
                    if model:
                        ordered_models[index] = model
                except Exception as exc:  # keep other model builds moving
                    rid = row[1].get("id", "unknown")
                    log(f"WARN Hugging Face quantization details failed for {rid}: {exc}")
                built += 1
                report_progress(2, f"Hugging Face quantization discovery: {built}/{len(rows)} base models processed",
                                stage_percent=65 + int(built * 34 / max(1, len(rows))))
            models = [ordered_models[i] for i in sorted(ordered_models)]

    for m in models:
        m["first_seen"] = first_seen.setdefault(m["id"], today)

    log(f"  {len(models)} models with Spark fit data")
    return models


def build_model(info, params, signal):
    rid = info["id"]
    total_b = round(params / 1e9, 2) if params else None
    active_b = active_params_b(rid, total_b) if total_b else None
    own_fmt = format_of(info.get("tags"), rid)

    # Quant repos for this base, best first.
    quants = hf_list(filter=f"base_model:quantized:{rid}", sort="downloads", limit=HF_MAX_QUANT_REPOS)
    quants.sort(key=lambda q: (q["id"].split("/")[0].lower() not in TRUSTED_QUANTIZERS, -(q.get("downloads") or 0)))

    variants = []
    trees_left = HF_MAX_TREE_PER_BASE
    formats_done = set()

    # Base repo itself (usually BF16, sometimes native FP8 / MXFP4).
    base_fmt = own_fmt or "bf16"
    base_size = None
    if total_b:
        base_size = round(total_b * FORMAT_BITS.get(base_fmt, 16) / 8, 1)
    variants.append({"repo": rid, "format": base_fmt, "quant": base_fmt.upper(), "size_gb": base_size,
                     "estimated": True, "downloads": info.get("downloads")})

    for q in quants:
        fmt = format_of(q.get("tags"), q["id"])
        if not fmt:
            continue
        org = q["id"].split("/")[0].lower()
        need_tree = fmt == "gguf" or (fmt not in formats_done)
        if need_tree and trees_left > 0:
            trees_left -= 1
            files = hf_tree(q["id"])
            if fmt == "gguf":
                for v in gguf_variants(q["id"], files):
                    v["downloads"] = q.get("downloads")
                    v["trusted"] = org in TRUSTED_QUANTIZERS
                    variants.append(v)
            else:
                size = safetensor_bytes(files)
                variants.append({"repo": q["id"], "format": fmt, "quant": fmt.upper(),
                                 "size_gb": round(size / GB, 1) if size else None,
                                 "downloads": q.get("downloads"), "trusted": org in TRUSTED_QUANTIZERS})
            formats_done.add(fmt)
            continue

        est = round(total_b * FORMAT_BITS.get(fmt, 4.5) / 8, 1) if total_b else None
        variants.append({"repo": q["id"], "format": fmt, "quant": fmt.upper(), "size_gb": est,
                         "estimated": True, "downloads": q.get("downloads"),
                         "trusted": org in TRUSTED_QUANTIZERS})

    # Keep the best repo per (format, quant).
    best = {}
    for v in variants:
        key = (v["format"], v["quant"])
        cur = best.get(key)
        rank = (bool(v.get("trusted")), not v.get("estimated"), v.get("downloads") or 0)
        if cur is None or rank > cur[0]:
            best[key] = (rank, v)
    variants = [v for _, v in best.values()]

    for v in variants:
        v["fit"] = fit_of(v.get("size_gb"))
        v["runtimes"] = FORMAT_RUNTIMES.get(v["format"], [])
        v["tok_s"] = est_tok_s(v.get("size_gb"), total_b, active_b) if v["format"] != "mlx" else None
        v["cmds"] = run_commands(v)
    variants.sort(key=lambda v: (v.get("size_gb") or 9e9))

    spark_variants = [v for v in variants if v["format"] != "mlx"]
    fits = [v for v in spark_variants if v["fit"] in ("fits", "tight")]
    if total_b and not fits:
        return None

    pipeline = info.get("pipeline_tag") or "text-generation"
    tags = info.get("tags") or []
    return {
        "id": rid,
        "org": rid.split("/")[0],
        "name": rid.split("/")[-1],
        "url": f"{HF_WEB}/{rid}",
        "pipeline": pipeline,
        "categories": categories_of(rid, pipeline, info.get("tags", [])),
        "official": rid.split("/")[0].lower() in OFFICIAL_ORGS,
        "params_b": total_b,
        "active_b": active_b,
        "moe": bool(total_b and active_b and active_b < total_b),
        "context": (info.get("gguf") or {}).get("context_length"),
        "license": license_of(tags),
        "created": (info.get("createdAt") or "")[:10],
        "modified": (info.get("lastModified") or "")[:10],
        "likes": info.get("likes") or 0,
        "downloads": info.get("downloads") or 0,
        "trending": info.get("trendingScore") or 0,
        "signal": round(signal, 1),
        "best_fit": best_variant(fits),
        "variants": variants,
    }


def best_variant(fits):
    """Highest quality build that still fits: prefer NVFP4, FP8, then GGUF Q8..Q4."""
    if not fits:
        return None
    order = ["nvfp4", "fp8", "mxfp4", "bf16", "awq", "gptq", "gguf", "int8", "exl", "bnb"]
    fits = sorted(fits, key=lambda v: (v["fit"] != "fits", order.index(v["format"]) if v["format"] in order else 99,
                                        -(v.get("size_gb") or 0)))
    v = fits[0]
    return {"repo": v["repo"], "quant": v["quant"], "size_gb": v.get("size_gb"), "fit": v["fit"]}


# ---------------------------------------------------------------------------
# Source 4: Artificial Analysis ratings (optional)
# ---------------------------------------------------------------------------

def norm_name(text):
    text = (text or "").lower().split("/")[-1]
    text = re.sub(r"(instruct|chat|-it\b|preview|reasoning|thinking|nvfp4|fp8|gguf|awq|bf16|hf)", "", text)
    return re.sub(r"[^a-z0-9]", "", text)


def fetch_ratings():
    key = os.environ.get("AA_API_KEY")
    if not key:
        log("Ratings: AA_API_KEY not set, skipping Artificial Analysis")
        return {}
    log("Ratings: Artificial Analysis")
    data = http_get(AA_URL, {"x-api-key": key})
    out = {}
    for item in data.get("data", []):
        ev = item.get("evaluations") or {}
        rec = {
            "name": item.get("name"),
            "intelligence": ev.get("artificial_analysis_intelligence_index"),
            "coding": ev.get("artificial_analysis_coding_index"),
            "math": ev.get("artificial_analysis_math_index"),
            "mmlu_pro": ev.get("mmlu_pro"),
            "gpqa": ev.get("gpqa"),
            "hle": ev.get("hle"),
            "livecodebench": ev.get("livecodebench"),
            "aime": ev.get("aime"),
            "source": "Artificial Analysis",
            "url": f"https://artificialanalysis.ai/models/{item.get('slug')}",
        }
        for name in (item.get("slug"), item.get("name")):
            if name:
                out.setdefault(norm_name(name), rec)
    log(f"  {len(out)} rated models")
    return out


def attach_ratings(models, ratings, overrides):
    for m in models:
        rec = overrides.get(m["id"])
        if rec is None:
            rec = ratings.get(norm_name(m["id"]))
        if rec is None:
            # Try base family name, e.g. 'Qwen3.6-35B-A3B-Instruct-2607' -> 'qwen3635ba3b'
            short = re.sub(r"-\d{4}$", "", m["name"])
            rec = ratings.get(norm_name(short))
        m["ratings"] = rec


# ---------------------------------------------------------------------------
# Source 5: NVIDIA developer forum (Discourse)
# ---------------------------------------------------------------------------

def forum_list(path):
    data = try_get(f"{FORUM}{path}", default={}) or {}
    return (data.get("topic_list") or {}).get("topics") or []


def tag_topic(title, tags, excerpt):
    """Tag from title + forum tags; fall back to the excerpt only if empty."""
    head = " ".join([title, " ".join(tags)]).lower()
    found = [key for key, (_, pattern) in FORUM_TOPICS.items() if re.search(pattern, head)]
    if not found:
        body = (excerpt or "").lower()[:300]
        found = [key for key, (_, pattern) in FORUM_TOPICS.items() if re.search(pattern, body)][:2]
    models = sorted(set(m.lower() for m in re.findall(MODEL_FAMILY_RE, head)))
    return found, models


def kind_of(title, accepted):
    low = title.lower()
    if accepted:
        return "solved"
    if re.search(FORUM_KIND_TIP, low):
        return "tip"
    if re.search(FORUM_KIND_ISSUE, low):
        return "issue"
    return "discussion"


def fetch_forum(cache):
    log("Community: NVIDIA DGX Spark forum")
    topics = {}

    paths = [f"/c/{FORUM_CAT_MAIN}/l/latest.json?page={p}" for p in range(FORUM_LATEST_PAGES)]
    paths += [f"/c/{FORUM_CAT_MAIN}/l/top.json?period=monthly",
              f"/c/{FORUM_CAT_MAIN}/l/top.json?period=weekly"]
    announce_path = f"/c/{FORUM_CAT_ANNOUNCE}/l/latest.json"

    for path in paths:
        for t in forum_list(path):
            topics.setdefault(t["id"], t)
        time.sleep(FORUM_SLEEP_S)

    announce_ids = set()
    for t in forum_list(announce_path):
        topics.setdefault(t["id"], t)
        announce_ids.add(t["id"])

    if not topics:
        raise RuntimeError("forum returned no topics")

    excerpts = cache.setdefault("forum_excerpts", {})
    ranked = sorted(topics.values(), key=lambda t: (t.get("like_count") or 0) + (t.get("views") or 0) / 200, reverse=True)
    for t in ranked[:FORUM_MAX_EXCERPTS]:
        key = str(t["id"])
        if key in excerpts:
            continue
        data = try_get(f"{FORUM}/t/{t['id']}.json")
        time.sleep(FORUM_SLEEP_S)
        if not data:
            continue
        posts = (data.get("post_stream") or {}).get("posts") or []
        excerpts[key] = strip_html(posts[0].get("cooked", ""))[:420] if posts else ""

    # Keep the excerpt cache bounded to live topics.
    for key in list(excerpts):
        if int(key) not in topics:
            del excerpts[key]

    out = []
    for t in topics.values():
        tags = tag_list(t.get("tags"))
        excerpt = excerpts.get(str(t["id"])) or strip_html(t.get("excerpt") or "")
        topic_tags, model_tags = tag_topic(t.get("title", ""), tags, excerpt)
        out.append({
            "id": t["id"],
            "title": t.get("title"),
            "url": f"{FORUM}/t/{t.get('slug')}/{t['id']}",
            "created": t.get("created_at"),
            "last": t.get("last_posted_at") or t.get("bumped_at"),
            "replies": t.get("reply_count") or max((t.get("posts_count") or 1) - 1, 0),
            "views": t.get("views") or 0,
            "likes": t.get("like_count") or 0,
            "tags": tags,
            "topics": topic_tags,
            "models": model_tags,
            "kind": "announcement" if t["id"] in announce_ids else kind_of(t.get("title", ""), t.get("has_accepted_answer")),
            "pinned": bool(t.get("pinned")),
            "excerpt": excerpt,
        })

    out.sort(key=lambda t: t["last"] or "", reverse=True)
    log(f"  {len(out)} topics")
    return out


# ---------------------------------------------------------------------------
# Joins
# ---------------------------------------------------------------------------

def build_supported(playbooks, recipes, validate):
    """Official model list: NVIDIA recipes + models named in playbooks."""
    rows = {}

    def row(handle):
        return rows.setdefault(handle.lower(), {
            "id": handle, "url": f"{HF_WEB}/{handle}", "playbooks": [], "recipes": [],
            "ollama": False,
        })

    for r in recipes:
        if not r.get("checkpoint"):
            continue
        handle = "/".join(r["checkpoint"].split("/")[:2])
        row(handle)["recipes"].append({
            "name": r["name"], "backend": r["backend"], "precision": r["precision"],
            "sparks": r["sparks"], "status": r["status"], "min_mem_gb": r["min_mem_gb"],
            "variant": r["variant"], "id": r["id"],
        })

    weak_all = set()
    for p in playbooks:
        for handle in p["models_strong"]:
            row(handle)["playbooks"].append(p["slug"])
        weak_all.update((h, p["slug"]) for h in p["models_weak"])
        for tag in p["ollama"]:
            r = row(f"ollama:{tag}")
            r["playbooks"].append(p["slug"])
            r["ollama"] = True
            r["url"] = f"https://ollama.com/library/{tag.split(':')[0]}"

    # Weak handles become rows only when the Hub confirms they exist.
    if validate:
        unique = sorted({h for h, _ in weak_all})
        verdict = dict(zip(unique, parallel_map(hf_exists, unique)))
        for handle, slug in weak_all:
            if verdict.get(handle):
                row(handle)["playbooks"].append(slug)

    out = []
    for r in rows.values():
        r["playbooks"] = sorted(set(r["playbooks"]))
        name = r["id"].split("/")[-1].replace("ollama:", "")
        r["name"] = name
        found = {x["precision"] for x in r["recipes"] if x.get("precision")}
        found |= set(re.findall(r"(?i)nvfp4|fp8|mxfp4|fp4|bf16|gguf|awq|int4|q4_k_m", name))
        found = {p.upper() for p in found}
        if "FP4" in found and r["id"].lower().startswith("nvidia/"):
            found.add("NVFP4")      # NVIDIA's '-FP4' checkpoints are NVFP4
        if any(p != "FP4" and "FP4" in p for p in found):
            found.discard("FP4")
        r["precision"] = sorted(found)
        r["backends"] = sorted({x["backend"] for x in r["recipes"] if x.get("backend")})
        r["single_spark"] = any(x["sparks"] == 1 for x in r["recipes"]) or not r["recipes"]
        out.append(r)

    out.sort(key=lambda r: (-len(r["recipes"]), -len(r["playbooks"]), r["id"].lower()))
    return out


def mark_official(models, supported):
    ids = {s["id"].lower() for s in supported}
    for m in models:
        repos = {m["id"].lower()} | {v["repo"].lower() for v in m["variants"]}
        m["nvidia_supported"] = bool(repos & ids)


def build_picks(models):
    """One suggested model per job, among builds that fit one Spark."""
    def score(m):
        r = m.get("ratings") or {}
        bench = r.get("intelligence") or 0
        return (bench, m.get("nvidia_supported", False), m["official"], m["signal"])

    def pick(pred, rating_key=None):
        pool = [m for m in models if m.get("best_fit") and m["best_fit"]["fit"] == "fits" and pred(m)]
        if rating_key:
            pool.sort(key=lambda m: ((m.get("ratings") or {}).get(rating_key) or 0,
                                     rating_key == "coding" and "coding" in m["categories"], score(m)), reverse=True)
        else:
            pool.sort(key=score, reverse=True)
        return pool[0]["id"] if pool else None

    small = lambda m: (m["active_b"] or 99) <= 8
    llm = lambda m: m["pipeline"] in ("text-generation", "image-text-to-text", "any-to-any")
    return {
        "coding": pick(lambda m: "coding" in m["categories"] or llm(m), "coding"),
        "reasoning": pick(lambda m: llm(m), "intelligence"),
        "autocomplete": pick(lambda m: m["pipeline"] == "text-generation" and small(m) and (m["params_b"] or 99) <= 14, "coding"),
        "vision": pick(lambda m: m["pipeline"] in ("image-text-to-text", "any-to-any")),
        "embedding": pick(lambda m: "embedding" in m["categories"]),
        "image": pick(lambda m: "image-gen" in m["categories"]),
        "speech": pick(lambda m: "speech" in m["categories"]),
        "jetson": pick(lambda m: llm(m) and (m["params_b"] or 99) <= 4),
    }


def build_changes(cache, playbooks, models):
    """Diff against last run for the Home 'what changed' list."""
    prev = cache.get("snapshot", {})
    changes = []
    today = dt.date.today().isoformat()

    prev_pb = prev.get("playbooks", {})
    for p in playbooks:
        old = prev_pb.get(p["slug"])
        if old is None and prev_pb:
            changes.append({"date": today, "kind": "playbook-new", "text": f"New playbook: {p['title']}", "url": p["url"]})
        elif old and old != p["updated"]:
            changes.append({"date": today, "kind": "playbook-updated", "text": f"Playbook updated: {p['title']} ({p['updated']})", "url": p["url"]})

    previous_models = prev.get("models", [])
    # Older cache versions stored a list of IDs; newer versions store the last
    # modified date too. Support both formats during upgrade.
    if isinstance(previous_models, dict):
        prev_model_map = {str(k): (str(v) if v else None) for k, v in previous_models.items()}
    elif isinstance(previous_models, list):
        prev_model_map = {str(item): None for item in previous_models if item}
    else:
        prev_model_map = {}
    if prev_model_map:
        for model in models:
            model_id = model.get("id")
            if not model_id:
                continue
            if model_id not in prev_model_map:
                changes.append({"date": today, "kind": "model-new", "text": f"New model on the list: {model_id}", "url": model.get("url") or f"{HF_WEB}/{model_id}"})
            else:
                old_modified = prev_model_map.get(model_id)
                new_modified = model.get("modified") or None
                if old_modified and new_modified and old_modified != new_modified:
                    changes.append({"date": today, "kind": "model-updated", "text": f"Model updated: {model_id} (modified {new_modified})", "url": model.get("url") or f"{HF_WEB}/{model_id}"})

    cache["snapshot"] = {
        "playbooks": {p["slug"]: p["updated"] for p in playbooks},
        "models": {m["id"]: (m.get("modified") or None) for m in models if m.get("id")},
    }
    # Keep both a cumulative, de-duplicated history and the exact delta from
    # this refresh. The dashboard uses the latter for run summaries.
    cache["changes_this_run"] = changes[:200]
    history = (changes + cache.get("changes", []))[:200]
    cache["changes"] = history
    return history


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_source(name, stage, func, previous, status):
    started = time.time()
    report_progress(stage, f"Fetching {name.replace('_', ' ')}")
    try:
        value = func()
        status[name] = {"ok": True, "at": now_iso(), "secs": round(time.time() - started, 1)}
        report_progress(stage, f"Fetched {name.replace('_', ' ')}", stage_percent=100)
        return value
    except Exception as err:  # noqa: BLE001
        log(f"ERROR {name}: {err}")
        prev_status = previous.get("status", {}).get(name, {})
        status[name] = {"ok": False, "at": prev_status.get("at"), "error": str(err)[:200]}
        report_progress(stage, f"{name.replace('_', ' ')} failed; retaining previous data: {err}", stage_percent=100)
        return previous.get(name)


def main():
    parser = argparse.ArgumentParser(description="Refresh data.js for Spark Hub")
    parser.add_argument("--out", default=DEFAULT_OUT, help="folder holding index.html")
    parser.add_argument("--only", choices=["playbooks", "recipes", "models", "ratings", "forum"],
                        action="append", help="refresh only these sources (repeatable)")
    parser.add_argument("--no-validate", action="store_true", help="skip Hub checks of playbook handles")
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)
    data_json = os.path.join(args.out, "data.json")
    cache_path = os.path.join(args.out, ".cache.json")
    previous = load_json(data_json, {})
    cache = load_json(cache_path, {})
    overrides = load_json(os.path.join(args.out, "ratings_overrides.json"), {})
    overrides = {k: v for k, v in overrides.items() if not k.startswith("_")}
    status = dict(previous.get("status", {}))
    wanted = set(args.only or ["playbooks", "recipes", "models", "ratings", "forum"])

    def keep(name):
        return previous.get(name)

    report_progress(1, "Collector initialized; preparing source refresh")
    models = run_source("models", 2, lambda: fetch_models(cache), previous, status) if "models" in wanted else keep("models")
    playbooks = run_source("playbooks", 3, lambda: fetch_playbooks(cache), previous, status) if "playbooks" in wanted else keep("playbooks")
    recipes = run_source("recipes", 4, fetch_recipes, previous, status) if "recipes" in wanted else keep("recipes")
    forum = run_source("forum", 5, lambda: fetch_forum(cache), previous, status) if "forum" in wanted else keep("forum")
    ratings = run_source("ratings", 6, fetch_ratings, previous, status) if "ratings" in wanted else keep("ratings")

    playbooks, recipes, models, forum = playbooks or [], recipes or [], models or [], forum or []
    ratings = ratings or {}

    report_progress(7, "Building supported-model data and applying ratings")
    supported = build_supported(playbooks, recipes, validate=not args.no_validate and "playbooks" in wanted)
    if "playbooks" not in wanted and previous.get("supported"):
        supported = previous["supported"]
    attach_ratings(models, ratings, overrides)
    mark_official(models, supported)

    report_progress(8, "Comparing the current snapshot and detecting changes")
    changes = build_changes(cache, playbooks, models)

    report_progress(9, "Serializing and writing data files")
    data = {
        "schema": SCHEMA_VERSION,
        "generated": now_iso(),
        "spark": {"mem_gb": SPARK_MEM_GB, "usable_gb": SPARK_USABLE_GB, "fit_ok_gb": SPARK_FIT_OK_GB,
                  "bw_gbs": SPARK_BW_GBS, "efficiency": DECODE_EFFICIENCY},
        "status": status,
        "playbooks": playbooks,
        "recipes": recipes,
        "supported": supported,
        "models": models,
        "ratings": ratings,
        "picks": build_picks(models),
        "forum": forum,
        "forum_topics": {k: v[0] for k, v in FORUM_TOPICS.items()},
        "changes": changes,
        "changes_this_run": cache.get("changes_this_run", []),
    }

    text = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    report_progress(9, "Writing generated data files")
    write_atomic(data_json, text)
    write_atomic(os.path.join(args.out, "data.js"), "window.SPARK_DATA=" + text + ";\n")
    write_atomic(cache_path, json.dumps(cache, ensure_ascii=False))

    report_progress(10, "Refresh data files saved; finalizing", stage_percent=90)

    bad = [k for k, v in status.items() if not v.get("ok") and k in wanted]
    log(f"Done: {len(models)} models, {len(playbooks)} playbooks, {len(recipes)} recipes, "
        f"{len(forum)} forum topics. Failed: {', '.join(bad) or 'none'}")
    report_progress(10, f"Refresh finished. Failed sources: {', '.join(bad) or 'none'}",
                    state="completed" if not bad else "completed_with_errors", stage_percent=100)
    return 1 if len(bad) == len(wanted) else 0


if __name__ == "__main__":
    sys.exit(main())
