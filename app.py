import customtkinter as ctk
import asyncio, threading, json, os, re, time, random, subprocess, socket, csv, logging, webbrowser
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
from tkinter import messagebox

# ── Logging fichier ───────────────────────────────────────────────────────────

class RateLimitError(Exception):
    """Raised when Threads returns HTTP 429."""
    pass

logging.basicConfig(
    filename="actions.log", level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
def log_file(action, pseudo, score=None, reason=None):
    logging.info(f"{action} | @{pseudo} | score={score} | {reason or ''}")

def trace(scope, message):
    logging.info(f"[{scope}] {message}")

def trace_exc(scope, message, exc=None):
    if exc:
        logging.error(f"[{scope}] {message}: {type(exc).__name__}: {exc}")
    else:
        logging.error(f"[{scope}] {message}")

def log_verbose(msg):
    """Write detailed logs to file only (not shown in GUI)."""
    logging.debug(f"[VERBOSE] {msg}")

# ── Profils de sécurité ───────────────────────────────────────────────────────
SAFETY_PROFILES = {
    "Prudent":  {"limit_day": 160, "limit_hour": 25, "pause_min": 15,
                 "pause_max": 30,  "scan_batch": 80,  "clean_batch": 160,
                 "anti_bot_every": 15},
    "Normal":   {"limit_day": 300, "limit_hour": 40, "pause_min": 8,
                 "pause_max": 15,  "scan_batch": 120, "clean_batch": 300,
                 "anti_bot_every": 20},
    "Agressif": {"limit_day": 500, "limit_hour": 50, "pause_min": 5,
                 "pause_max": 10,  "scan_batch": 150, "clean_batch": 500,
                 "anti_bot_every": 25},
}
_profile = SAFETY_PROFILES["Normal"].copy()

# ── Error detection thresholds ────────────────────────────────────────────
CONSECUTIVE_ERROR_LIMIT = 8   # Auto-stop after N consecutive errors
ERROR_RATE_WINDOW = 20        # Check error rate over last N operations
ERROR_RATE_THRESHOLD = 0.6    # Stop if >60% errors in the window

def get_limit_day():      return _profile["limit_day"]
def get_limit_hour():     return _profile["limit_hour"]
def get_pause_min():      return _profile["pause_min"]
def get_pause_max():      return _profile["pause_max"]
def get_scan_batch():     return _profile["scan_batch"]
def get_clean_batch():    return _profile["clean_batch"]
def get_anti_bot_every(): return _profile["anti_bot_every"]

# ── Config ────────────────────────────────────────────────────────────────────
DB_FILE            = "followers_db.json"
RESCAN_DAYS        = 14
CDP_PORT           = 9222
FETCH_MAX_DURATION = 1800
CHROME_PROFILE     = os.path.join(os.path.dirname(__file__), "chrome_profile")
CHROME_PATHS       = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"),
]
BLOCK_FILTER = "**/*.{png,jpg,jpeg,gif,woff2,woff,ttf,mp4,webp,ico}"

NO_REPLY_PATTERNS = [
    r"no replies yet",
    r"pas encore de r[ée]ponse",
    r"aucune r[ée]ponse",
    r"nothing here yet",
    r"rien pour l.instant",
    r"hasn.t replied",
    r"n.a pas encore r[ée]pondu",
    r"aucune r[ée]ponse pour le moment",
]

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")
_task_lock = threading.Lock()

# ── Sleep interruptible ───────────────────────────────────────────────────────
async def isleep(seconds: float, stop_event: threading.Event, step: float = 0.2):
    loop = asyncio.get_running_loop()
    end  = loop.time() + seconds
    while loop.time() < end:
        if stop_event.is_set():
            return
        await asyncio.sleep(min(step, end - loop.time()))

# ── DB ────────────────────────────────────────────────────────────────────────
def load_db():
    if not os.path.exists(DB_FILE):
        return {"followers": {}, "daily": {"date": "", "count": 0},
                "hourly": {"hour": "", "count": 0},
                "username": "", "whitelist": []}
    try:
        with open(DB_FILE, encoding="utf-8") as f:
            db = json.load(f)
            db.setdefault("whitelist", [])
            return db
    except Exception:
        backup = DB_FILE + ".corrupted"
        if os.path.exists(backup):
            os.remove(backup)
        os.rename(DB_FILE, backup)
        return {"followers": {}, "daily": {"date": "", "count": 0},
                "hourly": {"hour": "", "count": 0},
                "username": "", "whitelist": []}

def save_db(db):
    tmp  = DB_FILE + ".tmp"
    data = json.dumps(db, separators=(",", ":"))
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, DB_FILE)

def can_act(db):
    now   = datetime.now()
    today = now.strftime("%Y-%m-%d")
    hour  = now.strftime("%Y-%m-%d-%H")
    if db["daily"]["date"] != today:
        db["daily"] = {"date": today, "count": 0}
    if db.get("hourly", {}).get("hour") != hour:
        db["hourly"] = {"hour": hour, "count": 0}
    return (db["daily"]["count"] < get_limit_day() and
            db["hourly"]["count"] < get_limit_hour())

def seconds_until_next_hour():
    now = datetime.now()
    return (60 - now.minute) * 60 - now.second

def log_action(db):
    db["daily"]["count"] += 1
    db.setdefault("hourly", {})
    db["hourly"]["count"] = db["hourly"].get("count", 0) + 1
    db["hourly"]["hour"]  = datetime.now().strftime("%Y-%m-%d-%H")
    save_db(db)

def is_recent(ts, days=RESCAN_DAYS):
    if not ts:
        return False
    return (datetime.now().timestamp() - ts) < days * 86400

def get_pending(db):
    return [
        u for u, d in db["followers"].items()
        if d["status"] == "pending"
        or (d["status"] in ("scanned", "not_found")
            and not is_recent(d.get("scanned_at"), RESCAN_DAYS))
    ]

def get_fakes(db, threshold):
    whitelist = set(db.get("whitelist", []))
    return [(u, d) for u, d in db["followers"].items()
            if d["status"] == "scanned"
            and (d.get("score") or 0) >= threshold
            and u not in whitelist]

def count_known_followers(db):
    return len(db["followers"])

def export_csv(db, path="export.csv"):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["pseudo", "statut", "score", "privé",
                    "posts", "a_bio", "a_reponses", "photo",
                    "nom", "lien_ig", "followers", "scanned_at",
                    "refollow_count", "last_refollow", "removed_at",
                    "is_spambot"])
        for pseudo, d in db["followers"].items():
            ts = d.get("scanned_at")
            rm = d.get("removed_at")
            rf = d.get("last_refollow_at")
            w.writerow([
                pseudo, d.get("status", ""), d.get("score", ""),
                d.get("is_private", ""), d.get("threads_articles", ""),
                d.get("has_bio", ""), d.get("has_replies", ""),
                d.get("has_real_pic", ""), d.get("has_full_name", ""),
                d.get("has_ig_link", ""), d.get("follower_count", ""),
                datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M") if ts else "",
                d.get("refollow_count", 0),
                datetime.fromtimestamp(rf).strftime("%Y-%m-%d %H:%M") if rf else "",
                datetime.fromtimestamp(rm).strftime("%Y-%m-%d %H:%M") if rm else "",
                d.get("is_spambot", False),
            ])
    return os.path.abspath(path)

# ── Chrome ────────────────────────────────────────────────────────────────────
def find_chrome():
    for p in CHROME_PATHS:
        if os.path.exists(p):
            return p
    return None

def is_port_open():
    try:
        with socket.create_connection(("127.0.0.1", CDP_PORT), timeout=1):
            return True
    except OSError:
        return False

def launch_chrome():
    if is_port_open():
        return True
    path = find_chrome()
    if not path:
        return False
    subprocess.run(["taskkill", "/F", "/IM", "chrome.exe", "/T"],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)
    subprocess.Popen([path,
                      f"--remote-debugging-port={CDP_PORT}",
                      f"--user-data-dir={CHROME_PROFILE}",
                      "--no-first-run", "--no-default-browser-check",
                      "--start-maximized", "https://www.threads.net"])
    for _ in range(20):
        time.sleep(1)
        if is_port_open():
            return True
    return False

# ── Helpers Playwright ────────────────────────────────────────────────────────
async def _wait_for_profile(page, timeout_ms: int = 6000):
    try:
        await page.wait_for_function(
            "() => document.querySelector('header,main,h1,"
            "[data-pressable-container]') !== null",
            timeout=timeout_ms
        )
    except Exception:
        pass

async def _get_page(browser):
    contexts = browser.contexts
    if contexts:
        ctx   = contexts[0]
        pages = ctx.pages
        page  = pages[0] if pages else await ctx.new_page()
    else:
        ctx  = await browser.new_context()
        page = await ctx.new_page()
    return ctx, page

async def _reset_page(page):
    try:
        await asyncio.wait_for(
            page.goto("about:blank", wait_until="commit"), timeout=4.0
        )
    except Exception:
        pass


async def _navigate_to_profile(page, username, log_fn=None, timeout=15000):
    """Navigate to a Threads profile, handling post-page redirects.
    Returns True if we're on the profile page.
    Raises RateLimitError on HTTP 429."""
    profile_url = f"https://www.threads.net/@{username}"

    for attempt in range(3):
        try:
            await page.goto(profile_url,
                            wait_until="domcontentloaded", timeout=timeout)
            await _wait_for_profile(page)
            await asyncio.sleep(1.5)
        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "ERR_HTTP_RESPONSE_CODE_FAILURE" in err_str:
                # Check if it's really a 429 by looking at the page
                try:
                    body = await page.evaluate(
                        "() => document.body?.innerText || ''")
                    if "429" in body or "cette page ne fonctionne pas" in body.lower():
                        raise RateLimitError("HTTP 429")
                except RateLimitError:
                    raise
                except Exception:
                    pass
            trace("NAV", f"@{username}: goto err {type(e).__name__}")
            if attempt == 2:
                return False
            continue

        # Check for 429 in loaded page body
        try:
            body_check = await page.evaluate(
                "() => (document.body?.innerText || '').substring(0, 300)")
            if ("429" in body_check and "cette page ne fonctionne pas" in body_check.lower()) \
               or "too many requests" in body_check.lower():
                raise RateLimitError("HTTP 429 in page body")
        except RateLimitError:
            raise
        except Exception:
            pass

        # Check: are we on the profile and not on a post page?
        url = page.url
        on_profile = (f"/@{username}" in url.lower()
                      and "/post/" not in url
                      and "/p/" not in url)

        if on_profile:
            trace("NAV", f"@{username}: OK {url[:60]}")
            return True

        # Wrong page — try force navigation
        trace("NAV", f"@{username}: redirect to {url[:60]}, retry {attempt+1}")
        if log_fn:
            log_fn(f"  ⚠️ Redirigé vers {url[:50]}... retry")
        try:
            await page.evaluate(f"() => window.location.href = '{profile_url}'")
            await asyncio.sleep(3)
            await _wait_for_profile(page)
            url = page.url
            if f"/@{username}" in url.lower() and "/post/" not in url:
                return True
        except Exception:
            pass

    trace("NAV", f"@{username}: ÉCHEC après 3 tentatives")
    return False

# ── JS autoscroll ─────────────────────────────────────────────────────────────
_JS_MARK_CONTAINER = r"""
    () => {
        // Try dialog first, then any container with profile links
        let links = Array.from(
            document.querySelectorAll('div[role="dialog"] a[href*="/@"]')
        );
        // Fallback: find profile links anywhere (for panels without role=dialog)
        if (!links.length) {
            links = Array.from(document.querySelectorAll('a[href*="/@"]'))
                .filter(a => {
                    const h = a.getAttribute('href') || '';
                    return /^\/@[\w.]+$/.test(h);
                });
        }
        if (!links.length) return {ok: false, reason: 'no_links'};
        let el = links[links.length - 1].parentElement;
        while (el && el !== document.body) {
            const oy = window.getComputedStyle(el).overflowY;
            if ((oy === 'scroll' || oy === 'auto')
                && el.scrollHeight > el.clientHeight + 10) {
                el.setAttribute('data-autoscroll', 'true');
                return {ok: true, links: links.length, height: el.scrollHeight};
            }
            el = el.parentElement;
        }
        return {ok: false, reason: 'no_scrollable', links: links.length};
    }
"""
_JS_START_SCROLL = """
    (speed) => {
        const el = document.querySelector('[data-autoscroll="true"]');
        if (!el) return;
        if (window._autoScrollId) clearInterval(window._autoScrollId);
        window._autoScrollId = setInterval(() => { el.scrollTop += speed; }, 16);
    }
"""
_JS_STOP_SCROLL = """
    () => {
        if (window._autoScrollId) {
            clearInterval(window._autoScrollId);
            window._autoScrollId = null;
        }
    }
"""

# ── JS: Resolve user_id ────────────────────────────────────────────────────
JS_RESOLVE_USER_ID = r"""
async (username) => {
    const log = [];
    const csrf = (document.cookie.match(/csrftoken=([^;]+)/)||[])[1]||'';
    const headers = {
        'X-IG-App-ID': '238260118697367',
        'Accept': 'application/json',
        'X-Requested-With': 'XMLHttpRequest',
    };
    if (csrf) headers['X-CSRFToken'] = csrf;
    const endpoints = [
        `https://www.threads.net/api/v1/users/web_profile_info/?username=${username}`,
        `https://www.threads.net/api/v1/users/search/?q=${username}`,
    ];
    for (const url of endpoints) {
        try {
            log.push('try: ' + url.substring(0, 80));
            const r = await fetch(url, { credentials: 'include', headers });
            log.push('status: ' + r.status);
            if (!r.ok) continue;
            const j = await r.json();
            const uid = j?.data?.user?.id || j?.data?.user?.pk
                || j?.user?.pk || j?.user?.id || j?.data?.user?.pk_id;
            if (uid) { log.push('found: ' + uid); return {uid: String(uid), log}; }
            const users = j?.users || [];
            const match = users.find(u => u.username === username);
            if (match) { log.push('search: ' + (match.pk||match.id)); return {uid: String(match.pk || match.id), log}; }
            log.push('no_uid');
        } catch(e) { log.push('err: ' + e.toString()); }
    }
    try {
        const scripts = document.querySelectorAll('script[type="application/json"]');
        for (const s of scripts) {
            const text = s.textContent || '';
            if (text.includes(username)) {
                const pkM = text.match(/"pk":"?(\d+)"?/);
                if (pkM) { log.push('embed_pk: ' + pkM[1]); return {uid: pkM[1], log}; }
                const idM = text.match(/"user_id":"?(\d+)"?/);
                if (idM) { log.push('embed_id: ' + idM[1]); return {uid: idM[1], log}; }
            }
        }
    } catch(e) { log.push('embed_err: ' + e.toString()); }
    log.push('NOT_FOUND');
    return {uid: null, log};
}
"""


async def _extract_csrf_token(page):
    """Extract csrftoken from cookies via JS."""
    try:
        csrf = await page.evaluate(
            "() => (document.cookie.match(/csrftoken=([^;]+)/)||[])[1]||''"
        )
        if csrf:
            trace("CSRF", f"OK: {csrf[:12]}...")
        return csrf or ""
    except Exception as e:
        trace("CSRF", f"err: {type(e).__name__}")
        return ""


async def _fetch_page_api(page, user_id, max_id, csrf_token=""):
    """Fetch one page of followers via internal API (runs inside browser JS)."""
    cursor = f"&max_id={max_id}" if max_id else ""
    url = (f"https://www.threads.net/api/v1/friendships/{user_id}/followers/"
           f"?count=50&search_surface=follow_list_page{cursor}")
    try:
        result = await page.evaluate(r"""
        async ([url, csrf]) => {
            try {
                const h = {
                    'X-IG-App-ID': '238260118697367',
                    'Accept': 'application/json',
                    'X-Requested-With': 'XMLHttpRequest',
                };
                if (csrf) h['X-CSRFToken'] = csrf;
                else {
                    const m = document.cookie.match(/csrftoken=([^;]+)/);
                    if (m) h['X-CSRFToken'] = m[1];
                }
                const r = await fetch(url, { credentials: 'include', headers: h });
                if (!r.ok) return {http_error: r.status, body: (await r.text()).substring(0, 300)};
                return await r.json();
            } catch(e) { return {error: 'fetch: ' + e.toString()}; }
        }
        """, [url, csrf_token])
        return result or {"error": "null response"}
    except Exception as e:
        return {"error": f"evaluate: {type(e).__name__}"}


# ── Fetch abonnés ─────────────────────────────────────────────────────────────
def _save_collected_to_db(collected, db, username):
    """Save collected followers to DB incrementally. Detects re-follows."""
    new = 0
    refollows = 0
    for pseudo, api_data in collected.items():
        if pseudo == username:
            continue
        if pseudo not in db["followers"]:
            db["followers"][pseudo] = {
                "status": "pending", "score": None, "scanned_at": None,
                **api_data,
            }
            new += 1
        else:
            existing = db["followers"][pseudo]
            # ── Re-follow detection ──────────────────────────────────
            if existing["status"] in ("removed", "blocked"):
                removed_at = existing.get("removed_at")
                delay_h = ""
                if removed_at:
                    delta = datetime.now().timestamp() - removed_at
                    delay_h = f" (re-follow après {delta/3600:.1f}h)"
                logging.warning(
                    f"RE-FOLLOW | @{pseudo} | was {existing['status']}"
                    f" | old_score={existing.get('score')}{delay_h}"
                )
                existing["status"] = "pending"
                existing["score"] = None
                existing["scanned_at"] = None
                existing["refollow_count"] = existing.get("refollow_count", 0) + 1
                existing["last_refollow_at"] = datetime.now().timestamp()
                refollows += 1
            # Update metadata only for already-active followers
            for k in ("follower_count", "is_verified", "full_name", "is_private"):
                if api_data.get(k) is not None:
                    db["followers"][pseudo][k] = api_data[k]
    if new > 0 or refollows > 0:
        save_db(db)
    return new, refollows


async def _try_api_fetch(page, db, username, log_fn, stop_event):
    """Try to fetch all followers via API pagination. Returns True if it worked."""
    trace("API_FETCH", f"Tentative API pour @{username}")
    log_fn("  \U0001f50d Tentative fetch via API...")

    # Step 1: Resolve user_id
    try:
        raw = await page.evaluate(JS_RESOLVE_USER_ID, username)
        if isinstance(raw, dict):
            for line in raw.get("log", []):
                trace("API_FETCH", f"  {line}")
            user_id = raw.get("uid")
        else:
            user_id = raw
    except Exception as e:
        trace("API_FETCH", f"resolve err: {type(e).__name__}")
        user_id = None

    if not user_id:
        log_fn("  \u274c user_id introuvable via API")
        return False

    log_fn(f"  \u2705 user_id={user_id}")

    # Step 2: Get CSRF token
    csrf = await _extract_csrf_token(page)
    if not csrf:
        log_fn("  \u26a0\ufe0f Pas de CSRF token")

    # Step 3: Test first page
    log_fn("  \U0001f9ea Test API followers...")
    first = await _fetch_page_api(page, user_id, None, csrf)
    if not first or "error" in first or "http_error" in first:
        err = first.get("error") or first.get("http_error") or "?"
        log_fn(f"  \u274c API test: {str(err)[:80]}")
        return False

    users = first.get("users", [])
    if not users:
        log_fn("  \u274c API: 0 users")
        return False

    log_fn(f"  \u2705 API OK \u2014 {len(users)} premiers followers")

    # Step 4: Paginate
    collected = {}
    for u in users:
        pseudo = (u.get("username") or "").strip()
        if pseudo and pseudo != username:
            collected[pseudo] = {
                "follower_count": u.get("follower_count"),
                "is_verified": u.get("is_verified", False),
                "full_name": (u.get("full_name") or "").strip(),
                "is_private": u.get("is_private", False),
            }

    max_id = first.get("next_max_id")
    page_num = 1
    errors = 0

    # Sauvegarde immédiate de la première page
    _new, _refollow = _save_collected_to_db(collected, db, username)
    rf_tag = f" | ♻️{_refollow} re-follows" if _refollow else ""
    log_fn(f"  📄 Page 1: {len(collected)} followers | DB={len(db['followers'])}{rf_tag}")

    while max_id and not stop_event.is_set():
        page_num += 1
        await isleep(random.uniform(0.8, 1.5 + page_num * 0.02), stop_event)
        if stop_event.is_set():
            break

        t0 = time.time()
        result = await _fetch_page_api(page, user_id, max_id, csrf)
        t_api = time.time() - t0

        if not result or "error" in result:
            errors += 1
            log_fn(f"  \u26a0\ufe0f Page {page_num}: {str(result.get('error',''))[:60]} ({t_api:.1f}s)")
            if errors >= 3:
                break
            await isleep(random.uniform(3, 6), stop_event)
            continue

        if "http_error" in result:
            status = result["http_error"]
            if status == 429:
                pause = random.uniform(60, 120)
                log_fn(f"  \u26a0\ufe0f 429 Rate limit \u2014 pause {int(pause)}s")
                await isleep(pause, stop_event)
                continue
            elif status in (401, 403):
                log_fn(f"  \u274c Auth {status}")
                break
            errors += 1
            if errors >= 3:
                break
            continue

        errors = 0
        new_batch = 0
        users_in_page = result.get("users", [])
        for u in users_in_page:
            pseudo = (u.get("username") or "").strip()
            if pseudo and pseudo != username:
                collected[pseudo] = {
                    "follower_count": u.get("follower_count"),
                    "is_verified": u.get("is_verified", False),
                    "full_name": (u.get("full_name") or "").strip(),
                    "is_private": u.get("is_private", False),
                }

        # ── Sauvegarde incrémentale après chaque page ─────────────
        new_batch, rf_batch = _save_collected_to_db(collected, db, username)

        rf_tag = f" | ♻️{rf_batch} re-follows" if rf_batch else ""
        log_fn(f"  📄 Page {page_num}: +{len(users_in_page)} ({t_api:.1f}s) | "
               f"total={len(collected)} | DB={len(db['followers'])}{rf_tag}")
        max_id = result.get("next_max_id")
        if not max_id:
            log_fn("  ✅ Fin pagination API")

    # Sauvegarde finale
    _fn, _fr = _save_collected_to_db(collected, db, username)
    total_rf = sum(1 for d in db["followers"].values() if d.get("refollow_count", 0) > 0
                   and d["status"] == "pending")
    rf_msg = f" | ♻️ {total_rf} re-follows détectés" if total_rf else ""
    log_fn(f"✅ API: {len(db['followers'])} en DB ({len(collected)} collectés{rf_msg})")
    return True


async def fetch_followers_async(db, log_fn, stop_event: threading.Event,
                                scroll_speed: int = 120):
    username = db["username"]
    log_fn(f"📥 Récupération des abonnés de @{username}...")

    # Internal logs go to file only
    _vlog = log_verbose

    async with async_playwright() as p:
        browser   = await p.chromium.connect_over_cdp("http://127.0.0.1:9222")
        ctx, page = await _get_page(browser)
        try:
            # ── Navigation vers le profil ─────────────────────────────
            if not await _navigate_to_profile(page, username, _vlog):
                log_fn("  ❌ Navigation vers le profil impossible.")
                return
            _vlog(f"URL: {page.url[:60]}")
            if stop_event.is_set():
                return

            # ── Tentative API (rapide, pas de scroll) ─────────────────
            t_api_start = time.time()
            api_ok = await _try_api_fetch(page, db, username, _vlog, stop_event)
            trace("FETCH", f"API tentative: {time.time()-t_api_start:.1f}s ok={api_ok}")
            if api_ok:
                total_rf = sum(1 for d in db["followers"].values()
                               if d.get("refollow_count", 0) > 0
                               and d["status"] == "pending")
                rf_msg = f" | ♻️ {total_rf} re-follows" if total_rf else ""
                log_fn(f"  ✅ {len(db['followers'])} abonnés récupérés via API{rf_msg}")
                return

            # ── Fallback : scroll ─────────────────────────────────────
            log_fn("  ⚠️ API indisponible — récupération par scroll...")

            # Diagnostic DOM avant de tenter le clic
            try:
                diag = await page.evaluate(r"""
                () => {
                    const url = location.href;
                    const body = document.body?.innerText?.substring(0, 300) || '';
                    const links = Array.from(document.querySelectorAll('a')).map(a => ({
                        text: (a.textContent || '').trim().substring(0, 40),
                        href: (a.href || '').substring(0, 60),
                    })).filter(l => l.text.match(/follow|abonn/i) || l.href.includes('follow'));
                    const dialogs = document.querySelectorAll('[role="dialog"]').length;
                    const buttons = Array.from(document.querySelectorAll('button, [role="button"]')).length;
                    return {url, body_len: body.length, follower_links: links, dialogs, buttons,
                            body_start: body.substring(0, 150)};
                }
                """)
                trace("FETCH_DIAG", f"URL={diag.get('url','?')}")
                trace("FETCH_DIAG", f"Body={diag.get('body_len',0)}c: {diag.get('body_start','')[:100]}")
                trace("FETCH_DIAG", f"Follower links: {diag.get('follower_links',[])}")
                trace("FETCH_DIAG", f"Dialogs={diag.get('dialogs',0)} Buttons={diag.get('buttons',0)}")
                _vlog(f"  📊 Page: {diag.get('buttons',0)} boutons, "
                       f"{len(diag.get('follower_links',[]))} liens followers, "
                       f"{diag.get('dialogs',0)} dialogs")
            except Exception as e:
                trace("FETCH_DIAG", f"Err: {type(e).__name__}")

            # Clic bouton abonnés — UNIQUEMENT le lien de la liste followers
            # PAS de recherche de texte "abonnés" car ça matche les posts
            clicked = False
            for attempt in range(3):
                if clicked or stop_event.is_set():
                    break

                if attempt > 0:
                    _vlog(f"  🔄 Retry clic abonnés ({attempt+1}/3)...")
                    await isleep(2, stop_event)

                # Méthode 1 (prioritaire): Lien <a> avec href contenant "followers"
                if not clicked:
                    try:
                        el = page.locator("a[href*='followers']").first
                        if await asyncio.wait_for(el.is_visible(), timeout=2.0):
                            await el.click()
                            clicked = True
                            _vlog("  🔗 Clic abonnés: a[href*=followers]")
                    except Exception:
                        pass

                # Méthode 2: JS — chercher un élément dont le texte COMPLET
                # est "X followers" ou "X abonnés" (pas un post qui contient le mot)
                if not clicked:
                    try:
                        js_clicked = await page.evaluate(r"""
                        () => {
                            // Only look at links and small spans (not post content)
                            const candidates = document.querySelectorAll('a, span, header *');
                            for (const el of candidates) {
                                const t = (el.textContent || '').trim();
                                // Must be EXACTLY "X followers" or "X abonnés" — nothing else
                                if (/^\d[\d,.\s\u00a0\u202fKkMm]*\s*(followers|abonnés)$/i.test(t)) {
                                    // Extra check: must be small (not a post paragraph)
                                    const r = el.getBoundingClientRect();
                                    if (r.height < 50) {
                                        el.click();
                                        return t;
                                    }
                                }
                            }
                            return false;
                        }
                        """)
                        if js_clicked:
                            clicked = True
                            _vlog(f"  🔗 Clic abonnés: JS '{js_clicked}'")
                    except Exception:
                        pass

                # Méthode 3: Playwright get_by_text UNIQUEMENT avec le pattern strict
                # (nombre + followers/abonnés, rien d'autre)
                if not clicked:
                    try:
                        btn = page.get_by_text(
                            re.compile(r"^\d[\d,.\s]*\s*(abonnés|followers)$", re.IGNORECASE)
                        ).first
                        if await asyncio.wait_for(btn.is_visible(), timeout=2.0):
                            await btn.click()
                            clicked = True
                            _vlog("  🔗 Clic abonnés: texte strict")
                    except Exception:
                        pass

            if not clicked:
                await page.screenshot(path="debug_fetch.png")
                _vlog("Bouton abonnés introuvable (debug_fetch.png sauvegardé).")
                log_fn("  ❌ Bouton abonnés introuvable")
                return

            await isleep(4, stop_event)
            if stop_event.is_set():
                return

            # ── Trouver le conteneur scrollable (pas de detection dialog) ─
            # On essaie directement de trouver le container avec des liens
            async def start_scroll():
                if stop_event.is_set():
                    return
                t = time.time()
                mark = await page.evaluate(_JS_MARK_CONTAINER)
                await page.evaluate(_JS_START_SCROLL, scroll_speed)
                trace("SCROLL", f"start_scroll: {time.time()-t:.2f}s mark={mark}")
                return mark

            async def stop_scroll():
                try:
                    t = time.time()
                    await page.evaluate(_JS_STOP_SCROLL)
                    trace("SCROLL", f"stop_scroll: {time.time()-t:.2f}s")
                except Exception:
                    pass

            # Essayer de trouver le container scrollable
            container_found = False
            for attempt in range(8):
                if stop_event.is_set():
                    break
                mark = await page.evaluate(_JS_MARK_CONTAINER)
                trace("FETCH", f"Mark attempt {attempt+1}: {mark}")

                if mark and isinstance(mark, dict) and mark.get("ok"):
                    _vlog(f"  ✅ Container trouvé: {mark.get('links',0)} liens "
                           f"(h={mark.get('height',0)})")
                    container_found = True
                    break

                if attempt >= 2:
                    _vlog(f"  ⚠️ Container pas trouvé ({attempt+1}/8)... "
                           f"{mark}")
                    # Dump DOM pour comprendre
                    if attempt == 3:
                        try:
                            dump = await page.evaluate(r"""
                            () => {
                                const all_a = document.querySelectorAll('a');
                                const hrefs = Array.from(all_a).slice(0, 30).map(a => 
                                    (a.getAttribute('href') || '').substring(0, 50));
                                const scrollables = [];
                                document.querySelectorAll('*').forEach(el => {
                                    const s = window.getComputedStyle(el);
                                    if ((s.overflowY === 'scroll' || s.overflowY === 'auto')
                                        && el.scrollHeight > el.clientHeight + 50) {
                                        scrollables.push({
                                            tag: el.tagName,
                                            role: el.getAttribute('role'),
                                            h: el.scrollHeight,
                                            children: el.children.length,
                                        });
                                    }
                                });
                                return {
                                    url: location.href,
                                    total_links: all_a.length,
                                    sample_hrefs: hrefs,
                                    scrollables: scrollables.slice(0, 5),
                                    body_len: (document.body?.innerText||'').length,
                                };
                            }
                            """)
                            trace("FETCH_DOM", f"Dump: {dump}")
                            _vlog(f"  📊 DOM: {dump.get('total_links',0)} liens, "
                                   f"{len(dump.get('scrollables',[]))} scrollables")
                            for s in dump.get('scrollables', []):
                                trace("FETCH_DOM", f"  Scrollable: {s}")
                        except Exception as e:
                            trace("FETCH_DOM", f"Dump err: {type(e).__name__}")

                await isleep(2, stop_event)

            if not container_found:
                await page.screenshot(path="debug_container.png")
                _vlog("Container scrollable introuvable (debug_container.png)")
                log_fn("  ❌ Container scrollable introuvable")
                return

            await start_scroll()

            pseudos      = set()
            last_count   = 0
            no_change    = 0
            unsaved_new  = 0
            total_new    = 0
            last_save_t  = time.time()
            start_time   = time.time()
            cycle_num    = 0

            while not stop_event.is_set():
                cycle_num += 1
                elapsed = time.time() - start_time
                if elapsed > FETCH_MAX_DURATION:
                    _vlog(f"⏱️  Durée maximale ({FETCH_MAX_DURATION//60} min) atteinte.")
                    break

                await isleep(0.5, stop_event)
                if stop_event.is_set():
                    break

                # ── Extract links from dialog (single JS call) ────────
                t0 = time.time()
                before = len(pseudos)
                dom_links = 0
                try:
                    hrefs = await page.evaluate(r"""
                    () => {
                        // Try dialog first
                        let links = document.querySelectorAll(
                            'div[role="dialog"] a[href*="/@"]'
                        );
                        // Fallback: get from the scrollable container
                        if (!links.length) {
                            const scroller = document.querySelector('[data-autoscroll="true"]');
                            if (scroller) {
                                links = scroller.querySelectorAll('a[href*="/@"]');
                            }
                        }
                        // Last fallback: all profile links on page
                        if (!links.length) {
                            links = document.querySelectorAll('a[href*="/@"]');
                        }
                        return Array.from(links, a => a.getAttribute('href') || '');
                    }
                    """)
                    dom_links = len(hrefs)
                    t_query = time.time() - t0
                    for href in hrefs:
                        pseudo = href.split("/@")[-1].strip("/")
                        if (pseudo
                                and "?" not in pseudo
                                and "/" not in pseudo
                                and pseudo != username):
                            pseudos.add(pseudo)
                    t_extract = time.time() - t0
                except Exception as e:
                    t_query = t_extract = time.time() - t0
                    trace("SCROLL", f"Extract err: {type(e).__name__}")

                new_found = len(pseudos) - before

                # ── Ajout en DB (mémoire) ─────────────────────────────
                t1 = time.time()
                new_this_round = 0
                refollow_this_round = 0
                for pseudo in pseudos:
                    if pseudo not in db["followers"]:
                        db["followers"][pseudo] = {
                            "status": "pending", "score": None,
                            "scanned_at": None,
                        }
                        new_this_round += 1
                    elif db["followers"][pseudo]["status"] in ("removed", "blocked"):
                        # ── Re-follow detected ──
                        existing = db["followers"][pseudo]
                        removed_at = existing.get("removed_at")
                        delay_h = ""
                        if removed_at:
                            delta = datetime.now().timestamp() - removed_at
                            delay_h = f" (après {delta/3600:.1f}h)"
                        logging.warning(
                            f"RE-FOLLOW | @{pseudo} | was {existing['status']}"
                            f" | old_score={existing.get('score')}{delay_h}"
                        )
                        existing["status"] = "pending"
                        existing["score"] = None
                        existing["scanned_at"] = None
                        existing["refollow_count"] = existing.get("refollow_count", 0) + 1
                        existing["last_refollow_at"] = datetime.now().timestamp()
                        refollow_this_round += 1
                        new_this_round += 1
                t_db = time.time() - t1

                unsaved_new += new_this_round
                total_new   += new_this_round

                # ── Sauvegarde : tous les 50 nouveaux OU toutes les 2 min
                since_save = time.time() - last_save_t
                if unsaved_new >= 50 or (since_save >= 120 and unsaved_new > 0):
                    t2 = time.time()
                    save_db(db)
                    t_save = time.time() - t2
                    _vlog(f"  💾 Sauvegarde: {len(db['followers'])} en DB "
                           f"(+{unsaved_new} nouveaux, {int(since_save)}s, "
                           f"write={t_save:.1f}s)")
                    unsaved_new = 0
                    last_save_t = time.time()

                loaded = len(pseudos)
                # Log détaillé avec timing → file only
                _vlog(f"  📜 {loaded} chargés | DB: {len(db['followers'])} "
                       f"(+{new_this_round}) [{int(elapsed)}s] "
                       f"dom={dom_links} query={t_query:.1f}s extract={t_extract:.1f}s")

                # GUI progress every 100 profiles
                if loaded % 100 < 5 and loaded > 0:
                    log_fn(f"  📥 {loaded} profils récupérés...")

                # Log périodique détaillé dans le fichier
                if cycle_num % 10 == 0:
                    trace("SCROLL", f"Cycle {cycle_num}: "
                          f"loaded={loaded} dom_links={dom_links} "
                          f"query={t_query:.2f}s extract={t_extract:.2f}s "
                          f"db_insert={t_db:.3f}s elapsed={int(elapsed)}s "
                          f"no_change={no_change}")

                # Diagnostic DOM complet tous les 20 cycles
                if cycle_num % 20 == 0:
                    try:
                        dom_info = await page.evaluate(r"""
                        () => {
                            const scroller = document.querySelector('[data-autoscroll="true"]');
                            if (!scroller) return {error: 'no_scroller'};
                            const allNodes = scroller.querySelectorAll('*').length;
                            const allLinks = scroller.querySelectorAll('a[href*="/@"]').length;
                            return {
                                nodes: allNodes, links: allLinks,
                                scroll: {
                                    scrollTop: scroller.scrollTop,
                                    scrollHeight: scroller.scrollHeight,
                                    clientHeight: scroller.clientHeight,
                                    pct: Math.round(scroller.scrollTop / Math.max(1, scroller.scrollHeight - scroller.clientHeight) * 100),
                                }
                            };
                        }
                        """)
                        si = dom_info.get("scroll") or {}
                        _vlog(f"  📊 DOM: {dom_info.get('nodes',0)} nodes, "
                               f"{dom_info.get('links',0)} links | "
                               f"scroll: {si.get('pct',0)}% "
                               f"({si.get('scrollTop',0)}/{si.get('scrollHeight',0)})")
                        trace("SCROLL", f"DOM diag: {dom_info}")
                    except Exception as e:
                        trace("SCROLL", f"DOM diag err: {type(e).__name__}")

                if loaded == last_count:
                    no_change += 1
                else:
                    no_change  = 0
                    last_count = loaded

                if no_change >= 6:
                    _vlog(f"  ⏸️  Stall: {no_change} cycles sans nouveaux "
                           f"({loaded} chargés, dom={dom_links})")
                    break

                await stop_scroll()
                await isleep(1.2, stop_event)
                if stop_event.is_set():
                    break
                await start_scroll()

            await stop_scroll()

            # Sauvegarde finale
            final_new = 0
            final_refollow = 0
            for pseudo in pseudos:
                if pseudo not in db["followers"]:
                    db["followers"][pseudo] = {
                        "status": "pending", "score": None,
                        "scanned_at": None,
                    }
                    final_new += 1
                elif db["followers"][pseudo]["status"] in ("removed", "blocked"):
                    existing = db["followers"][pseudo]
                    removed_at = existing.get("removed_at")
                    delay_h = ""
                    if removed_at:
                        delta = datetime.now().timestamp() - removed_at
                        delay_h = f" (après {delta/3600:.1f}h)"
                    logging.warning(
                        f"RE-FOLLOW | @{pseudo} | was {existing['status']}"
                        f" | old_score={existing.get('score')}{delay_h}"
                    )
                    existing["status"] = "pending"
                    existing["score"] = None
                    existing["scanned_at"] = None
                    existing["refollow_count"] = existing.get("refollow_count", 0) + 1
                    existing["last_refollow_at"] = datetime.now().timestamp()
                    final_refollow += 1
                    final_new += 1
            save_db(db)

            elapsed = int(time.time() - start_time)
            rf_tag = f", ♻️{final_refollow} re-follows" if final_refollow else ""
            new_total = total_new + final_new
            if stop_event.is_set():
                _vlog(f"Interrompu après {elapsed}s — "
                       f"{len(db['followers'])} en DB "
                       f"({len(pseudos)} collectés, +{new_total} nouveaux{rf_tag}).")
                log_fn(f"  ⏹️ Interrompu — {len(db['followers'])} abonnés en DB "
                       f"(+{new_total} nouveaux{rf_tag})")
            else:
                _vlog(f"Terminé en {elapsed}s — "
                       f"{len(db['followers'])} en DB "
                       f"({len(pseudos)} collectés, +{new_total} nouveaux{rf_tag}).")
                log_fn(f"  ✅ {len(db['followers'])} abonnés récupérés "
                       f"(+{new_total} nouveaux{rf_tag})")

        finally:
            try:
                await page.evaluate(_JS_STOP_SCROLL)
            except Exception:
                pass
            await browser.close()

# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE : Extraction ──► scoring
# ══════════════════════════════════════════════════════════════════════════════
async def extract_profile_data(page, username: str, log_fn=None) -> dict:
    def _log(msg):
        if log_fn:
            log_fn(f"    ↳ {msg}")

    data = {
        "username": username, "not_found": False, "is_private": False,
        "full_text": "", "header_text": "", "replies_text": "",
        "threads_articles": 0, "error": None,
        "_has_bio": False, "_has_replies": False,
        "_has_real_pic": False, "_has_full_name": False,
        "_has_ig_link": False, "_follower_count": None,
    }
    try:
        _log("navigation...")
        try:
            await page.goto(
                f"https://www.threads.net/@{username}",
                timeout=12000, wait_until="domcontentloaded"
            )
        except Exception as nav_err:
            err_str = str(nav_err)
            if "429" in err_str or "ERR_HTTP_RESPONSE_CODE_FAILURE" in err_str:
                _log(f"⛔ HTTP 429 rate limit détecté")
                data["not_found"] = True
                data["error"] = "429_RATE_LIMIT"
                return data
            raise

        _log("attente DOM...")
        await _wait_for_profile(page, timeout_ms=5000)
        await asyncio.sleep(random.uniform(0.3, 0.6))

        _log("lecture body...")
        try:
            full_text = await asyncio.wait_for(
                page.inner_text("body"), timeout=5.0
            )
        except asyncio.TimeoutError:
            _log("⚠️  fallback evaluate")
            full_text = await page.evaluate(
                "() => document.body?.innerText || ''"
            ) or ""

        data["full_text"] = full_text

        # ── 429 rate limit detection in page body ──
        if re.search(r"429|too many requests|trop de requêtes"
                     r"|cette page ne fonctionne pas",
                     full_text, re.IGNORECASE) and len(full_text) < 500:
            _log("⛔ HTTP 429 détecté dans le body")
            data["not_found"] = True
            data["error"] = "429_RATE_LIMIT"
            return data

        if re.search(r"not found|not available|n'est pas disponible"
                     r"|page isn.t available|page introuvable",
                     full_text, re.IGNORECASE):
            _log("introuvable")
            data["not_found"] = True
            return data

        data["is_private"] = bool(re.search(
            r"account is private|compte est priv[ée]|profil priv",
            full_text, re.IGNORECASE
        ))

        # ── Extraction JS complète ────────────────────────────────────
        _log("extraction JS...")
        try:
            info = await page.evaluate(r"""
            () => {
                const result = {
                    follower_count: null,
                    has_real_pic: false,
                    has_full_name: false,
                    has_ig_link: false,
                    has_bio: false,
                    is_verified: false,
                    bio_text: '',
                    full_name: '',
                    debug: [],
                };

                // ── Follower count ──────────────────────────────────
                try {
                    // Strategy 1: Find the specific element containing "X followers"
                    // Walk all text nodes and find the one with just a number + "followers"
                    const allEls = document.querySelectorAll('span, a, div, p');
                    let found = false;
                    for (const el of allEls) {
                        // Only check leaf-ish elements (avoid containers)
                        if (el.children.length > 3) continue;
                        const t = (el.textContent || '').trim();
                        // Match "0 followers", "6 followers", "1.2K abonnés", "8 352 followers"
                        const m = t.match(/^([\d][\d,. \u00a0\u202f]*[KkMm]?)\s*(followers|abonnés)$/i);
                        if (m) {
                            let raw = m[1].trim();
                            let cleaned = raw.replace(/[\s\u00a0\u202f]/g, '');
                            const suffix = cleaned.slice(-1).toUpperCase();
                            if (suffix === 'K')
                                result.follower_count = Math.round(
                                    parseFloat(cleaned.slice(0,-1).replace(',','.')) * 1000);
                            else if (suffix === 'M')
                                result.follower_count = Math.round(
                                    parseFloat(cleaned.slice(0,-1).replace(',','.')) * 1000000);
                            else
                                result.follower_count = parseInt(
                                    cleaned.replace(/[^\d]/g,''), 10) || 0;
                            result.debug.push('fc_dom=' + result.follower_count + '(' + t + ')');
                            found = true;
                            break;
                        }
                    }
                    // Strategy 2: fallback — find "follower" in text, get the number just before
                    if (!found) {
                        const bodyText = document.body.innerText || '';
                        // Split by lines, find the line with "followers"
                        const lines = bodyText.split(/\n/);
                        for (const line of lines) {
                            const m = line.match(/([\d][\d,. \u00a0\u202f]*[KkMm]?)\s*(followers|abonnés)/i);
                            if (m) {
                                let cleaned = m[1].trim().replace(/[\s\u00a0\u202f]/g, '');
                                const suffix = cleaned.slice(-1).toUpperCase();
                                if (suffix === 'K')
                                    result.follower_count = Math.round(
                                        parseFloat(cleaned.slice(0,-1).replace(',','.')) * 1000);
                                else if (suffix === 'M')
                                    result.follower_count = Math.round(
                                        parseFloat(cleaned.slice(0,-1).replace(',','.')) * 1000000);
                                else
                                    result.follower_count = parseInt(
                                        cleaned.replace(/[^\d]/g,''), 10) || 0;
                                result.debug.push('fc_line=' + result.follower_count + '(' + line.trim() + ')');
                                break;
                            }
                        }
                    }
                } catch(e) { result.debug.push('fc_err: ' + e); }

                // ── Profile picture (real vs default) ───────────────
                try {
                    const imgs = document.querySelectorAll('img');
                    for (const img of imgs) {
                        const src = img.src || '';
                        const alt = (img.alt || '').toLowerCase();
                        const w = img.naturalWidth || img.width || 0;
                        // Profile pics are typically circular, 80-150px
                        // Default avatars contain specific patterns
                        if ((alt.includes('photo') || alt.includes('profile')
                             || alt.includes('avatar') || alt.includes(username.toLowerCase()))
                            && w >= 40) {
                            const isDefault = src.includes('default') 
                                || src.includes('empty') 
                                || src.includes('placeholder')
                                || src.includes('/44884218_345');
                            result.has_real_pic = !isDefault;
                            result.debug.push('pic=' + (result.has_real_pic ? 'real' : 'default'));
                            break;
                        }
                    }
                    // Fallback: any large img in the header area
                    if (!result.has_real_pic) {
                        const headerImgs = document.querySelectorAll(
                            'img[width], img[style*="width"]'
                        );
                        for (const img of headerImgs) {
                            const r = img.getBoundingClientRect();
                            if (r.width >= 60 && r.width <= 200 && r.top < 400) {
                                const src = img.src || '';
                                result.has_real_pic = !src.includes('default')
                                    && !src.includes('empty')
                                    && !src.includes('/44884218_345')
                                    && src.length > 20;
                                result.debug.push('pic_fb=' + result.has_real_pic);
                                break;
                            }
                        }
                    }
                } catch(e) { result.debug.push('pic_err'); }

                // ── Full name ───────────────────────────────────────
                try {
                    // From og:title meta
                    const ogTitle = document.querySelector('meta[property="og:title"]');
                    if (ogTitle) {
                        const m = (ogTitle.content || '').match(/^(.+?)\s*\(@/);
                        if (m) {
                            result.full_name = m[1].trim();
                            result.has_full_name = result.full_name.length >= 3
                                && result.full_name !== username;
                        }
                    }
                    // Fallback: first large text before username
                    if (!result.has_full_name) {
                        const headings = document.querySelectorAll(
                            'h1, h2, [role="heading"], span[dir="auto"]'
                        );
                        for (const h of headings) {
                            const t = (h.textContent || '').trim();
                            if (t.length >= 3 && t.length < 60
                                && t !== username && !t.match(/^\d/)) {
                                result.full_name = t;
                                result.has_full_name = true;
                                break;
                            }
                        }
                    }
                    result.debug.push('name=' + result.full_name);
                } catch(e) { result.debug.push('name_err'); }

                // ── Instagram link ──────────────────────────────────
                try {
                    result.has_ig_link = !!document.querySelector(
                        'a[href*="instagram.com"]'
                    );
                    result.debug.push('ig=' + result.has_ig_link);
                } catch(e) {}

                // ── Bio ─────────────────────────────────────────────
                try {
                    const metaDesc = document.querySelector(
                        'meta[name="description"]'
                    );
                    if (metaDesc) {
                        let bio = metaDesc.content || '';
                        // Remove the stats portion
                        bio = bio.replace(
                            /[\d,.\s]*\s*(followers?|abonnés|following|replies).*/gi, ''
                        ).trim();
                        // Remove username prefix
                        bio = bio.replace(/^.*?-\s*/, '').trim();
                        result.bio_text = bio;
                        result.has_bio = bio.length >= 5;
                    }
                    result.debug.push('bio=' + result.has_bio + 
                        '(' + result.bio_text.substring(0,30) + ')');
                } catch(e) { result.debug.push('bio_err'); }

                // ── Verified badge ──────────────────────────────────
                try {
                    result.is_verified = !!document.querySelector(
                        '[data-testid="verified-badge"], '
                        + 'svg[aria-label*="Verified"], '
                        + 'svg[aria-label*="vérifié"]'
                    );
                } catch(e) {}

                return result;
            }
            """)
            if info:
                for dbg in info.get("debug", []):
                    trace("EXTRACT", f"@{username}: {dbg}")
                data["_follower_count"] = info.get("follower_count")
                data["_has_real_pic"] = info.get("has_real_pic", False)
                data["_has_full_name"] = info.get("has_full_name", False)
                data["_has_ig_link"] = info.get("has_ig_link", False)
                data["_has_bio"] = info.get("has_bio", False)
                data["_is_verified"] = info.get("is_verified", False)
                data["_full_name"] = info.get("full_name", "")
                _log(f"fc={data['_follower_count']} pic={data['_has_real_pic']} "
                     f"name={data['_has_full_name']} ig={data['_has_ig_link']} "
                     f"bio={data['_has_bio']} v={info.get('is_verified')}")
        except Exception as e:
            _log(f"JS extract err: {type(e).__name__}")

        # ── Header text (fallback for bio) ────────────────────────────
        _log("lecture header...")
        try:
            data["header_text"] = await asyncio.wait_for(
                page.inner_text("header"), timeout=3.0
            )
        except Exception:
            pass

        # ── Bio fallback from header ──────────────────────────────────
        if not data["_has_bio"] and data["header_text"]:
            bio = re.sub(r'@[\w\.]+|\d+\s*(followers|abonnés)', '',
                         data["header_text"], flags=re.IGNORECASE).strip()
            data["_has_bio"] = len(bio) >= 10

        # ── Onglet Threads (déjà actif au chargement) ─────────────────
        if not data["is_private"]:
            _log("lecture threads (onglet actif)...")

            # Check for "Aucun thread" text first (most reliable)
            threads_empty = bool(re.search(
                r"aucun thread|no threads yet|nothing here yet"
                r"|hasn.t posted|n.a pas encore publi",
                full_text, re.IGNORECASE
            ))

            if threads_empty:
                data["threads_articles"] = 0
                data["_all_posts_recent"] = False
                _log("threads: 0 (texte 'aucun thread')")
            else:
                # Count posts + check recency via JS
                try:
                    post_info = await page.evaluate(r"""
                    () => {
                        // Count top-level articles only (not quoted/embedded)
                        const articles = document.querySelectorAll('article');
                        // Filter: only direct children of main content, not nested quotes
                        let topLevel = 0;
                        for (const a of articles) {
                            // If this article is inside another article, skip it
                            if (!a.closest('article')?.closest('article') 
                                || a.closest('article') === a) {
                                topLevel++;
                            }
                        }
                        // Fallback if article tag not used
                        if (topLevel === 0) {
                            const pressable = document.querySelectorAll('[data-pressable-container]');
                            topLevel = pressable.length;
                        }
                        
                        // Check post timestamps
                        const times = document.querySelectorAll('time[datetime]');
                        let allRecent = times.length > 0;
                        let recentCount = 0;
                        const now = Date.now();
                        const h72 = 72 * 60 * 60 * 1000;
                        for (const t of times) {
                            const dt = new Date(t.getAttribute('datetime'));
                            if (!isNaN(dt.getTime())) {
                                if ((now - dt.getTime()) <= h72) {
                                    recentCount++;
                                } else {
                                    allRecent = false;
                                }
                            }
                        }
                        // Also check relative time hints (19h, 21h, 1j, 2j)
                        if (times.length === 0) {
                            const body = document.body?.innerText || '';
                            const timeHints = body.match(/\b\d+\s*[hj]\b/gi) || [];
                            recentCount = timeHints.length;
                            allRecent = recentCount > 0 && timeHints.every(h => {
                                const val = parseInt(h);
                                const unit = h.slice(-1).toLowerCase();
                                return (unit === 'h' && val <= 72) || (unit === 'j' && val <= 3);
                            });
                        }
                        
                        return {
                            count: topLevel, 
                            all_recent: allRecent && topLevel > 0,
                            recent_count: recentCount,
                            time_tags: times.length,
                            // Spam detection: check for duplicate content
                            duplicate_ratio: (() => {
                                const articles = document.querySelectorAll('article');
                                if (articles.length < 2) return 0;
                                const texts = Array.from(articles)
                                    .map(a => (a.innerText || '').trim().substring(0, 120).toLowerCase())
                                    .filter(t => t.length > 20);
                                if (texts.length < 2) return 0;
                                // Count how many are similar to the first one
                                const ref = texts[0];
                                let dupes = 0;
                                for (let i = 1; i < texts.length; i++) {
                                    // Simple similarity: shared prefix > 60% of length
                                    let shared = 0;
                                    const minLen = Math.min(ref.length, texts[i].length);
                                    for (let j = 0; j < minLen; j++) {
                                        if (ref[j] === texts[i][j]) shared++;
                                    }
                                    if (shared / minLen > 0.6) dupes++;
                                }
                                return dupes / (texts.length - 1);
                            })(),
                            // Spam keywords detection
                            has_spam_keywords: (() => {
                                const body = (document.body?.innerText || '').toLowerCase();
                                const spamPatterns = [
                                    /whatsapp|telegram|signal/,
                                    /\b0\d{9,}\b/,      // phone numbers
                                    /\+\d{10,}/,         // intl phone
                                    /envie de faire connaissance/,
                                    /click.*link.*bio/,
                                    /dm.*for.*promo/i,
                                    /follow.*for.*follow/i,
                                    /check.*my.*profile/i,
                                ];
                                return spamPatterns.some(p => p.test(body));
                            })(),
                        };
                    }
                    """)
                    if post_info:
                        data["threads_articles"] = post_info.get("count", 0)
                        data["_all_posts_recent"] = post_info.get("all_recent", False)
                        data["_duplicate_ratio"] = post_info.get("duplicate_ratio", 0)
                        data["_has_spam_keywords"] = post_info.get("has_spam_keywords", False)
                        _log(f"threads: {post_info.get('count',0)} posts "
                             f"(recent={post_info.get('recent_count',0)}, "
                             f"all_recent={post_info.get('all_recent',False)}, "
                             f"time_tags={post_info.get('time_tags',0)}, "
                             f"dupes={post_info.get('duplicate_ratio',0):.0%}, "
                             f"spam_kw={post_info.get('has_spam_keywords',False)})")
                except Exception as e:
                    _log(f"threads count err: {type(e).__name__}")

            # ── Onglet Réponses ───────────────────────────────────────
            _log("clic onglet réponses...")
            replies_clicked = False

            # Méthode 1: Clic par texte (le plus fiable)
            for text_pat in ["Réponses", "Replies", "réponses", "replies"]:
                try:
                    tab = page.get_by_text(text_pat, exact=True).first
                    if await asyncio.wait_for(tab.is_visible(), timeout=2.0):
                        await tab.click()
                        replies_clicked = True
                        _log(f"clic réponses OK (texte '{text_pat}')")
                        break
                except Exception:
                    continue

            # Méthode 2: Clic par role="tab"
            if not replies_clicked:
                try:
                    tab = page.get_by_role(
                        "tab", name=re.compile(r"r[ée]ponses|replies", re.IGNORECASE)
                    ).first
                    if await asyncio.wait_for(tab.is_visible(), timeout=2.0):
                        await tab.click()
                        replies_clicked = True
                        _log("clic réponses OK (role=tab)")
                except Exception:
                    pass

            # Méthode 3: JS — chercher tous les onglets et cliquer le bon
            if not replies_clicked:
                try:
                    clicked = await page.evaluate(r"""
                    () => {
                        // Find all tab-like elements
                        const candidates = document.querySelectorAll(
                            '[role="tab"], [role="tablist"] > *, a, div[class]'
                        );
                        for (const el of candidates) {
                            const t = (el.textContent || '').trim().toLowerCase();
                            if (t === 'réponses' || t === 'replies') {
                                el.click();
                                return true;
                            }
                        }
                        return false;
                    }
                    """)
                    if clicked:
                        replies_clicked = True
                        _log("clic réponses OK (JS)")
                except Exception as e:
                    _log(f"JS tab err: {type(e).__name__}")

            if replies_clicked:
                # Poll : attendre que le contenu de l'onglet charge
                reply_info = None
                for r_attempt in range(5):  # 5 x 1.5s = 7.5s max
                    await asyncio.sleep(1.5)
                    try:
                        reply_info = await page.evaluate(r"""
                        (username) => {
                            const body = document.body?.innerText || '';
                            
                            // 1. Check for explicit "no replies" messages
                            const emptyPatterns = [
                                /aucune r[ée]ponse/i,
                                /no replies yet/i,
                                /nothing here yet/i,
                                /hasn.t replied/i,
                                /pas encore de r[ée]ponse/i,
                                /rien pour l.instant/i,
                            ];
                            for (const pat of emptyPatterns) {
                                if (pat.test(body)) {
                                    return {has_replies: false, reason: 'empty_text:' + body.match(pat)[0], final: true};
                                }
                            }
                            
                            // 2. Look for article elements
                            const articles = document.querySelectorAll('article, [data-pressable-container]');
                            if (articles.length > 0) {
                                return {has_replies: true, reason: 'articles=' + articles.length, final: true};
                            }
                            
                            // 3. Look for timestamps (1 j, 2 j, 12 h, etc.) - sign of loaded content
                            const timeEls = document.querySelectorAll('time[datetime]');
                            if (timeEls.length > 0) {
                                return {has_replies: true, reason: 'time_tags=' + timeEls.length, final: true};
                            }
                            
                            // 4. Check if the username appears multiple times in the page
                            // (once in header, extra = in replies)
                            if (username) {
                                const regex = new RegExp(username, 'gi');
                                const matches = (body.match(regex) || []).length;
                                if (matches >= 2) {
                                    return {has_replies: true, reason: 'username_x' + matches, final: true};
                                }
                            }
                            
                            // 5. Look for reply-like structures: links to other profiles + short text
                            const profileLinks = document.querySelectorAll('a[href^="/@"]');
                            // Filter: only links below the tabs area (y > 300px)
                            let linksInContent = 0;
                            for (const a of profileLinks) {
                                const r = a.getBoundingClientRect();
                                if (r.top > 350) linksInContent++;
                            }
                            if (linksInContent >= 2) {
                                return {has_replies: true, reason: 'profile_links=' + linksInContent, final: true};
                            }
                            
                            // 6. Check scrollable content height below tabs
                            const tablist = document.querySelector('[role="tablist"]');
                            if (tablist) {
                                const tabBottom = tablist.getBoundingClientRect().bottom;
                                // Count visible elements below tabs
                                const belowTabs = document.elementsFromPoint(
                                    window.innerWidth / 2, tabBottom + 100
                                );
                                // If there's substantial content below tabs
                                const contentEl = belowTabs.find(el => 
                                    el.scrollHeight > 200 && el !== document.body && el !== document.documentElement
                                );
                                if (contentEl && contentEl.innerText && contentEl.innerText.length > 100) {
                                    return {has_replies: true, reason: 'content_below_tabs', final: true};
                                }
                            }
                            
                            // Nothing conclusive yet
                            return {has_replies: false, reason: 'loading', final: false};
                        }
                        """, data.get("username", ""))
                        if reply_info and reply_info.get("final"):
                            break
                    except Exception as e:
                        _log(f"reply poll err: {type(e).__name__}")

                if reply_info:
                    data["_has_replies"] = reply_info.get("has_replies", False)
                    reason = reply_info.get("reason", "?")
                    if data["_has_replies"]:
                        data["replies_text"] = "HAS_REPLIES"
                        _log(f"réponses: OUI ({reason})")
                    else:
                        data["replies_text"] = ""
                        _log(f"réponses: NON ({reason})")
                else:
                    data["replies_text"] = ""
                    _log("réponses: NON (timeout)")
            else:
                _log("⚠️ onglet réponses introuvable")

            # ── Revenir sur onglet Threads pour vérification ──────────
            # Si on n'a pas compté les articles avant, essayons maintenant
            if data["threads_articles"] == 0 and not threads_empty:
                for text_pat in ["Threads", "threads"]:
                    try:
                        tab = page.get_by_text(text_pat, exact=True).first
                        if await asyncio.wait_for(tab.is_visible(), timeout=1.5):
                            await tab.click()
                            await asyncio.sleep(0.8)
                            count = await page.locator(
                                "article, [data-pressable-container]"
                            ).count()
                            data["threads_articles"] = count
                            _log(f"threads (retour): {count}")
                            break
                    except Exception:
                        continue

        _log(f"✓ threads={data['threads_articles']} "
             f"replies={'OUI' if data.get('_has_replies') else 'NON'} "
             f"private={data['is_private']}")

    except Exception as e:
        data["error"] = str(e)
        _log(f"❌ {e}")

    return data


# ── Private account scoring mode ──────────────────────────────────────────
# False = intelligent mode (new: tier by followers + bio/pic)
# True  = strict mode (old: less permissive, same rules as public)
_strict_private_mode = False

def set_strict_private_mode(val: bool):
    global _strict_private_mode
    _strict_private_mode = val


def score_from_data(data: dict) -> tuple:
    """Score 0-100: higher = more likely fake/inactive. Seuil recommandé : 70"""
    if data.get("not_found"):
        return -1, ["Introuvable"]
    if data.get("error") and not data.get("full_text"):
        return -1, [str(data["error"])[:40]]
    if data.get("_is_verified"):
        return 0, ["\u2713 Vérifié"]

    score    = 0
    details  = []

    full_text        = data.get("full_text", "")
    threads_articles = data.get("threads_articles", 99)
    is_private       = data.get("is_private", False)
    has_bio          = data.get("_has_bio", False)
    has_full_name    = data.get("_has_full_name", False)

    # ── Followers count ───────────────────────────────────────────────
    fc = data.get("_follower_count")
    if fc is None:
        for line in full_text.split("\n"):
            m = re.search(r'([\d][\d   ,\.]*[KkMm]?)\s*(followers|abonnés)',
                          line, re.IGNORECASE)
            if m:
                fc = int(re.sub(r'[^\d]', '', m.group(1)) or "0")
                break

    if fc is not None:
        if fc == 0:
            score += 15; details.append("0abn +15")
        elif fc <= 10:
            score += 10; details.append(f"{fc}abn +10")
        elif fc <= 50:
            score += 5;  details.append(f"{fc}abn +5")
        elif fc >= 500:
            score -= 10; details.append(f"{fc}abn -10")
        elif fc >= 100:
            score -= 5;  details.append(f"{fc}abn -5")
        # 51-99: neutral
    else:
        score += 5; details.append("abn? +5")

    # ── Threads (posts) ───────────────────────────────────────────────
    has_posts = False
    all_posts_recent = data.get("_all_posts_recent", False)
    duplicate_ratio  = data.get("_duplicate_ratio", 0)
    has_spam_kw      = data.get("_has_spam_keywords", False)
    is_spambot       = False

    if not is_private:
        if threads_articles == 0:
            score += 35; details.append("0post +35")
        elif threads_articles <= 2:
            score += 20; details.append(f"{threads_articles}post +20")
            if all_posts_recent:
                score += 20; details.append("spam(<72h) +20")
        elif threads_articles <= 4:
            score += 10; details.append(f"{threads_articles}post +10")
            if all_posts_recent:
                score += 20; details.append("spam(<72h) +20")
        elif threads_articles >= 5:
            has_posts = True
            score -= 15; details.append(f"{threads_articles}post -15")

        # ── Spam content detection ────────────────────────────────
        # Duplicate posts = spambot (overrides activity bonus)
        if duplicate_ratio >= 0.5 and threads_articles >= 3:
            is_spambot = True
            # Cancel the -15 bonus for "having posts"
            if has_posts:
                score += 15; details.append("dupes! annule post")
            score += 40; details.append(f"spam_dupes({duplicate_ratio:.0%}) +40")

        if has_spam_kw:
            score += 25; details.append("spam_keywords +25")
            is_spambot = True

    # ── Réponses ──────────────────────────────────────────────────────
    has_replies = data.get("_has_replies", False)
    if not is_private:
        if not has_replies:
            score += 25; details.append("0rép +25")
        elif is_spambot:
            # Spambot with replies = still spam, no bonus
            score += 10; details.append("rép_spam +10")
        elif has_posts:
            score -= 15; details.append("rép+posts -15")
        else:
            # Réponses SANS posts = spambot
            score += 10; details.append("rép_sans_post +10")

    # ── COMBOS ────────────────────────────────────────────────────────
    if not is_private and threads_articles == 0 and not has_replies:
        score += 20; details.append("combo(0p+0r) +20")

    if not is_private and threads_articles == 0 and has_replies:
        score += 10; details.append("spammer(0p+rép) +10")

    # Inactif : peu de posts + 0 réponses + pas de bio
    if (not is_private and 1 <= threads_articles <= 4
            and not has_replies and not has_bio):
        score += 10; details.append("inactif +10")

    # ── Bio ───────────────────────────────────────────────────────────
    # Bio ne sauve pas un compte avec 0 posts + 0 réponses
    zero_activity = (threads_articles == 0 and not has_replies and not is_private)
    if has_bio:
        if zero_activity:
            score -= 5;  details.append("bio(inactif) -5")
        else:
            score -= 10; details.append("bio -10")
    else:
        score += 15; details.append("!bio +15")

    # ── Privé ─────────────────────────────────────────────────────────
    if is_private:
        has_pic = data.get("_has_real_pic", False)
        if _strict_private_mode:
            # Mode strict (ancien système) : privé = modérément suspect
            score += 10; details.append("privé +10")
        else:
            # Mode intelligent : scoring basé sur followers
            if fc is not None and fc < 10:
                score += 40; details.append(f"privé(<10abn) +40")
            elif fc is not None and fc < 30:
                if not has_bio and not has_pic:
                    score += 30; details.append("privé(<30,!bio,!pic) +30")
                elif not has_bio or not has_pic:
                    score += 20; details.append("privé(<30,partiel) +20")
                else:
                    score += 5; details.append("privé(<30,bio+pic) +5")
            else:
                # 30+ followers ou inconnu → review manuel
                data["_needs_manual_review"] = True
                score += 5; details.append("privé(30+) +5 📋")

    # ── Nom complet ───────────────────────────────────────────────────
    if has_full_name:
        score -= 5; details.append("nom -5")

    # ── Store flags ───────────────────────────────────────────────────
    data["_has_bio"]     = has_bio
    data["_has_replies"] = has_replies

    final = max(0, min(100, score))
    return final, details


# ── Helpers clean ─────────────────────────────────────────────────────────────
async def _click_three_dots(page, log_fn=None) -> bool:
    """Click the ⋯ (circle with dots) next to the bell and Instagram icons.
    
    NOT the page-level ⋯ at the very top — the one in the profile header row.
    Layout: [Instagram icon] [Bell icon] [⋯ button] ← this one
    """
    async def _menu_appeared():
        try:
            await asyncio.sleep(0.5)
            cnt = await page.locator(
                "[role='menu'], [role='menuitem'], [role='dialog'] [role='list']"
            ).count()
            return cnt > 0
        except Exception:
            return False

    # Strategy 1: JS — find the ⋯ button adjacent to the Instagram link
    try:
        clicked = await page.evaluate(r"""
            () => {
                // Find the Instagram link
                const igLink = document.querySelector('a[href*="instagram.com"]');
                if (!igLink) return 'no_ig_link';
                
                // Walk up to the row container (the div that holds IG + bell + ⋯)
                let row = igLink.parentElement;
                for (let i = 0; i < 5 && row; i++) {
                    const clickables = row.querySelectorAll(
                        'div[role="button"], button, [role="button"], [tabindex="0"]'
                    );
                    // We need at least 2 clickable items (bell + ⋯) besides the IG link
                    const btns = Array.from(clickables).filter(el => {
                        const r = el.getBoundingClientRect();
                        return r.width > 0 && r.width < 80
                            && !el.closest('a[href*="instagram"]');
                    });
                    if (btns.length >= 1) {
                        // The ⋯ is the LAST button in this row (after bell)
                        btns[btns.length - 1].click();
                        return 'ig_row_last';
                    }
                    row = row.parentElement;
                }
                return 'no_btn_in_row';
            }
        """)
        trace("DOTS", f"Strategy 1: {clicked}")
        if clicked and clicked.startswith("ig_row"):
            if await _menu_appeared():
                return True
    except Exception as e:
        trace("DOTS", f"Strategy 1 err: {type(e).__name__}")

    # Strategy 2: Aria-label connus
    for label in ["Plus", "More", "Plus d'options", "More options"]:
        try:
            btn = page.get_by_role(
                "button", name=re.compile(f"^{label}$", re.IGNORECASE)
            ).first
            if await asyncio.wait_for(btn.is_visible(), timeout=1.5):
                await btn.click()
                if await _menu_appeared():
                    trace("DOTS", f"Strategy 2: aria={label}")
                    return True
        except Exception:
            pass

    # Strategy 3: All SVG-only buttons, pick the one closest to the IG link
    try:
        clicked = await page.evaluate(r"""
            () => {
                const igLink = document.querySelector('a[href*="instagram.com"]');
                const igRect = igLink ? igLink.getBoundingClientRect() : null;
                
                const allBtns = Array.from(document.querySelectorAll(
                    'div[role="button"], button, [role="button"]'
                )).filter(b => {
                    const r = b.getBoundingClientRect();
                    const t = (b.innerText || '').trim();
                    // SVG-only small button, no text
                    return r.width > 0 && r.width < 80 && r.height < 80
                        && (t === '' || t === '…' || t === '...' || t === '⋯')
                        && b.querySelector('svg');
                });
                
                if (!allBtns.length) return 'no_svg_btns';
                
                if (igRect) {
                    // Sort by proximity to the IG link (same row = similar Y)
                    allBtns.sort((a, b) => {
                        const ra = a.getBoundingClientRect();
                        const rb = b.getBoundingClientRect();
                        const da = Math.abs(ra.top - igRect.top) + Math.abs(ra.left - igRect.left);
                        const db = Math.abs(rb.top - igRect.top) + Math.abs(rb.left - igRect.left);
                        return da - db;
                    });
                    // Pick the closest one to IG that is to its RIGHT
                    for (const btn of allBtns) {
                        const r = btn.getBoundingClientRect();
                        if (Math.abs(r.top - igRect.top) < 40 && r.left > igRect.left) {
                            btn.click();
                            return 'svg_near_ig';
                        }
                    }
                }
                
                // Fallback: last SVG button on page
                allBtns[allBtns.length - 1].click();
                return 'svg_last';
            }
        """)
        trace("DOTS", f"Strategy 3: {clicked}")
        if clicked and not clicked.startswith("no_"):
            if await _menu_appeared():
                return True
    except Exception as e:
        trace("DOTS", f"Strategy 3 err: {type(e).__name__}")

    trace("DOTS", "ÉCHEC toutes stratégies")
    return False


async def _click_remove_follower(page, force_block=False) -> str:
    """Click 'Supprimer follower' or fallback to 'Bloquer'.
    
    Args:
        force_block: If True, try Bloquer FIRST (for repeat re-followers).
    
    Returns:
        'removed' if Supprimer follower clicked
        'blocked' if Bloquer clicked (fallback)
        '' if nothing found
    """
    remove_patterns = [
        r"supprimer follower",
        r"remove follower",
        r"supprimer l.abonn",
        r"retirer.*abonn",
        r"remove.*follow",
    ]
    block_patterns = [r"^bloquer$", r"^block$"]

    async def _try_remove():
        for pat in remove_patterns:
            try:
                item = page.get_by_role(
                    "menuitem", name=re.compile(pat, re.IGNORECASE)
                ).first
                if await asyncio.wait_for(item.is_visible(), timeout=2.0):
                    await item.click()
                    return "removed"
            except Exception:
                pass
        for pat in remove_patterns:
            try:
                item = page.get_by_text(
                    re.compile(pat, re.IGNORECASE)
                ).first
                if await asyncio.wait_for(item.is_visible(), timeout=1.5):
                    await item.click()
                    return "removed"
            except Exception:
                pass
        return ""

    async def _try_block():
        for pat in block_patterns:
            try:
                item = page.get_by_text(
                    re.compile(pat, re.IGNORECASE)
                ).first
                if await asyncio.wait_for(item.is_visible(), timeout=1.5):
                    await item.click()
                    trace("CLEAN", "Bloquer (force_block)" if force_block
                          else "Fallback: Bloquer")
                    return "blocked"
            except Exception:
                pass
        return ""

    if force_block:
        # Repeat offender: block first, remove as fallback
        result = await _try_block()
        if result:
            return result
        return await _try_remove()
    else:
        # Normal: remove first, block as fallback
        result = await _try_remove()
        if result:
            return result
        return await _try_block()


async def _click_confirm(page) -> bool:
    """Click confirmation button for either remove or block action."""
    for pat in [r"^supprimer$", r"^remove$", r"^bloquer$", r"^block$",
                r"confirm", r"oui$", r"^yes$", r"supprimer follower"]:
        try:
            btn = page.get_by_role(
                "button", name=re.compile(pat, re.IGNORECASE)
            ).first
            if await asyncio.wait_for(btn.is_visible(), timeout=3.0):
                await btn.click()
                return True
        except Exception:
            pass
    return False


# ── Scan ──────────────────────────────────────────────────────────────────────
async def run_scan_async(usernames, db, log_fn, threshold, progress_fn,
                         stop_event, dry_run=False):
    results  = {}
    total    = len(usernames)
    tag      = "🔬 DRY RUN" if dry_run else "🔍 Scan"
    log_fn(f"{tag} de {total} profils...")

    # ── Error tracking ────────────────────────────────────────────────
    consecutive_errors = 0
    recent_results = []  # True=ok, False=error

    def _check_health(pseudo):
        """Check error rate and consecutive errors. Returns False if should stop."""
        nonlocal consecutive_errors
        if consecutive_errors >= CONSECUTIVE_ERROR_LIMIT:
            log_fn(f"🛑 Arrêt auto — {consecutive_errors} erreurs consécutives "
                   f"(attendez 15-30 min)")
            return False
        if len(recent_results) >= ERROR_RATE_WINDOW:
            window = recent_results[-ERROR_RATE_WINDOW:]
            error_rate = window.count(False) / len(window)
            if error_rate >= ERROR_RATE_THRESHOLD:
                log_fn(f"🛑 Arrêt auto — {error_rate:.0%} d'erreurs "
                       f"(attendez 15-30 min)")
                return False
        return True

    async with async_playwright() as p:
        browser   = await p.chromium.connect_over_cdp("http://127.0.0.1:9222")
        ctx, page = await _get_page(browser)

        try:
            await ctx.route(
                BLOCK_FILTER,
                lambda route: asyncio.ensure_future(route.abort())
            )

            for i, pseudo in enumerate(usernames):
                if stop_event.is_set():
                    break

                # ── Health check before each profile ──
                if not _check_health(pseudo):
                    stop_event.set()
                    break

                t_profile = time.time()
                progress_fn(i + 1, total, pseudo)

                try:
                    data = await asyncio.wait_for(
                        extract_profile_data(page, pseudo, log_verbose),
                        timeout=20.0
                    )
                except RateLimitError:
                    log_fn(f"  ⛔ [{i+1}/{total}] @{pseudo} → HTTP 429 Rate Limit")
                    log_fn("🛑 Threads bloque les requêtes — arrêt immédiat")
                    log_file("429_RATE_LIMIT", pseudo)
                    stop_event.set()
                    results["__429_DETECTED__"] = True
                    break
                except asyncio.TimeoutError:
                    log_verbose(f"@{pseudo} — timeout 20s")
                    await _reset_page(page)
                    data = {
                        "username": pseudo, "not_found": True,
                        "error": "timeout", "full_text": "",
                        "header_text": "", "replies_text": "",
                        "threads_articles": 0, "is_private": False,
                        "_has_bio": False, "_has_replies": False,
                    }

                dt = time.time() - t_profile
                trace("SCAN", f"@{pseudo}: extraction {dt:.1f}s")

                # ── 429 Rate Limit → immediate stop ──
                if data.get("error") == "429_RATE_LIMIT":
                    log_fn(f"  ⛔ [{i+1}/{total}] @{pseudo} → HTTP 429 Rate Limit")
                    log_fn("🛑 Threads bloque les requêtes — arrêt immédiat")
                    log_file("429_RATE_LIMIT", pseudo)
                    stop_event.set()
                    # Flag for GUI popup
                    results["__429_DETECTED__"] = True
                    break

                # Score immédiatement
                score, details = score_from_data(data)
                results[pseudo] = (score, details)

                if score == -1:
                    # ── Error: not found / blocked ──
                    consecutive_errors += 1
                    recent_results.append(False)

                    if not dry_run:
                        db["followers"].setdefault(pseudo, {
                            "status": "pending", "score": None, "scanned_at": None})
                        db["followers"][pseudo]["status"] = "not_found"
                        db["followers"][pseudo]["scanned_at"] = \
                            datetime.now().timestamp()
                        save_db(db)
                    log_fn(f"  🔍 [{i+1}/{total}] @{pseudo} → ⚠️ introuvable")
                    log_file("SCAN_NOT_FOUND", pseudo)

                elif score >= 0:
                    # ── Success ──
                    consecutive_errors = 0
                    recent_results.append(True)

                    dry_tag = " [DRY]" if dry_run else ""
                    needs_review = data.get("_needs_manual_review", False)
                    if not dry_run:
                        db["followers"].setdefault(pseudo, {
                            "status": "pending", "score": None, "scanned_at": None})
                        status = "manual_review" if needs_review else "scanned"
                        db["followers"][pseudo].update({
                            "score":            score,
                            "status":           status,
                            "scanned_at":       datetime.now().timestamp(),
                            "is_private":       data.get("is_private", False),
                            "threads_articles": data.get("threads_articles", 0),
                            "has_bio":          data.get("_has_bio", False),
                            "has_replies":      data.get("_has_replies", False),
                            "has_real_pic":     data.get("_has_real_pic", False),
                            "has_full_name":    data.get("_has_full_name", False),
                            "has_ig_link":      data.get("_has_ig_link", False),
                            "follower_count":   data.get("_follower_count"),
                            "all_posts_recent": data.get("_all_posts_recent", False),
                            "is_spambot":       data.get("_has_spam_keywords", False)
                                                or data.get("_duplicate_ratio", 0) >= 0.5,
                        })
                        save_db(db)
                    if needs_review:
                        log_fn(f"  🔍 [{i+1}/{total}] @{pseudo} → "
                               f"📋 À vérifier {score}/100{dry_tag}")
                    elif score >= threshold:
                        log_fn(f"  🔍 [{i+1}/{total}] @{pseudo} → "
                               f"🚨 FAKE {score}/100{dry_tag}")
                    else:
                        log_fn(f"  🔍 [{i+1}/{total}] @{pseudo} → "
                               f"✅ OK {score}/100{dry_tag}")
                    log_file("SCAN", pseudo, score, "|".join(details))
                    log_verbose(f"@{pseudo} details: {'|'.join(details)}")

                if not stop_event.is_set() and i < total - 1:
                    if i > 0 and i % get_anti_bot_every() == 0:
                        pause = random.uniform(8, 15)
                        log_verbose(f"Pause anti-bot {int(pause)}s")
                        await isleep(pause, stop_event)
                    else:
                        await isleep(random.uniform(0.4, 0.8), stop_event)

        finally:
            await browser.close()

    # ── Summary ──
    ok_count = recent_results.count(True)
    err_count = recent_results.count(False)
    if err_count > 0:
        log_fn(f"📊 Scan: {ok_count} OK, {err_count} erreurs "
               f"sur {len(recent_results)} traités")

    return results


# ── Clean ─────────────────────────────────────────────────────────────────────
async def run_clean_async(fakes, db, log_fn, progress_fn, stop_event):
    rate_limited = False
    async with async_playwright() as p:
        browser   = await p.chromium.connect_over_cdp("http://127.0.0.1:9222")
        ctx, page = await _get_page(browser)
        total     = len(fakes)

        # ── Error tracking ────────────────────────────────────────
        consecutive_errors = 0
        self_429 = [False]  # mutable for closure

        try:
            for i, (pseudo, data) in enumerate(fakes):
                if stop_event.is_set():
                    log_fn("⏹️  Nettoyage interrompu — progression sauvegardée.")
                    break

                # ── Health check ──
                if consecutive_errors >= CONSECUTIVE_ERROR_LIMIT:
                    log_fn(f"🛑 Arrêt auto — {consecutive_errors} erreurs "
                           f"consécutives (attendez 15-30 min)")
                    stop_event.set()
                    break

                if not can_act(db):
                    wait_s = seconds_until_next_hour()
                    log_fn(f"⏸️ Limite horaire — reprise dans {wait_s // 60} min")
                    await isleep(wait_s + 5, stop_event)
                    if stop_event.is_set():
                        break
                    if not can_act(db):
                        log_fn(f"🛑 Limite journalière ({get_limit_day()}) atteinte")
                        break

                progress_fn(i + 1, total, pseudo)
                t_clean_start = time.time()

                try:
                    t0 = time.time()
                    if not await _navigate_to_profile(page, pseudo):
                        log_fn(f"  🧹 [{i+1}/{total}] @{pseudo} → ❌ nav impossible")
                        log_file("ERROR", pseudo, data.get("score"), "nav_failed")
                        consecutive_errors += 1
                        continue
                    trace("CLEAN", f"@{pseudo}: nav {time.time()-t0:.1f}s → {page.url[:50]}")
                except RateLimitError:
                    log_fn(f"  ⛔ [{i+1}/{total}] @{pseudo} → HTTP 429 Rate Limit")
                    log_fn("🛑 Threads bloque les requêtes — arrêt immédiat")
                    log_file("429_RATE_LIMIT", pseudo)
                    stop_event.set()
                    self_429[0] = True
                    break
                except Exception as e:
                    log_fn(f"  🧹 [{i+1}/{total}] @{pseudo} → ❌ erreur nav")
                    log_verbose(f"@{pseudo} nav error: {e}")
                    log_file("ERROR", pseudo, data.get("score"), str(e))
                    consecutive_errors += 1
                    continue

                await isleep(random.uniform(1.0, 2.0), stop_event)
                if stop_event.is_set():
                    break

                # Étape 1 : ⋯ (celui à côté de la cloche/IG)
                t1 = time.time()
                log_verbose(f"@{pseudo}: recherche bouton ⋯")
                if not await _click_three_dots(page, log_verbose):
                    log_fn(f"  🧹 [{i+1}/{total}] @{pseudo} → ⚠️ bouton ⋯ introuvable")
                    # Diagnostic DOM → file only
                    try:
                        diag = await page.evaluate(r"""
                        () => ({
                            url: location.href,
                            ig_link: !!document.querySelector('a[href*="instagram.com"]'),
                            buttons: document.querySelectorAll('button,[role="button"]').length,
                            svg_btns: Array.from(document.querySelectorAll('button,[role="button"]'))
                                .filter(b => b.querySelector('svg') && (b.innerText||'').trim() === '')
                                .map(b => {
                                    const r = b.getBoundingClientRect();
                                    return {top: Math.round(r.top), left: Math.round(r.left),
                                            w: Math.round(r.width), h: Math.round(r.height)};
                                }),
                            body_snippet: (document.body?.innerText||'').substring(0, 150),
                        })
                        """)
                        trace("DOTS_DIAG", f"@{pseudo}: {diag}")
                    except Exception:
                        pass
                    await page.screenshot(path=f"debug_dots_{pseudo}.png")
                    log_file("SKIP", pseudo, data.get("score"),
                             "bouton ... introuvable")
                    db["followers"][pseudo]["status"] = "skipped"
                    save_db(db)
                    continue
                trace("CLEAN", f"@{pseudo}: dots {time.time()-t1:.1f}s")

                await isleep(0.7, stop_event)
                if stop_event.is_set():
                    break

                # Étape 2 : "Supprimer follower" ou "Bloquer" en fallback
                t2 = time.time()
                refollow_count = data.get("refollow_count", 0)
                force_block = refollow_count >= 2
                log_verbose(f"@{pseudo}: force_block={force_block} refollow={refollow_count}")
                action = await _click_remove_follower(page, force_block=force_block)
                trace("CLEAN", f"@{pseudo}: remove_follower={action} {time.time()-t2:.1f}s")
                if not action:
                    log_fn(f"  🧹 [{i+1}/{total}] @{pseudo} → ⚠️ option introuvable")
                    # Diagnostic → file only
                    try:
                        menu_diag = await page.evaluate(r"""
                        () => {
                            const items = document.querySelectorAll(
                                '[role="menu"] *, [role="menuitem"], [role="dialog"] [role="button"], [role="dialog"] button'
                            );
                            return Array.from(items)
                                .map(el => (el.textContent || '').trim())
                                .filter(t => t.length > 0 && t.length < 60)
                                .filter((v, i, a) => a.indexOf(v) === i)
                                .slice(0, 15);
                        }
                        """)
                        trace("MENU_DIAG", f"@{pseudo}: {menu_diag}")
                    except Exception:
                        pass
                    await page.screenshot(path=f"debug_menu_{pseudo}.png")
                    log_file("SKIP", pseudo, data.get("score"),
                             "option remove/block introuvable")
                    db["followers"][pseudo]["status"] = "skipped"
                    save_db(db)
                    try:
                        await page.keyboard.press("Escape")
                    except Exception:
                        pass
                    continue

                await isleep(0.7, stop_event)
                if stop_event.is_set():
                    break

                # Étape 3 : confirmation
                t3 = time.time()
                await _click_confirm(page)
                trace("CLEAN", f"@{pseudo}: confirm {time.time()-t3:.1f}s")
                await isleep(0.5, stop_event)

                dt_total = time.time() - t_clean_start
                status = "blocked" if action == "blocked" else "removed"
                db["followers"][pseudo]["status"] = status
                db["followers"][pseudo]["removed_at"] = datetime.now().timestamp()
                log_action(db)
                consecutive_errors = 0  # Reset on success
                log_file(status.upper(), pseudo, data.get("score"))

                # ── Simplified GUI log ──
                score_str = f"{data.get('score')}/100"
                cnt = f"[{db['daily']['count']}/{get_limit_day()}/j]"
                if force_block:
                    log_fn(f"  🧹 [{i+1}/{total}] @{pseudo} → "
                           f"🔒 bloqué récidiviste ×{refollow_count} "
                           f"({score_str}) {cnt}")
                elif action == "blocked":
                    log_fn(f"  🧹 [{i+1}/{total}] @{pseudo} → "
                           f"🚫 bloqué ({score_str}) {cnt}")
                else:
                    log_fn(f"  🧹 [{i+1}/{total}] @{pseudo} → "
                           f"✅ supprimé ({score_str}) {cnt}")

                if stop_event.is_set():
                    break

                pause = random.uniform(get_pause_min(), get_pause_max())
                log_verbose(f"Pause {int(pause)}s avant prochain clean")
                await isleep(pause, stop_event)

        finally:
            await browser.close()

    return {"rate_limited": self_429[0]}


# ── Autopilot ─────────────────────────────────────────────────────────────────
AUTOPILOT_REFETCH_EVERY = 3  # Re-fetch followers every N cycles to catch re-follows

async def autopilot_loop(db, log_fn, progress_fn, stop_event,
                         threshold_fn, stats_fn):
    log_fn("🤖 Autopilot démarré")
    cycle = 0
    while not stop_event.is_set():
        cycle += 1

        # ── Re-fetch followers periodically to detect re-follows ────
        if cycle % AUTOPILOT_REFETCH_EVERY == 0:
            log_fn(f"♻️ Cycle {cycle} — re-fetch abonnés...")
            removed_before = {u for u, d in db["followers"].items()
                              if d["status"] in ("removed", "blocked")}
            try:
                await fetch_followers_async(db, log_fn, stop_event)
            except Exception as e:
                log_fn(f"  ⚠️ Re-fetch échoué: {type(e).__name__}")
            refollowed = [u for u in removed_before
                          if db["followers"].get(u, {}).get("status") == "pending"]
            if refollowed:
                log_fn(f"  🔄 {len(refollowed)} re-follows détectés")
                log_verbose(f"Re-follows: {refollowed[:10]}")
            stats_fn()
            if stop_event.is_set():
                break

        pending = get_pending(db)
        log_fn(f"── Cycle {cycle} — {len(pending)} à scanner ──")

        if pending:
            scan_results = await run_scan_async(
                pending[:get_scan_batch()], db, log_fn,
                threshold_fn(), progress_fn, stop_event
            )
            stats_fn()
            if scan_results.get("__429_DETECTED__"):
                return {"rate_limited": True}

        if stop_event.is_set():
            break

        fakes = get_fakes(db, threshold_fn())
        if fakes:
            repeat_fakes = [(u, d) for u, d in fakes
                            if d.get("refollow_count", 0) > 0]
            rf_info = f" ({len(repeat_fakes)} récidivistes)" if repeat_fakes else ""
            log_fn(f"🧹 {len(fakes)} fakes à nettoyer{rf_info}")
            clean_result = await run_clean_async(
                fakes[:get_clean_batch()], db, log_fn,
                progress_fn, stop_event
            )
            stats_fn()
            if clean_result and clean_result.get("rate_limited"):
                return {"rate_limited": True}
        else:
            log_fn("✨ Aucun fake ce cycle")

        if stop_event.is_set():
            break

        if not get_pending(db) and not get_fakes(db, threshold_fn()):
            log_fn("🎉 Nettoyage complet !")
            break

        log_fn("⏸️ Pause 60s...")
        await isleep(60, stop_event)

    log_fn("⏹️ Autopilot arrêté")
    return {"rate_limited": False}


# ══════════════════════════════════════════════════════════════════════════════
# GUI
# ══════════════════════════════════════════════════════════════════════════════
class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Wav Fake Cleaner")
        self.geometry("1120x860")
        self.resizable(False, False)
        self.db          = load_db()
        self._stop_event = threading.Event()
        self._running    = False
        self._build()
        self._start_periodic_refresh()
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    # ── Refresh toutes les 2s ─────────────────────────────────────────────────
    def _start_periodic_refresh(self):
        self._refresh_stats_impl()
        self.after(2000, self._start_periodic_refresh)

    # ── Thread-safe ───────────────────────────────────────────────────────────
    def log(self, msg):
        self.after(0, self._log_impl, msg)

    def _log_impl(self, msg):
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_box.configure(state="normal")
        self.log_box.insert("end", f"[{ts}] {msg}\n")
        self.log_box.see("end")
        self.log_box.configure(state="disabled")

    def _progress_fn(self, done, total, pseudo):
        self.after(0, self._progress_impl, done, total, pseudo)

    def _progress_impl(self, done, total, pseudo):
        self.label_progress.configure(text=f"@{pseudo} ({done}/{total})")
        self.progress.set(done / max(total, 1))

    def _refresh_stats(self):
        self.after(0, self._refresh_stats_impl)

    def _refresh_stats_impl(self):
        db        = self.db
        total     = len(db["followers"])
        pending   = len(get_pending(db))
        threshold = int(self.slider_threshold.get())
        fakes     = len(get_fakes(db, threshold))
        removed   = sum(1 for d in db["followers"].values()
                        if d["status"] in ("removed", "blocked"))
        review    = sum(1 for d in db["followers"].values()
                        if d["status"] == "manual_review")
        refollows = sum(d.get("refollow_count", 0)
                        for d in db["followers"].values())
        wl        = len(db.get("whitelist", []))
        d_count   = db["daily"].get("count", 0)
        h_count   = db.get("hourly", {}).get("count", 0)
        ld, lh    = get_limit_day(), get_limit_hour()

        self._update_card(self.card_total,     total)
        self._update_card(self.card_pending,   pending,
                          "#f39c12" if pending > 0 else "gray")
        self._update_card(self.card_fakes,     fakes,
                          "#e74c3c" if fakes > 0 else "gray")
        self._update_card(self.card_removed,   removed,
                          "#2ecc71" if removed > 0 else "gray")
        self._update_card(self.card_review,    review,
                          "#e67e22" if review > 0 else "gray")
        self._update_card(self.card_refollow,  refollows,
                          "#9b59b6" if refollows > 0 else "gray")
        self._update_card(self.card_whitelist, wl,
                          "#3498db" if wl > 0 else "gray")
        self._update_card(self.card_day,   f"{d_count}/{ld}",
                          "#e74c3c" if d_count >= ld else "gray")
        self._update_card(self.card_hour,  f"{h_count}/{lh}",
                          "#e74c3c" if h_count >= lh else "#f39c12")

        if fakes > 0:
            days = fakes / ld
            eta  = (datetime.now() + timedelta(days=days)).strftime("%d/%m/%Y")
            self.label_eta.configure(
                text=f"📅 ETA : ~{days:.1f}j ({fakes} fakes @ {ld}/j) → {eta}"
            )
        else:
            self.label_eta.configure(text="")

    # ── Running state — Stop TOUJOURS actif ───────────────────────────────────
    def _set_running(self, running: bool):
        self.after(0, self._set_running_impl, running)

    def _set_running_impl(self, running: bool):
        self._running = running
        idle = "disabled" if running else "normal"
        for btn in [self.btn_chrome, self.btn_fetch, self.btn_scan,
                    self.btn_clean, self.btn_rescan, self.btn_auto,
                    self.btn_export, self.btn_reset, self.btn_whitelist,
                    self.btn_review]:
            btn.configure(state=idle)
        if running:
            self.btn_stop.configure(
                state="normal", fg_color="#c0392b",
                hover_color="#e74c3c", text="⏹️  ARRÊTER"
            )
        else:
            self.btn_stop.configure(
                state="disabled", fg_color="#4a4a4a",
                hover_color="#4a4a4a", text="⏹️  ARRÊTER"
            )

    def _task_done(self, label: str = ""):
        if label:
            self.after(0, self.label_progress.configure, {"text": label})
        self.after(0, self.progress.set, 0)
        self._set_running(False)

    def _show_rate_limit_popup(self):
        """Show a popup when Threads returns HTTP 429."""
        def _show():
            messagebox.showwarning(
                "⛔ Rate Limit Threads (HTTP 429)",
                "Threads a bloqué temporairement les requêtes.\n\n"
                "Votre adresse IP est rate-limitée (trop de visites de profils).\n\n"
                "Actions recommandées :\n"
                "  1. Fermez Chrome\n"
                "  2. Attendez au minimum 6 heures\n"
                "  3. Vérifiez manuellement que threads.com\n"
                "     fonctionne avant de relancer\n\n"
                "⚠️  Relancer trop tôt peut prolonger le blocage.\n\n"
                "Toutes les actions ont été stoppées automatiquement."
            )
        self.after(0, _show)

    # ── Build UI ──────────────────────────────────────────────────────────────
    def _build(self):
        self.sidebar = ctk.CTkFrame(self, width=260, corner_radius=0)
        self.sidebar.pack(side="left", fill="y")
        self.sidebar.pack_propagate(False)

        # Scrollable inner frame for all sidebar content
        sb = ctk.CTkScrollableFrame(self.sidebar, width=240,
                                     fg_color="transparent")
        sb.pack(fill="both", expand=True)

        ctk.CTkLabel(sb, text="Wav Fake Cleaner",
                     font=ctk.CTkFont(size=18, weight="bold")).pack(pady=(12, 0))
        ctk.CTkLabel(sb, text="for Threads",
                     font=ctk.CTkFont(size=12),
                     text_color="gray").pack(pady=(0, 2))
        link = ctk.CTkLabel(sb, text="by Fred Wav",
                     font=ctk.CTkFont(size=11),
                     text_color="#3498db", cursor="hand2")
        link.pack(pady=(0, 4))
        link.bind("<Button-1>",
                  lambda e: webbrowser.open("https://www.threads.com/@fredwavoff"))

        ctk.CTkButton(
            sb, text="☕ Faire un don",
            fg_color="#0070ba", hover_color="#003087",
            text_color="white", height=28,
            font=ctk.CTkFont(size=11, weight="bold"),
            command=lambda: webbrowser.open("http://paypal.me/fredwav")
        ).pack(padx=30, fill="x", pady=(0, 8))

        ctk.CTkLabel(sb, text="Votre pseudo (@)",
                     font=ctk.CTkFont(size=12)).pack(padx=12, anchor="w")
        self.entry_pseudo = ctk.CTkEntry(
            sb, placeholder_text="monpseudo"
        )
        self.entry_pseudo.pack(padx=12, fill="x", pady=(2, 8))
        if self.db.get("username"):
            self.entry_pseudo.insert(0, self.db["username"])

        ctk.CTkLabel(sb, text="Seuil fake (score)",
                     font=ctk.CTkFont(size=12)).pack(padx=12, anchor="w")
        self.slider_threshold = ctk.CTkSlider(
            sb, from_=50, to=100, number_of_steps=10,
            command=self._on_threshold
        )
        self.slider_threshold.set(70)
        self.slider_threshold.pack(padx=12, fill="x", pady=(2, 1))
        self.label_threshold = ctk.CTkLabel(
            sb, text="Seuil : 70/100",
            font=ctk.CTkFont(size=11), text_color="gray"
        )
        self.label_threshold.pack(padx=12, anchor="w", pady=(0, 6))

        self.var_strict_private = ctk.BooleanVar(value=False)
        self.chk_strict_private = ctk.CTkCheckBox(
            sb, text="Filtrage strict (privés)",
            variable=self.var_strict_private,
            command=self._on_private_mode,
            font=ctk.CTkFont(size=11),
        )
        self.chk_strict_private.pack(padx=12, anchor="w", pady=(0, 6))

        ctk.CTkLabel(sb, text="Profil de sécurité",
                     font=ctk.CTkFont(size=12)).pack(padx=12, anchor="w")
        self.seg_profile = ctk.CTkSegmentedButton(
            sb,
            values=["Prudent", "Normal", "Agressif"],
            command=self._on_profile
        )
        self.seg_profile.set("Normal")
        self.seg_profile.pack(padx=12, fill="x", pady=(2, 1))
        self.label_profile_info = ctk.CTkLabel(
            sb,
            text=f"{get_limit_day()}/j · {get_limit_hour()}/h · "
                 f"scan {get_scan_batch()}/batch · pause {get_pause_min()}-{get_pause_max()}s",
            font=ctk.CTkFont(size=10), text_color="gray"
        )
        self.label_profile_info.pack(padx=12, anchor="w", pady=(1, 6))

        ctk.CTkLabel(sb, text="Vitesse scroll fetch",
                     font=ctk.CTkFont(size=12)).pack(padx=12, anchor="w")
        self.slider_speed = ctk.CTkSlider(
            sb, from_=40, to=300, number_of_steps=13,
            command=self._on_speed
        )
        self.slider_speed.set(120)
        self.slider_speed.pack(padx=12, fill="x", pady=(2, 1))
        self.label_speed = ctk.CTkLabel(
            sb, text="Scroll : 120 px/frame",
            font=ctk.CTkFont(size=11), text_color="gray"
        )
        self.label_speed.pack(padx=12, anchor="w", pady=(0, 6))

        sep = lambda t: ctk.CTkLabel(
            sb, text=f"── {t} ──",
            font=ctk.CTkFont(size=10), text_color="gray"
        ).pack(pady=(4, 2))

        sep("Étapes")

        ctk.CTkLabel(sb, text="1.",
                     font=ctk.CTkFont(size=11), text_color="gray"
                     ).pack(padx=12, anchor="w", pady=(2, 0))
        self.btn_chrome = ctk.CTkButton(
            sb, text="🚀 Lancer Chrome",
            command=self._start_chrome
        )
        self.btn_chrome.pack(padx=12, fill="x", pady=2)

        ctk.CTkLabel(sb, text="2.",
                     font=ctk.CTkFont(size=11), text_color="gray"
                     ).pack(padx=12, anchor="w", pady=(2, 0))
        self.btn_fetch = ctk.CTkButton(
            sb, text="📥 Récupérer abonnés",
            command=self._run_fetch, state="disabled"
        )
        self.btn_fetch.pack(padx=12, fill="x", pady=2)

        ctk.CTkLabel(sb, text="3.",
                     font=ctk.CTkFont(size=11), text_color="gray"
                     ).pack(padx=12, anchor="w", pady=(2, 0))
        self.btn_scan = ctk.CTkButton(
            sb, text="🔍 Scanner les profils",
            fg_color="#2980b9", hover_color="#3498db",
            command=self._run_scan, state="disabled"
        )
        self.btn_scan.pack(padx=12, fill="x", pady=2)

        ctk.CTkLabel(sb, text="4.",
                     font=ctk.CTkFont(size=11), text_color="gray"
                     ).pack(padx=12, anchor="w", pady=(2, 0))
        self.btn_clean = ctk.CTkButton(
            sb, text="🧹 Nettoyer les fakes",
            fg_color="#1a6b3c", hover_color="#27ae60",
            command=self._run_clean, state="disabled"
        )
        self.btn_clean.pack(padx=12, fill="x", pady=2)

        self.btn_stop = ctk.CTkButton(
            sb, text="⏹️  ARRÊTER",
            fg_color="#c0392b", hover_color="#e74c3c",
            command=self._stop_all, state="disabled"
        )
        self.btn_stop.pack(padx=12, fill="x", pady=(8, 2))

        sep("Outils")

        self.btn_auto = ctk.CTkButton(
            sb, text="🤖 Autopilot (scan+clean)",
            fg_color="#7f4b00", hover_color="#b36b00",
            border_width=1,
            command=self._run_autopilot, state="disabled"
        )
        self.btn_auto.pack(padx=12, fill="x", pady=2)

        self.btn_rescan = ctk.CTkButton(
            sb, text="🔄 Rescanner",
            fg_color="transparent", border_width=1,
            text_color="#aaaaaa",
            command=self._run_rescan, state="disabled"
        )
        self.btn_rescan.pack(padx=12, fill="x", pady=2)

        self.btn_export = ctk.CTkButton(
            sb, text="📊 Exporter CSV",
            fg_color="transparent", border_width=1,
            text_color="#aaaaaa", command=self._export_csv
        )
        self.btn_export.pack(padx=12, fill="x", pady=2)

        self.btn_whitelist = ctk.CTkButton(
            sb, text="🛡️ Gérer whitelist",
            fg_color="transparent", border_width=1,
            text_color="#aaaaaa", command=self._manage_whitelist
        )
        self.btn_whitelist.pack(padx=12, fill="x", pady=2)

        self.btn_review = ctk.CTkButton(
            sb, text="📋 À vérifier",
            fg_color="transparent", border_width=1,
            text_color="#e67e22", command=self._manage_review
        )
        self.btn_review.pack(padx=12, fill="x", pady=2)

        self.btn_reset_counters = ctk.CTkButton(
            sb, text="🔢 Reset compteurs jour/heure",
            fg_color="transparent", border_width=1,
            text_color="#aaaaaa", command=self._reset_counters
        )
        self.btn_reset_counters.pack(padx=12, fill="x", pady=2)

        self.btn_reset = ctk.CTkButton(
            sb, text="🔄 Reset DB",
            fg_color="transparent", border_width=1,
            text_color="#666666", command=self._reset_db
        )
        self.btn_reset.pack(padx=12, fill="x", pady=(2, 12))

        # ── Zone principale ───────────────────────────────────────────────────
        main = ctk.CTkFrame(self, fg_color="transparent")
        main.pack(side="right", fill="both", expand=True, padx=14, pady=14)

        row1 = ctk.CTkFrame(main, fg_color="transparent")
        row1.pack(fill="x", pady=(0, 6))

        self.card_total     = self._stat_card(row1, "Total",      "0", "gray")
        self.card_pending   = self._stat_card(row1, "À scanner",  "0", "#f39c12")
        self.card_fakes     = self._stat_card(row1, "Fakes",      "0", "#e74c3c")
        self.card_removed   = self._stat_card(row1, "Retirés",    "0", "#2ecc71")
        self.card_review    = self._stat_card(row1, "À vérifier", "0", "#e67e22")
        self.card_refollow  = self._stat_card(row1, "Re-follows", "0", "#9b59b6")
        self.card_whitelist = self._stat_card(row1, "Whitelist",  "0", "#3498db")
        self.card_day       = self._stat_card(
            row1, "Actes/jour",  f"0/{get_limit_day()}", "gray")
        self.card_hour      = self._stat_card(
            row1, "Actes/heure", f"0/{get_limit_hour()}", "gray")

        for c in [self.card_total, self.card_pending, self.card_fakes,
                  self.card_removed, self.card_review, self.card_refollow,
                  self.card_whitelist, self.card_day, self.card_hour]:
            c.pack(side="left", expand=True, fill="x", padx=2)

        self.label_eta = ctk.CTkLabel(
            main, text="", font=ctk.CTkFont(size=11), text_color="#3498db"
        )
        self.label_eta.pack(anchor="w", pady=(0, 4))

        self.progress = ctk.CTkProgressBar(main)
        self.progress.set(0)
        self.progress.pack(fill="x", pady=(0, 2))

        self.label_progress = ctk.CTkLabel(
            main, text="En attente...",
            font=ctk.CTkFont(size=11), text_color="gray"
        )
        self.label_progress.pack(anchor="w", pady=(0, 6))

        self.log_box = ctk.CTkTextbox(
            main, font=ctk.CTkFont(family="Consolas", size=11),
            fg_color="#111111", text_color="#cccccc", wrap="word"
        )
        self.log_box.pack(fill="both", expand=True)
        self.log_box.configure(state="disabled")

    def _stat_card(self, parent, label, value, color):
        frame = ctk.CTkFrame(parent)
        ctk.CTkLabel(frame, text=label,
                     font=ctk.CTkFont(size=10),
                     text_color="gray").pack(pady=(8, 1))
        val = ctk.CTkLabel(frame, text=str(value),
                           font=ctk.CTkFont(size=16, weight="bold"),
                           text_color=color)
        val.pack(pady=(0, 8))
        frame._val_label = val
        return frame

    def _update_card(self, card, value, color=None):
        card._val_label.configure(text=str(value))
        if color:
            card._val_label.configure(text_color=color)

    # ── Callbacks ─────────────────────────────────────────────────────────────
    def _on_threshold(self, val):
        self.label_threshold.configure(text=f"Seuil : {int(val)}/100")

    def _on_private_mode(self):
        strict = self.var_strict_private.get()
        set_strict_private_mode(strict)
        mode = "strict (ancien)" if strict else "intelligent (nouveau)"
        self.log(f"⚙️ Filtrage privés : {mode}")

    def _on_profile(self, name):
        global _profile
        _profile = SAFETY_PROFILES[name].copy()
        self.label_profile_info.configure(
            text=f"{get_limit_day()}/j · {get_limit_hour()}/h · "
                 f"scan {get_scan_batch()}/batch · pause {get_pause_min()}-{get_pause_max()}s"
        )
        self.log(f"⚙️ Profil «{name}» — "
                 f"{get_limit_day()}/j · {get_limit_hour()}/h · "
                 f"scan {get_scan_batch()}/batch")

    def _on_speed(self, val):
        self.label_speed.configure(text=f"Scroll : {int(val)} px/frame")

    # ── Actions ───────────────────────────────────────────────────────────────
    def _start_chrome(self):
        if self._running:
            return
        self.btn_chrome.configure(state="disabled", text="⏳ Démarrage...")
        def task():
            ok = launch_chrome()
            if ok:
                self.log("✅ Chrome prêt")
                self.after(0, lambda: [
                    b.configure(state="normal")
                    for b in [self.btn_fetch, self.btn_scan,
                               self.btn_clean, self.btn_rescan,
                               self.btn_auto]
                ])
                self.after(0, self.btn_chrome.configure,
                           {"text": "✅ Chrome actif", "state": "normal"})
            else:
                self.log("❌ Chrome introuvable ou timeout.")
                self.after(0, self.btn_chrome.configure,
                           {"state": "normal", "text": "🚀 Lancer Chrome"})
        threading.Thread(target=task, daemon=True).start()

    def _stop_all(self):
        if not self._running:
            return
        self._stop_event.set()
        self.log("⏹️  Arrêt demandé — fin de l'opération en cours...")
        self.after(0, self.btn_stop.configure,
                   {"text": "⏳ Arrêt...", "state": "disabled"})

    def _run_fetch(self):
        if self._running:
            return
        username = self.entry_pseudo.get().strip().lstrip("@")
        if not username:
            self.log("❌ Entrez votre pseudo.")
            return
        self.db["username"] = username
        save_db(self.db)
        self._stop_event.clear()
        self._set_running(True)
        speed = int(self.slider_speed.get())
        def task():
            asyncio.run(fetch_followers_async(
                self.db, self.log, self._stop_event, speed
            ))
            self._task_done("Récupération terminée ✅")
        threading.Thread(target=task, daemon=True).start()

    def _run_scan(self):
        if self._running:
            return
        self._stop_event.clear()
        self._set_running(True)
        def task():
            pending = get_pending(self.db)[:get_scan_batch()]
            if not pending:
                self.log("📋 Aucun profil à scanner.")
                self._task_done()
                return
            results = asyncio.run(run_scan_async(
                pending, self.db, self.log,
                int(self.slider_threshold.get()),
                self._progress_fn, self._stop_event
            ))
            if results.get("__429_DETECTED__"):
                self._show_rate_limit_popup()
                self._task_done("⛔ Rate limit — en pause")
            else:
                self._task_done("Scan terminé ✅")
        threading.Thread(target=task, daemon=True).start()

    def _run_dry_scan(self):
        if self._running:
            return
        self._stop_event.clear()
        self._set_running(True)
        def task():
            pending = get_pending(self.db)[:get_scan_batch()]
            if not pending:
                self.log("📋 Aucun profil à analyser.")
                self._task_done()
                return
            self.log("🔬 Mode DRY RUN — aucune écriture en DB.")
            asyncio.run(run_scan_async(
                pending, self.db, self.log,
                int(self.slider_threshold.get()),
                self._progress_fn, self._stop_event,
                dry_run=True
            ))
            self._task_done("Dry run terminé ✅ (rien écrit)")
        threading.Thread(target=task, daemon=True).start()

    def _run_rescan(self):
        """Reset all 'scanned' accounts to 'pending' and run a scan batch."""
        if self._running:
            return
        scanned = [u for u, d in self.db["followers"].items()
                   if d["status"] == "scanned"]
        if not scanned:
            self.log("📋 Aucun profil déjà scanné à rescanner.")
            return
        if not messagebox.askyesno(
            "Rescanner",
            f"{len(scanned)} profils déjà scannés vont être\n"
            f"remis en file d'attente et rescannés.\n\n"
            f"Les scores seront recalculés avec le barème actuel.\n\n"
            f"Continuer ?"
        ):
            self.log("🚫 Rescan annulé.")
            return
        for pseudo in scanned:
            self.db["followers"][pseudo]["status"] = "pending"
        save_db(self.db)
        self.log(f"🔄 {len(scanned)} profils remis en 'pending'.")
        # Launch scan
        self._stop_event.clear()
        self._set_running(True)
        def task():
            pending = get_pending(self.db)[:get_scan_batch()]
            if not pending:
                self.log("📋 Rien à scanner.")
                self._task_done()
                return
            self.log(f"🔍 Rescan de {len(pending)} profils...")
            results = asyncio.run(run_scan_async(
                pending, self.db, self.log,
                int(self.slider_threshold.get()),
                self._progress_fn, self._stop_event
            ))
            if results.get("__429_DETECTED__"):
                self._show_rate_limit_popup()
                self._task_done("⛔ Rate limit — en pause")
            else:
                self._task_done("Rescan terminé ✅")
        threading.Thread(target=task, daemon=True).start()

    def _run_clean(self):
        if self._running:
            return
        threshold = int(self.slider_threshold.get())
        fakes = get_fakes(self.db, threshold)
        if not fakes:
            self.log("✨ Aucun fake à retirer.")
            return
        preview = fakes[:get_clean_batch()]
        lines   = "\n".join(
            f"  @{u:<30} {d.get('score')}/100"
            for u, d in preview[:15]
        )
        suffix = f"\n  (+{len(preview)-15} autres...)" if len(preview) > 15 else ""
        if not messagebox.askyesno(
            "Confirmer le nettoyage",
            f"{len(preview)} comptes vont être retirés :\n\n{lines}{suffix}"
            f"\n\nContinuer ?"
        ):
            self.log("🚫 Nettoyage annulé.")
            return
        self._stop_event.clear()
        self._set_running(True)
        def task():
            result = asyncio.run(run_clean_async(
                preview, self.db, self.log,
                self._progress_fn, self._stop_event
            ))
            if result and result.get("rate_limited"):
                self._show_rate_limit_popup()
                self._task_done("⛔ Rate limit — en pause")
            else:
                self._task_done("Nettoyage terminé ✅")
        threading.Thread(target=task, daemon=True).start()

    def _run_autopilot(self):
        if self._running:
            return
        username = self.entry_pseudo.get().strip().lstrip("@")
        if not username:
            self.log("❌ Entrez votre pseudo.")
            return

        # ── Popup de confirmation avec résumé ──
        pending = len(get_pending(self.db))
        threshold = int(self.slider_threshold.get())
        fakes = len(get_fakes(self.db, threshold))
        ld, lh = get_limit_day(), get_limit_hour()
        eta_days = max(fakes / ld, 0.1) if fakes else 0
        refollows = sum(1 for d in self.db["followers"].values()
                        if d.get("refollow_count", 0) > 0)

        msg = (
            "🤖 MODE AUTOPILOT\n\n"
            "L'application va tourner en continu :\n"
            "  • Scanner tous les profils en attente\n"
            "  • Supprimer/bloquer les fakes détectés\n"
            "  • Re-fetch les abonnés tous les 3 cycles\n"
            "  • Bloquer automatiquement les récidivistes\n\n"
            f"État actuel :\n"
            f"  📋 {pending} profils à scanner\n"
            f"  🎯 {fakes} fakes à retirer (seuil {threshold}/100)\n"
            f"  ♻️  {refollows} re-followers connus\n"
            f"  ⚡ Limite : {ld}/jour, {lh}/heure\n"
            f"  📅 ETA : ~{eta_days:.0f} jours\n\n"
            "⚠️  Peut tourner des heures ou des jours.\n"
            "Appuyez sur ARRÊTER à tout moment pour stopper."
        )

        if not messagebox.askyesno("Lancer l'Autopilot ?", msg):
            self.log("🚫 Autopilot annulé.")
            return

        self.db["username"] = username
        save_db(self.db)
        self._stop_event.clear()
        self._set_running(True)
        self.log("─" * 60)
        def task():
            result = asyncio.run(autopilot_loop(
                self.db, self.log, self._progress_fn,
                self._stop_event,
                lambda: int(self.slider_threshold.get()),
                self._refresh_stats
            ))
            if result and result.get("rate_limited"):
                self._show_rate_limit_popup()
                self._task_done("⛔ Rate limit — en pause")
            else:
                self._task_done("Autopilot arrêté ✅")
        threading.Thread(target=task, daemon=True).start()

    def _export_csv(self):
        if self._running:
            self.log("⚠️  Export impossible pendant une opération.")
            return
        path = export_csv(self.db)
        self.log(f"📊 Export CSV : {path}")
        messagebox.showinfo("Export réussi", f"Fichier enregistré :\n{path}")

    def _manage_review(self):
        """Window to review manual_review accounts."""
        reviews = [(u, d) for u, d in self.db["followers"].items()
                   if d["status"] == "manual_review"]

        if not reviews:
            messagebox.showinfo("À vérifier", "Aucun compte en attente de vérification.")
            return

        win = ctk.CTkToplevel(self)
        win.title("📋 Comptes à vérifier")
        win.geometry("600x550")
        win.grab_set()

        ctk.CTkLabel(
            win, text=f"{len(reviews)} comptes privés à vérifier",
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(padx=16, pady=(16, 4))

        ctk.CTkLabel(
            win, text="Cochez les comptes à supprimer, les autres seront whitelistés",
            font=ctk.CTkFont(size=11), text_color="gray"
        ).pack(padx=16, pady=(0, 8))

        # Scrollable list
        scroll = ctk.CTkScrollableFrame(win, fg_color="transparent")
        scroll.pack(padx=16, fill="both", expand=True, pady=(0, 8))

        checkboxes = {}
        for pseudo, data in sorted(reviews, key=lambda x: x[1].get("score", 0),
                                    reverse=True):
            fc = data.get("follower_count", "?")
            score = data.get("score", "?")
            bio = "✓bio" if data.get("has_bio") else "✗bio"
            pic = "✓pic" if data.get("has_real_pic") else "✗pic"

            var = ctk.BooleanVar(value=False)
            row = ctk.CTkFrame(scroll, fg_color="transparent")
            row.pack(fill="x", pady=1)

            cb = ctk.CTkCheckBox(
                row, text="",
                variable=var, width=24
            )
            cb.pack(side="left", padx=(0, 4))

            # Clickable username → opens profile in browser
            lbl_user = ctk.CTkLabel(
                row, text=f"@{pseudo}",
                font=ctk.CTkFont(size=12, weight="bold"),
                text_color="#3498db", cursor="hand2"
            )
            lbl_user.pack(side="left", padx=(0, 8))
            lbl_user.bind("<Button-1>",
                          lambda e, u=pseudo: webbrowser.open(
                              f"https://www.threads.net/@{u}"))

            ctk.CTkLabel(
                row, text=f"{fc} abn · {score}/100 · {bio} · {pic}",
                font=ctk.CTkFont(size=11), text_color="gray"
            ).pack(side="left")

            checkboxes[pseudo] = var

        # Action buttons
        btn_frame = ctk.CTkFrame(win, fg_color="transparent")
        btn_frame.pack(padx=16, fill="x", pady=(0, 8))

        def _select_all():
            for v in checkboxes.values():
                v.set(True)

        def _select_none():
            for v in checkboxes.values():
                v.set(False)

        ctk.CTkButton(
            btn_frame, text="Tout cocher", width=100,
            fg_color="transparent", border_width=1,
            text_color="#aaaaaa", command=_select_all
        ).pack(side="left", padx=(0, 4))

        ctk.CTkButton(
            btn_frame, text="Tout décocher", width=100,
            fg_color="transparent", border_width=1,
            text_color="#aaaaaa", command=_select_none
        ).pack(side="left")

        def _apply():
            to_fake = []
            to_whitelist = []
            for pseudo, var in checkboxes.items():
                if var.get():
                    to_fake.append(pseudo)
                else:
                    to_whitelist.append(pseudo)

            # Mark checked as scanned (will be picked up by clean)
            for pseudo in to_fake:
                self.db["followers"][pseudo]["status"] = "scanned"

            # Add unchecked to whitelist + mark as scanned
            wl = set(self.db.get("whitelist", []))
            for pseudo in to_whitelist:
                wl.add(pseudo)
                self.db["followers"][pseudo]["status"] = "scanned"
            self.db["whitelist"] = sorted(wl)

            save_db(self.db)
            self._refresh_stats()
            self.log(f"📋 Vérification : {len(to_fake)} → fakes, "
                     f"{len(to_whitelist)} → whitelist")
            win.destroy()

        ctk.CTkButton(
            win, text="✅ Appliquer",
            fg_color="#1a6b3c", hover_color="#27ae60",
            command=_apply
        ).pack(padx=16, fill="x", pady=(0, 16))

    def _manage_whitelist(self):
        win = ctk.CTkToplevel(self)
        win.title("🛡️ Whitelist")
        win.geometry("420x500")
        win.grab_set()
        ctk.CTkLabel(win, text="Pseudos protégés (un par ligne)",
                     font=ctk.CTkFont(size=13)).pack(padx=16, pady=(16, 4))
        txt = ctk.CTkTextbox(
            win, font=ctk.CTkFont(family="Consolas", size=12)
        )
        txt.pack(padx=16, fill="both", expand=True, pady=(0, 8))
        txt.insert("end", "\n".join(self.db.get("whitelist", [])))
        def _save():
            raw = txt.get("1.0", "end").strip()
            wl  = [x.strip().lstrip("@")
                   for x in raw.splitlines() if x.strip()]
            self.db["whitelist"] = wl
            save_db(self.db)
            self.log(f"🛡️  Whitelist : {len(wl)} pseudo(s) protégé(s).")
            win.destroy()
        ctk.CTkButton(win, text="💾 Sauvegarder",
                      command=_save).pack(padx=16, fill="x", pady=(0, 16))

    def _reset_counters(self):
        self.db["daily"] = {"date": "", "count": 0}
        self.db["hourly"] = {"hour": "", "count": 0}
        save_db(self.db)
        self.log("🔢 Compteurs jour/heure remis à zéro.")

    def _reset_db(self):
        if self._running:
            self.log("⚠️  Impossible de reset pendant une opération.")
            return
        if messagebox.askyesno("Confirmation",
                               "Supprimer toute la base de données ?"):
            for f in [DB_FILE, DB_FILE + ".tmp"]:
                if os.path.exists(f):
                    os.remove(f)
            self.db = load_db()
            self.log("🔄 Base réinitialisée.")

    def _on_close(self):
        self._stop_event.set()
        self.destroy()


if __name__ == "__main__":
    app = App()
    app.mainloop()
