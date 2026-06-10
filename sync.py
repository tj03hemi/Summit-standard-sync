#!/usr/bin/env python3
"""
SS Activewear -> Shopify Product Sync
Summit Standard Co. — summitstandardco.com

Pulls every style for the brands listed in sync_config.json from the
S&S Activewear API and creates/updates fully-populated Shopify products:

  - Custom title, rich HTML description (with customization CTA)
  - Shopify standard product taxonomy category (tax + search metafields)
  - Price / compare-at (sale handling) / weight / barcode (GTIN)
  - Variants (Color x Size) with per-color images attached
  - Inventory quantities from SS warehouse data
  - Status DRAFT on first creation
  - Collections (gender + category based), tags, product type, vendor
  - Gender assignment: mens / womens / youth / unisex (unisex -> mens + womens)
  - SEO title + meta description

Architecture highlights (the operationally important bits):

  CHECKPOINT / RESUME
    checkpoint.json stores, per styleID: a content hash of the SS data that
    matters (price, inventory, availability, images), the Shopify product
    GIDs created for it (one or more if split), and a last-synced timestamp.
    Each run loads the checkpoint, skips styles whose hash is unchanged,
    and SAVES THE CHECKPOINT AFTER EVERY STYLE so a timeout/crash never
    loses progress. The GitHub Actions workflow commits it back to the repo.

  TIME BUDGET
    GitHub Actions kills jobs at 6h. We stop cleanly at `time_budget_minutes`
    (default 290 = ~4.8h), commit the checkpoint, and the next scheduled run
    resumes where we left off. When every style has been visited this cycle,
    the cycle marker resets and the next run starts a fresh full pass.

  100-VARIANT SPLIT
    Shopify hard-caps 100 variants/product. Styles are split automatically:
    colors are packed greedily into groups such that (colors_in_group x
    n_sizes) <= 100. Split products are named
        "{Brand} {Style} {Title} — {FirstColor}–{LastColor}"
    and each split's Shopify GID is tracked independently in the checkpoint
    so subsequent runs UPDATE the same products instead of re-creating.

  RATE LIMITS
    SS: 60 req/min -> token-bucket limiter shared across threads.
    Shopify GraphQL: cost-based; we read throttleStatus from every response
    and sleep when remaining cost is low. Exponential backoff on THROTTLED.
"""

import base64
import hashlib
import html
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

# --------------------------------------------------------------------------
# Paths & config
# --------------------------------------------------------------------------
ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(ROOT, "sync_config.json")
CHECKPOINT_PATH = os.path.join(ROOT, "checkpoint.json")
LOG_DIR = os.path.join(ROOT, "logs")

SS_BASE = "https://api.ssactivewear.com/v2"
SS_CDN = "https://www.ssactivewear.com/"

SHOPIFY_API_VERSION = os.environ.get("SHOPIFY_API_VERSION", "2025-01")

START_TIME = time.monotonic()


def load_json(path, default):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default


CONFIG = load_json(CONFIG_PATH, {})
TIME_BUDGET_SECONDS = CONFIG.get("time_budget_minutes", 290) * 60


def out_of_time():
    return (time.monotonic() - START_TIME) > TIME_BUDGET_SECONDS


# --------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------
class RunLog:
    def __init__(self):
        self.events = []
        self.counts = {
            "styles_processed": 0, "products_created": 0, "products_updated": 0,
            "variants_written": 0, "skipped_no_change": 0, "errors": 0,
            "styles_remaining": 0,
        }
        self.lock = threading.Lock()

    def event(self, level, style, message, **extra):
        with self.lock:
            rec = {"ts": datetime.now(timezone.utc).isoformat(),
                   "level": level, "style": style, "message": message, **extra}
            self.events.append(rec)
            print(f"[{level}] {style}: {message}", flush=True)

    def bump(self, key, n=1):
        with self.lock:
            self.counts[key] = self.counts.get(key, 0) + n

    def save(self):
        os.makedirs(LOG_DIR, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        path = os.path.join(LOG_DIR, f"run_log_{stamp}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"counts": self.counts, "events": self.events}, f, indent=2)
        # GitHub Actions job summary
        summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
        if summary_path:
            with open(summary_path, "a", encoding="utf-8") as f:
                f.write("## SS Activewear Sync Summary\n\n")
                f.write("| Metric | Count |\n|---|---|\n")
                for k, v in self.counts.items():
                    f.write(f"| {k.replace('_', ' ').title()} | {v} |\n")
        return path


LOG = RunLog()


# --------------------------------------------------------------------------
# Rate limiters
# --------------------------------------------------------------------------
class TokenBucket:
    """Simple thread-safe token bucket. SS allows 60 req/min."""

    def __init__(self, rate_per_minute=55):  # stay under 60 for headroom
        self.capacity = rate_per_minute
        self.tokens = rate_per_minute
        self.fill_rate = rate_per_minute / 60.0
        self.last = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self):
        while True:
            with self.lock:
                now = time.monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.fill_rate)
                self.last = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
            time.sleep(0.25)


SS_LIMITER = TokenBucket()


# --------------------------------------------------------------------------
# SS Activewear client
# --------------------------------------------------------------------------
class SSClient:
    def __init__(self):
        user = os.environ["SS_USERNAME"]
        key = os.environ["SS_API_KEY"]
        self.session = requests.Session()
        self.session.auth = (user, key)
        self.session.headers["Accept"] = "application/json"

    def get(self, path, params=None, retries=5):
        url = f"{SS_BASE}/{path.lstrip('/')}"
        for attempt in range(retries):
            SS_LIMITER.acquire()
            try:
                r = self.session.get(url, params=params, timeout=120)
                if r.status_code == 404:
                    return None
                if r.status_code == 429 or r.status_code >= 500:
                    raise requests.HTTPError(f"{r.status_code}", response=r)
                r.raise_for_status()
                return r.json()
            except (requests.RequestException, ValueError) as e:
                wait = min(2 ** attempt * 2, 60)
                if attempt == retries - 1:
                    raise
                LOG.event("WARN", path, f"SS API retry {attempt+1} after error: {e} (sleep {wait}s)")
                time.sleep(wait)

    def all_styles(self):
        return self.get("styles/") or []

    def categories(self):
        return self.get("categories/") or []

    def products_for_style(self, style_id):
        return self.get("products/", params={"styleid": style_id}) or []


# --------------------------------------------------------------------------
# Shopify GraphQL client
# --------------------------------------------------------------------------
class ShopifyClient:
    def __init__(self):
        store = os.environ["SHOPIFY_STORE_URL"].replace("https://", "").strip("/")
        token = os.environ["SHOPIFY_ADMIN_TOKEN"]
        self.endpoint = f"https://{store}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
        self.session = requests.Session()
        self.session.headers.update({
            "X-Shopify-Access-Token": token,
            "Content-Type": "application/json",
        })
        self.lock = threading.Lock()

    def gql(self, query, variables=None, retries=6):
        for attempt in range(retries):
            r = self.session.post(self.endpoint,
                                  json={"query": query, "variables": variables or {}},
                                  timeout=120)
            if r.status_code >= 500:
                time.sleep(min(2 ** attempt, 30))
                continue
            data = r.json()
            # Cost-aware throttling
            throttle = (data.get("extensions", {}) or {}).get("cost", {}).get("throttleStatus")
            if throttle and throttle.get("currentlyAvailable", 1000) < 200:
                time.sleep(2)
            errs = data.get("errors")
            if errs:
                if any("THROTTLED" in str(e.get("extensions", {}).get("code", "")) for e in errs):
                    time.sleep(min(2 ** attempt * 2, 60))
                    continue
                raise RuntimeError(f"Shopify GraphQL errors: {errs}")
            return data["data"]
        raise RuntimeError("Shopify GraphQL: retries exhausted (throttled)")

    # -- lookups ------------------------------------------------------------
    def primary_location_id(self):
        d = self.gql("""{ locations(first: 5) { nodes { id name } } }""")
        nodes = d["locations"]["nodes"]
        want = CONFIG.get("inventory_location_name")
        for n in nodes:
            if want and n["name"].lower() == want.lower():
                return n["id"]
        return nodes[0]["id"]

    def collection_id_by_handle(self, handle):
        d = self.gql("""query($h: String!) { collectionByHandle(handle: $h) { id } }""",
                     {"h": handle})
        node = d.get("collectionByHandle")
        return node["id"] if node else None

    def build_sku_map(self):
        """ADOPTION PASS: page through every product in the store and map
        variant SKU -> product GID. Because both the old sync and this one
        use S&S SKUs (which never change), this lets a fresh checkpoint
        'adopt' products created by the previous sync and UPDATE them in
        place — same product ID, same handle/URL — instead of creating
        duplicates. This is what protects your Google-indexed pages."""
        sku_map, cursor = {}, None
        query = """
        query($cursor: String) {
          products(first: 50, after: $cursor) {
            pageInfo { hasNextPage endCursor }
            nodes {
              id
              variants(first: 100) { nodes { sku } }
            }
          }
        }"""
        while True:
            d = self.gql(query, {"cursor": cursor})
            page = d["products"]
            for prod in page["nodes"]:
                for v in prod["variants"]["nodes"]:
                    if v.get("sku"):
                        sku_map[v["sku"]] = prod["id"]
            if not page["pageInfo"]["hasNextPage"]:
                break
            cursor = page["pageInfo"]["endCursor"]
        return sku_map


SHOPIFY = None  # initialized in main()
LOCATION_ID = None
SKU_MAP = {}    # variant SKU -> existing product GID (adoption pass)
COLLECTION_CACHE = {}
COLLECTION_LOCK = threading.Lock()


def resolve_collection(handle):
    """Resolve a collection handle to a GID once and cache it."""
    with COLLECTION_LOCK:
        if handle in COLLECTION_CACHE:
            return COLLECTION_CACHE[handle]
    gid = SHOPIFY.collection_id_by_handle(handle)
    with COLLECTION_LOCK:
        COLLECTION_CACHE[handle] = gid
    if not gid:
        LOG.event("WARN", handle, "Collection handle not found in Shopify — skipping assignment")
    return gid


# --------------------------------------------------------------------------
# Gender detection
# --------------------------------------------------------------------------
YOUTH_PAT = re.compile(r"\b(youth|toddler|infant|girls?'?|boys?'?|kids?)\b", re.I)
WOMENS_PAT = re.compile(r"\b(women'?s?|ladies'?|ladys?|juniors?|womens|female|maternity)\b", re.I)
MENS_PAT = re.compile(r"\b(men'?s?|mens|male)\b", re.I)


def detect_gender(style):
    """Classify a style as youth / womens / mens / unisex from its text.
    Unisex products are assigned to BOTH the mens and womens collections."""
    text = " ".join(filter(None, [style.get("title", ""), style.get("styleName", ""),
                                  style.get("baseCategory", ""), style.get("description", "")[:300]]))
    if YOUTH_PAT.search(text):
        return "youth"
    w, m = bool(WOMENS_PAT.search(text)), bool(MENS_PAT.search(text))
    if w and not m:
        return "womens"
    if m and not w:
        return "mens"
    return "unisex"


# --------------------------------------------------------------------------
# Content builders
# --------------------------------------------------------------------------
def clean_text(s):
    return re.sub(r"\s+", " ", html.unescape(s or "")).strip()


def build_title(style):
    brand = style.get("brandName", "").strip()
    name = style.get("styleName", "").strip()
    title = clean_text(re.sub(r"<[^>]+>", "", style.get("title", "")))
    parts = [p for p in [brand, name, title] if p]
    return " ".join(parts)[:255]


def build_description_html(style, gender):
    """Rich product description + features list + customization CTA."""
    raw = style.get("description", "") or ""
    # SS descriptions are newline-delimited feature bullets, sometimes with HTML
    lines = [clean_text(re.sub(r"<[^>]+>", "", ln)) for ln in re.split(r"[\r\n]+", raw)]
    lines = [ln for ln in lines if ln]
    intro = lines[0] if lines else ""
    features = lines[1:]

    gender_label = {"mens": "Men's", "womens": "Women's", "youth": "Youth",
                    "unisex": "Unisex"}[gender]
    parts = [f"<p><strong>{html.escape(build_title(style))}</strong> — "
             f"{gender_label} {html.escape(style.get('baseCategory', 'apparel'))} "
             f"from {html.escape(style.get('brandName', ''))}, a wholesale-quality blank "
             f"ready for professional customization.</p>"]
    if intro:
        parts.append(f"<p>{html.escape(intro)}</p>")
    if features:
        items = "".join(f"<li>{html.escape(f)}</li>" for f in features)
        parts.append(f"<h4>Features</h4><ul>{items}</ul>")
    # Customization CTA — required on every product
    parts.append(CONFIG.get("customization_blurb_html", ""))
    return "".join(parts)


def build_seo(style, gender):
    gender_label = {"mens": "Men's", "womens": "Women's", "youth": "Youth",
                    "unisex": ""}[gender]
    base = build_title(style)
    seo_title = f"{base} | Custom {style.get('baseCategory', 'Apparel')}"[:60]
    desc = (f"{gender_label} {style.get('brandName', '')} {style.get('styleName', '')} "
            f"{style.get('baseCategory', '')}. Customizable with embroidery, DTF, "
            f"sublimation & patches at Summit Standard Co. Request a quote today.")
    return seo_title, clean_text(desc)[:160]


def build_tags(style, gender, category_names):
    tags = {
        style.get("brandName", ""),
        style.get("baseCategory", ""),
        f"Style {style.get('styleName', '')}",
        "Customizable", "Embroidery", "DTF", "Sublimation",
        {"mens": "Men", "womens": "Women", "youth": "Youth", "unisex": "Unisex"}[gender],
    }
    if gender == "unisex":
        tags.update({"Men", "Women"})
    if style.get("sustainableStyle"):
        tags.add("Sustainable")
    if style.get("newStyle"):
        tags.add("New Arrival")
    tags.update(category_names)
    return sorted(t for t in tags if t)


def collections_for(style, gender):
    handles = []
    gc = CONFIG.get("gender_collections", {})
    # Only add gender collections if they are explicitly set (not null/None)
    if gender == "unisex":
        for g in ["mens", "womens"]:
            h = gc.get(g)
            if h:
                handles.append(h)
    else:
        h = gc.get(gender)
        if h:
            handles.append(h)
    # Category-based collections — matched against SS baseCategory
    base = (style.get("baseCategory", "") or "").lower()
    for cat_key, cols in CONFIG.get("category_collection_map", {}).items():
        if cat_key.lower() in base:
            handles += [c for c in cols if c]
    # De-duplicate, resolve to GIDs
    gids = []
    for h in dict.fromkeys(handles):  # preserves order, removes dupes
        gid = resolve_collection(h)
        if gid:
            gids.append(gid)
    return gids


def taxonomy_category(style):
    tmap = CONFIG.get("shopify_taxonomy_map", {})
    base = style.get("baseCategory", "") or ""
    for key, gid in tmap.items():
        if key != "_fallback" and key.lower() in base.lower():
            return gid
    return tmap.get("_fallback")


def image_url(path):
    if not path:
        return None
    return SS_CDN + path.replace("_fm", "_fl")  # large images


# --------------------------------------------------------------------------
# Variant splitting (100-variant rule)
# --------------------------------------------------------------------------
def split_colors(colors, sizes_per_color):
    """Greedily pack colors into groups so each group stays <= 100 variants.

    sizes_per_color: dict color -> number of sizes (sizes can differ per color).
    Returns list of color-name lists. A single group means no split needed.
    """
    groups, current, current_count = [], [], 0
    for color in colors:
        n = sizes_per_color.get(color, 0)
        if n > 100:  # pathological; should never happen with apparel sizing
            LOG.event("WARN", color, "Single color exceeds 100 sizes?! Truncating.")
            n = 100
        if current and current_count + n > 100:
            groups.append(current)
            current, current_count = [], 0
        current.append(color)
        current_count += n
    if current:
        groups.append(current)
    return groups


def group_label(color_group, total_groups, idx):
    if total_groups == 1:
        return ""
    first, last = color_group[0], color_group[-1]
    return f" — {first}" if first == last else f" — {first}–{last}"


# --------------------------------------------------------------------------
# Checkpoint
# --------------------------------------------------------------------------
CHECKPOINT_LOCK = threading.Lock()


def load_checkpoint():
    return load_json(CHECKPOINT_PATH, {
        "cycle_started": None,
        "styles": {},          # styleID -> {hash, products: {splitKey: gid}, last_synced}
        "completed_this_cycle": []
    })


def save_checkpoint(cp):
    with CHECKPOINT_LOCK:
        tmp = CHECKPOINT_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cp, f, indent=2)
        os.replace(tmp, CHECKPOINT_PATH)


def style_hash(products):
    """Hash only the fields whose change should trigger a Shopify write."""
    sig = [(p.get("sku"), p.get("customerPrice") or p.get("piecePrice"),
            p.get("salePrice"), p.get("qty"), p.get("colorFrontImage"),
            p.get("unitWeight"))
           for p in sorted(products, key=lambda x: x.get("sku", ""))]
    return hashlib.sha256(json.dumps(sig, default=str).encode()).hexdigest()


# --------------------------------------------------------------------------
# Shopify mutations
# --------------------------------------------------------------------------
PRODUCT_SET = """
mutation productSet($input: ProductSetInput!, $synchronous: Boolean!) {
  productSet(input: $input, synchronous: $synchronous) {
    product {
      id
      variants(first: 100) { nodes { id sku } }
      media(first: 100) { nodes { id alt } }
    }
    userErrors { field message }
  }
}
"""

VARIANT_APPEND_MEDIA = """
mutation variantMedia($productId: ID!, $variantMedia: [ProductVariantAppendMediaInput!]!) {
  productVariantAppendMedia(productId: $productId, variantMedia: $variantMedia) {
    userErrors { field message }
  }
}
"""


def build_product_set_input(style, products_in_group, label, gender,
                            category_names, existing_gid=None):
    """Assemble the full ProductSetInput for one (possibly split) product."""
    title = build_title(style) + label
    seo_title, seo_desc = build_seo(style, gender)
    if label:
        seo_title = (build_title(style)[:40] + label)[:60]

    # Option values, preserving SS sort order
    colors_seen, sizes_seen = [], []
    for p in sorted(products_in_group, key=lambda x: (x.get("colorName", ""), x.get("sizeOrder", ""))):
        c, s = p.get("colorName", ""), p.get("sizeName", "")
        if c not in colors_seen:
            colors_seen.append(c)
        if s not in sizes_seen:
            sizes_seen.append(s)

    # One image per color (front shot), alt text = color name for variant linking
    files, seen_colors = [], set()
    for p in products_in_group:
        c = p.get("colorName", "")
        url = image_url(p.get("colorFrontImage"))
        if url and c not in seen_colors:
            seen_colors.add(c)
            files.append({"originalSource": url, "alt": c, "contentType": "IMAGE"})

    mult = float(CONFIG.get("markup_multiplier", 1.0))
    variants = []
    for p in products_in_group:
        base_price = p.get("customerPrice") or p.get("piecePrice") or p.get("mapPrice") or 0
        sale = p.get("salePrice")
        price = round(float(base_price) * mult, 2)
        compare_at = None
        if (CONFIG.get("use_sale_price_as_compare_at") and sale
                and float(sale) > 0 and float(sale) < float(base_price)):
            compare_at = price
            price = round(float(sale) * mult, 2)
        variants.append({
            "optionValues": [
                {"optionName": "Color", "name": p.get("colorName", "")},
                {"optionName": "Size", "name": p.get("sizeName", "")},
            ],
            "sku": p.get("sku", ""),
            "barcode": p.get("gtin") or None,
            "price": str(price),
            "compareAtPrice": str(compare_at) if compare_at else None,
            "inventoryItem": {
                "tracked": True,
                "measurement": {"weight": {"unit": "POUNDS",
                                           "value": float(p.get("unitWeight") or 0)}},
            },
            "inventoryQuantities": [{
                "locationId": LOCATION_ID,
                "name": "available",
                "quantity": int(p.get("qty") or 0),
            }],
        })

    inp = {
        "title": title,
        "descriptionHtml": build_description_html(style, gender),
        "vendor": style.get("brandName", ""),
        "productType": style.get("baseCategory", ""),
        "tags": build_tags(style, gender, category_names),
        "category": taxonomy_category(style),
        "collections": collections_for(style, gender),
        "seo": {"title": seo_title, "description": seo_desc},
        "productOptions": [
            {"name": "Color", "values": [{"name": c} for c in colors_seen]},
            {"name": "Size", "values": [{"name": s} for s in sizes_seen]},
        ],
        "files": files,
        "variants": variants,
    }
    if existing_gid:
        inp["id"] = existing_gid          # UPDATE existing product
    else:
        inp["status"] = CONFIG.get("default_status", "DRAFT")  # new -> draft
    return inp


def attach_variant_images(product_node, products_in_group):
    """Link each variant to the media whose alt text matches its color."""
    media_by_color = {m["alt"]: m["id"] for m in product_node["media"]["nodes"] if m.get("alt")}
    variant_by_sku = {v["sku"]: v["id"] for v in product_node["variants"]["nodes"]}
    payload = []
    for p in products_in_group:
        vid = variant_by_sku.get(p.get("sku", ""))
        mid = media_by_color.get(p.get("colorName", ""))
        if vid and mid:
            payload.append({"variantId": vid, "mediaIds": [mid]})
    # API caps batch sizes; chunk conservatively
    for i in range(0, len(payload), 50):
        d = SHOPIFY.gql(VARIANT_APPEND_MEDIA,
                        {"productId": product_node["id"],
                         "variantMedia": payload[i:i + 50]})
        errs = d["productVariantAppendMedia"]["userErrors"]
        # "already attached" errors are benign on updates
        real = [e for e in errs if "already" not in e["message"].lower()]
        if real:
            LOG.event("WARN", product_node["id"], f"variant media errors: {real}")


# --------------------------------------------------------------------------
# Per-style sync
# --------------------------------------------------------------------------
def sync_style(ss, style, checkpoint, category_lookup):
    style_id = str(style["styleID"])
    products = ss.products_for_style(style_id)
    if not products:
        LOG.event("WARN", style_id, f"No products returned for {build_title(style)} — skipping")
        return "skipped"

    if CONFIG.get("skip_closeouts"):
        products = [p for p in products
                    if not all(w.get("closeout") for w in p.get("warehouses", []))]
    if CONFIG.get("exclude_noe_retailing"):
        products = [p for p in products if not p.get("noeRetailing")]
    if not products:
        return "skipped"

    new_hash = style_hash(products)
    entry = checkpoint["styles"].get(style_id, {})
    if entry.get("hash") == new_hash and entry.get("products"):
        LOG.bump("skipped_no_change")
        return "no_change"

    gender = detect_gender(style)
    cat_ids = [c.strip() for c in (style.get("categories") or "").split(",") if c.strip()]
    category_names = {category_lookup.get(c, "") for c in cat_ids} - {""}

    # ---- split planning ----------------------------------------------------
    colors, sizes_per_color = [], {}
    for p in products:
        c = p.get("colorName", "")
        if c not in sizes_per_color:
            colors.append(c)
            sizes_per_color[c] = 0
        sizes_per_color[c] += 1
    groups = split_colors(colors, sizes_per_color)

    existing = entry.get("products", {})  # splitKey -> gid

    # ---- ADOPTION: match products created by the OLD sync ------------------
    # If the checkpoint has no record of this style but the store already has
    # products containing these SKUs (from the previous sync), adopt them so
    # we UPDATE in place — same product ID, same handle/URL — instead of
    # creating duplicates. Each color group adopts the existing product that
    # holds the most of its SKUs; an old product can only be adopted by ONE
    # group (the old sync may not have split the same way), so contested
    # groups beyond the best match are created fresh and logged.
    if not existing and SKU_MAP:
        claimed = set()
        candidates = []  # (overlap_count, idx, gid)
        for idx, color_group in enumerate(groups):
            gp = [p for p in products if p.get("colorName") in set(color_group)]
            counts = {}
            for p in gp:
                gid = SKU_MAP.get(p.get("sku", ""))
                if gid:
                    counts[gid] = counts.get(gid, 0) + 1
            for gid, n in counts.items():
                candidates.append((n, idx, gid))
        adoption = {}
        for n, idx, gid in sorted(candidates, reverse=True):
            key = f"part_{idx}"
            if key not in adoption and gid not in claimed:
                adoption[key] = gid
                claimed.add(gid)
        if adoption:
            existing = adoption
            LOG.event("INFO", style_id,
                      f"Adopted {len(adoption)} existing product(s) by SKU match "
                      f"for {build_title(style)} — updating in place (URLs preserved)")

    new_products_map = {}

    for idx, color_group in enumerate(groups):
        if out_of_time():
            raise TimeoutError("time budget reached mid-style")
        group_products = [p for p in products if p.get("colorName") in set(color_group)]
        label = group_label(color_group, len(groups), idx)
        split_key = f"part_{idx}"
        existing_gid = existing.get(split_key)

        inp = build_product_set_input(style, group_products, label, gender,
                                      category_names, existing_gid)
        d = SHOPIFY.gql(PRODUCT_SET, {"input": inp, "synchronous": True})
        result = d["productSet"]
        if result["userErrors"]:
            raise RuntimeError(f"productSet errors: {result['userErrors']}")
        node = result["product"]
        new_products_map[split_key] = node["id"]
        attach_variant_images(node, group_products)
        LOG.bump("products_updated" if existing_gid else "products_created")
        LOG.bump("variants_written", len(group_products))

    checkpoint["styles"][style_id] = {
        "hash": new_hash,
        "products": new_products_map,
        "title": build_title(style),
        "gender": gender,
        "last_synced": datetime.now(timezone.utc).isoformat(),
    }
    return "synced"


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def main():
    global SHOPIFY, LOCATION_ID, SKU_MAP
    SHOPIFY = ShopifyClient()
    LOCATION_ID = SHOPIFY.primary_location_id()
    ss = SSClient()

    checkpoint = load_checkpoint()
    if not checkpoint.get("cycle_started"):
        checkpoint["cycle_started"] = datetime.now(timezone.utc).isoformat()
        checkpoint["completed_this_cycle"] = []

    # Build the adoption map whenever any style still lacks a checkpoint
    # record (first run, or recovering after the old sync). Cheap to skip
    # once every style is tracked.
    if CONFIG.get("adopt_existing", True):
        LOG.event("INFO", "-", "Building SKU map of existing Shopify products (adoption pass)…")
        SKU_MAP = SHOPIFY.build_sku_map()
        LOG.event("INFO", "-", f"Found {len(SKU_MAP)} existing variant SKUs in store")

    LOG.event("INFO", "-", f"Fetching SS style catalog + categories…")
    category_lookup = {str(c["categoryID"]): c["name"] for c in ss.categories()}
    wanted_brands = {b.lower() for b in CONFIG.get("brands", [])}
    styles = [s for s in ss.all_styles()
              if (s.get("brandName") or "").lower() in wanted_brands]
    LOG.event("INFO", "-", f"{len(styles)} styles across {len(wanted_brands)} brands")

    done = set(checkpoint.get("completed_this_cycle", []))
    todo = [s for s in styles if str(s["styleID"]) not in done]
    LOG.event("INFO", "-", f"{len(todo)} styles remaining in this cycle")

    max_workers = int(CONFIG.get("max_workers", 4))
    stop = threading.Event()

    def worker(style):
        sid = str(style["styleID"])
        if stop.is_set():
            return sid, "deferred"
        try:
            status = sync_style(ss, style, checkpoint, category_lookup)
            LOG.bump("styles_processed")
            return sid, status
        except TimeoutError:
            stop.set()
            return sid, "deferred"
        except Exception as e:
            LOG.bump("errors")
            LOG.event("ERROR", sid, f"{build_title(style)}: {e}",
                      http_status=getattr(getattr(e, "response", None), "status_code", None))
            return sid, "error"

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(worker, s): s for s in todo}
        for fut in as_completed(futures):
            sid, status = fut.result()
            if status in ("synced", "no_change", "skipped"):
                checkpoint["completed_this_cycle"].append(sid)
            # Persist progress constantly — a crash/timeout loses nothing
            save_checkpoint(checkpoint)
            if out_of_time():
                stop.set()

    remaining = len(styles) - len(set(checkpoint["completed_this_cycle"]))
    LOG.counts["styles_remaining"] = max(remaining, 0)
    if remaining <= 0:
        LOG.event("INFO", "-", "Full cycle complete — resetting for next pass")
        checkpoint["cycle_started"] = None
        checkpoint["completed_this_cycle"] = []
    save_checkpoint(checkpoint)
    log_path = LOG.save()
    LOG.event("INFO", "-", f"Run log written to {log_path}")
    # Non-zero exit only on hard failure; deferred work is normal
    sys.exit(0)


if __name__ == "__main__":
    main()
