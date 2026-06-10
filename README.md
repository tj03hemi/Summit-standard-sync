# SS Activewear → Shopify Sync — Summit Standard Co.

Automated daily sync that pulls every style for your 68 selected brands from the S&S Activewear API and creates/updates fully-built Shopify products.

## What gets populated on every product

| Field | Source / Logic |
|---|---|
| Title | `{Brand} {StyleName} {Title}` (split products get `— FirstColor–LastColor` suffix) |
| Description | SS intro + Features list + **customization CTA linking to /pages/custom-orders** on every product |
| Category | Shopify standard taxonomy (drives tax rates + search/filter metafields), mapped from SS `baseCategory` in `sync_config.json` → `shopify_taxonomy_map` |
| Price | SS `customerPrice` (fallback `piecePrice`) × `markup_multiplier`; active `salePrice` becomes the price with the regular price as compare-at |
| Inventory | Combined SS warehouse qty, written to your Shopify location |
| Weight | SS `unitWeight` (lbs), per variant |
| Variants | Color × Size, with SKU + GTIN barcode; auto-split when >100 variants |
| Variant images | SS front image per color, attached to every variant of that color |
| Status | New products created as **DRAFT** — you review and publish |
| Collections | Gender collection (mens/womens/youth; **unisex → both mens and womens**) + category collections from `category_collection_map` |
| Tags | Brand, category, style #, gender, Customizable, Embroidery, DTF, Sublimation, Sustainable/New Arrival flags |
| Product type / Vendor | SS `baseCategory` / brand name |
| SEO | Title ≤60 chars, meta description ≤160 chars mentioning customization + quote request |

## Existing products & SEO — do NOT delete your old sync's products

The script runs an **adoption pass** before syncing: it maps every variant SKU already in your store to its product, and when a style isn't yet in the checkpoint, it matches by SKU and **updates the existing product in place**. Same product ID, same handle/URL — your 1,200+ Google-indexed pages stay live and simply get richer content (better titles, descriptions, taxonomy category, SEO fields), which helps rankings rather than hurting them. Deleting and recreating would 404 every indexed URL and lose accumulated link equity.

Notes:
- Match key is the S&S SKU, which never changes — so adoption works even if the old sync's titles/structure differ.
- If an old product covered more colors than a new split allows, the largest split adopts it and the overflow split is created fresh (logged as such, created in DRAFT).
- Adopted products keep their current published/draft status; only newly created products default to DRAFT.
- Turn off with `"adopt_existing": false` in `sync_config.json` (not recommended).
- The "products silently stopped updating" failure mode is structurally addressed: every style is guaranteed a visit each cycle, every style records `last_synced` in `checkpoint.json`, and each run's log + job summary reports created/updated/skipped/error counts so a stall is visible immediately.

## Setup

1. **GitHub Secrets** (repo → Settings → Secrets → Actions):
   - `SS_USERNAME` — your SS account number
   - `SS_API_KEY` — from ssactivewear.com → My Account
   - `SHOPIFY_ADMIN_TOKEN` — Admin API access token (custom app, scopes: `write_products`, `read_products`, `write_inventory`, `read_locations`)
   - `SHOPIFY_STORE_URL` — `summitstandardco.myshopify.com`

2. **Collections** — create these in Shopify (or edit the handles in `sync_config.json` to match yours):
   `mens`, `womens`, `youth`, `blanks-tees`, `sweatshirts-hoodies`, `hats`, `polos`, `outerwear`, `bags`, `activewear`, `workwear`, `pants-shorts`, `accessories`.
   Missing handles are logged and skipped — nothing breaks.

3. **First run** — go to Actions → "SS Activewear → Shopify Sync" → Run workflow. With 68 brands this is a multi-run job: each run processes as much as it can in ~4.8 hours, commits `checkpoint.json` back, and the next run resumes automatically. Once a full pass completes, daily runs only touch styles whose price/inventory/images actually changed (delta sync).

## Tuning (`sync_config.json`)

- `markup_multiplier` — global price multiplier (default 1.0 = SS cost, you mark up at order level)
- `time_budget_minutes` — when the job checkpoints and exits cleanly (default 290)
- `max_workers` — concurrency (SS's 60 req/min limit is the real ceiling; 4 is a good balance)
- `skip_closeouts` / `exclude_noe_retailing` — optional filters
- `inventory_location_name` — must match your Shopify location name exactly (falls back to first location)

## Verify before first run

- Shopify API version is set to `2025-01` (override with the `SHOPIFY_API_VERSION` env var if your admin shows newer). The `productSet` mutation used requires 2024-10+.
- Spot-check the taxonomy GIDs in `shopify_taxonomy_map` against Shopify's current taxonomy browser (Settings → Custom data, or shopify.github.io/product-taxonomy) — IDs occasionally shift between taxonomy releases.
