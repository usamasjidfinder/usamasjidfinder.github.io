# USA Masjid Finder

Masajid across all 50 states and DC: Jumu'ah times, prayer timings, iqamah and events, in one searchable page.
Built from the same pipeline as [MN Masjid Finder](https://mnmasjidfinder.github.io).

## Use

```bash
python3 fetch.py                      # refresh data/masajid.json (first run 15-30 min, later runs a few minutes)
python3 -m http.server 8765           # then open http://localhost:8765
```

Visitors can pick a state (or link straight to one, e.g. `/#TX`), tap **Near me**, search by name, city or
time ("1:30 pm" finds Jumu'ah within 15 minutes), and sort by earliest/latest Jumu'ah.

## Where the data comes from

| Source | Gives | How it's found |
|---|---|---|
| OpenStreetMap (Overpass) | mosque list, address, coordinates, website, state | every run; falls back to `data/osm_cache.json` |
| Masjidal directory | every US masjid registered on Masjidal | every run; cached in `data/masjidal_cache.json` |
| Masjidal / Athan+ API | today's adhan, iqamah, Jumu'ah; the browser pulls 7 days live for cards on screen | directory ID, or the widget on the masjid's site |
| Mohid (us.mohid.co) | Jumu'ah khutbah/iqamah, daily iqamah, events | link found on the masjid's site, or `mohid_url` in seed |
| WordPress Events Calendar | upcoming events | auto-detected, or `events_wp` in seed |
| Calculation (ISNA 15°/15°) | adhan only, computed in the browser in the masjid's time zone | fallback for everything else |
| OSM Nominatim | city names for listings without one | cached in `data/geocode_cache.json` |

Website scans are cached for 1-2 weeks per site in `data/discover_cache.json`. `data/errors.json` lists feeds that failed on the last run.

## Adding or fixing a masjid

Edit `mosques_seed.json` (the Minnesota fixes from MN Masjid Finder are already in it). Match an entry by `osm_id`,
`masjidal_id`, or exact `name` + `state`. To add a masjid missing from both sources, give `name`, `state` and `address`:

```json
{"name": "Masjid Example", "state": "TX", "address": "123 Main St, Austin, TX 78701",
 "jumuah": [{"khutbah": "1:30 PM"}], "jumuah_note": "confirmed by phone 2026-11"}
```

To hide a Masjidal directory entry, add its ID to `exclude_masjidal`.

## Publishing (GitHub Pages, refreshes daily)

`.github/workflows/refresh.yml` runs `fetch.py` every morning (and on each push or a manual "Run workflow"),
commits the refreshed `data/` back, and deploys `index.html` + data to GitHub Pages.

One-time setup: in the repo's **Settings → Pages**, set **Source** to **GitHub Actions**.
