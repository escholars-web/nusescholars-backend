# Developer Guide — Backend (`nusescholars-backend`)

This directory contains two projects:

- **[nusescholars/](nusescholars/)** — the Next.js frontend (public website). See its own guide: [nusescholars/developer_guide.md](nusescholars/developer_guide.md).
- **[nusescholars-backend/](nusescholars-backend/)** — a separate, in-progress FastAPI + Supabase backend intended to eventually replace the frontend's manual Excel-based intake, documented below. **It does not currently feed the live frontend's `database.json`** — the two pipelines maintain separate copies of "the database" today (`nusescholars/src/data/database.json` vs. `nusescholars-backend/data/*.json` / Supabase tables). Don't assume a change in one appears in the other.

## Using the Auto-Cropper (Face-Centered Profile Picture Tool)

This tool detects a face in an uploaded photo and crops it into a consistent 1200×1500 (4:5) portrait, suitable for profile pictures. It runs as a local Gradio web app.

Script: [nusescholars-backend/scripts/convert_img_aspect.py](nusescholars-backend/scripts/convert_img_aspect.py)

### 1. Start the app

```bash
cd nusescholars-backend
source .venv/bin/activate
python scripts/convert_img_aspect.py
```

This launches a local Gradio server, by default at:

```
http://127.0.0.1:7860
```

Open that URL in your browser.

### 2. Upload an image

In the **Upload Image** box (left panel), upload the photo you want to crop.

### 3. Detect & Resize

Click **Detect & Resize**. The app will:
- Detect the largest face in the image using OpenCV's Haar cascade face detector.
- Compute a crop centered on that face, sized so the face occupies roughly 40% of the crop height.
- Resize the crop to exactly 1200×1500 pixels.
- Show the result in the **Processed Preview** box (right panel).

If no face is detected, it falls back to a centered crop of the full image height.

### 4. Adjust the crop (optional)

Use the sliders under **Post-processing adjustments** to fine-tune the result:
- **Horizontal shift (%)**: moves the crop left/right relative to the detected face.
- **Vertical shift (%)**: moves the crop up/down relative to the detected face.
- **Crop scale (relative to auto)**: zooms in (lower value) or out (higher value) from the auto-detected crop.

Click **Apply adjustments** to preview the updated crop with your slider settings.

Click **Reset adjustments** at any time to return to the original auto-detected crop (sliders reset to default).

### 5. Download the result

Use the **Download Processed Image** file box to save the final cropped/resized PNG to your machine.

### Notes

- Target output size is fixed at 1200×1500 (aspect ratio 4:5) — this is defined by `TARGET_W` / `TARGET_H` at the top of the script.
- This is a manual, interactive tool. For batch/offline processing of many profile pictures at once (e.g. pulled from Supabase `picture_url` links), see:
  - [nusescholars-backend/app/services/link_to_crop_img.py](nusescholars-backend/app/services/link_to_crop_img.py) — crops all images in a local directory.
  - [nusescholars-backend/app/services/download_crop_img.py](nusescholars-backend/app/services/download_crop_img.py) — downloads profile pictures from Supabase staging data, then auto-crops them.

---

## Backend (`nusescholars-backend`) — other scripts

### General run pattern

From the backend README, the canonical setup is:

```bash
cd nusescholars-backend
source .venv/bin/activate
uvicorn app.main:app --reload          # start the API
PYTHONPATH=. python app/services/json_to_csv.py   # run an app/services script
```

Any script under `app/services/` imports from `app.*` (e.g. `app.database.supabase_client`), so it must be run with `PYTHONPATH=.` from the repo root. Scripts under `scripts/` do not import from `app.*` and are run directly with `python scripts/foo.py` (or as a module, noted below where relevant).

A `.env` file at the backend repo root must supply `SUPABASE_URL` and `SUPABASE_KEY` for any script that touches Supabase.

### `app/services/profile_service.py` and `app/services/json_to_csv.py`

Already covered by the auto-cropping review — see the top of this document; both just pass through `picture_url`, no cropping logic.

### `app/services/link_to_crop_img.py` / `app/services/download_crop_img.py`

Documented above (batch face-cropping pipeline).

### `app/services/check_exist.py`

Reconciliation script comparing Supabase's `profiles` table against the `staging` table by `full_name`, printing which staging names are new vs. already onboarded.

```bash
cd nusescholars-backend
PYTHONPATH=. python app/services/check_exist.py
```

Requires `.env` with Supabase credentials; `profiles` and `staging` tables must exist with a `full_name` column.

### Data-cleaning pipeline (`scripts/`)

These operate on JSON files under `data/` and are typically run in sequence when onboarding a new census/cohort. Run each from the `nusescholars-backend` repo root.

1. **`scripts/generate_new_json.py`** — Converts the raw Excel census export (`data/D&E-Scholars Census AY25_26(1-103).xlsx`) into `data/database_new.json`. Deduplicates by email, slugifies names, derives admit-year keys, splits multi-line free text into lists, and derives bachelor's/masters/faculty fields. Edit the `LAST_UPDATED`, `DEDUP_KEY`, and `REMOVE_BLANK_WRITEUPS` constants at the top before running for a new cycle.
   ```bash
   python scripts/generate_new_json.py
   ```
   Requires `pandas` + `openpyxl` (already in `requirements.txt`) and the exact Excel filename to exist at `data/`.

2. **`scripts/title_case.py`** — Title-cases every string under a `"name"` key in `data/database_new.json`, writing `data/database_new_cleaned.json`.
   ```bash
   python scripts/title_case.py
   ```

3. **`scripts/newline_delimited.py`** — Post-processes `data/database_new_cleaned.json` in place, converting `notable_achievements`/`interests_hobbies` list fields into newline-delimited strings.
   ```bash
   python scripts/newline_delimited.py
   ```

4. **`scripts/merge_json.py`** — Merges the legacy `data/database.json` with the new `data/database_new_cleaned.json` into `data/database_merged.json`, migrating schema (`major` → `bachelors`, adds `masters`), matching masters records back to students' original bucket, and merging overlapping fields (only overwriting with non-null values, refreshing `last_updated`). Prints a report of unmatched masters records and skipped null write-ups.
   ```bash
   python scripts/merge_json.py
   ```
   Requires both `data/database.json` and `data/database_new_cleaned.json` to exist.

5. **`scripts/sort_majors.py`** — Reads `data/database_merged.json` and reorders each admission-year batch's majors by a fixed priority list (MPE, EEE, BME, ESP, EVE, ISE, MLE, CEG, CHE, CVE, IPM, DS, masters; others alphabetical after), writing `data/database_sorted.json`.
   ```bash
   python scripts/sort_majors.py
   ```

6. **`scripts/clean_admit_year.py`** — Normalizes the `admit_year` field in `data/database.json` to strict `AY##/##` format, **overwriting the file in place**. Back up first if unsure.
   ```bash
   python scripts/clean_admit_year.py
   ```

7. **`scripts/add_last_updated.py`** — Stamps every record in `data/database.json` with a `last_updated` date, overwriting in place. Takes the date as a required CLI argument (`YYYY-MM-DD` or ISO datetime).
   ```bash
   python -m scripts.add_last_updated "2024-09-30"
   ```

---

## Top-level loose scripts (`escholars/` repo root)

### `link_to_crop_img.py`

A standalone (non-backend) version of the face-cropping utility. Scans every `.jpg/.jpeg/.png` in its own directory, detects the largest face via OpenCV Haar cascade, crops a 4:3 portrait centered on the face (3x face-height padding), and saves to a `cropped/` subfolder.

```bash
python link_to_crop_img.py
```

Run from the directory containing the source images (or place images alongside the script); `INPUT_DIR`/`OUTPUT_DIR`/`CROP_SCALE` are hardcoded at the top of the file. Requires `opencv-python` and `Pillow`.

### `img_to_supabase.py`

Empty file (0 bytes) — a placeholder/stub, not currently implemented.

## Known issues / cleanup candidates

- `requirements.txt` is missing dependencies actually imported by its image scripts (`opencv-python`/`cv2`, `Pillow`, `gradio`, `requests`).
- `app/api/student/` exists but is empty; `app/api/admin/endpoints.py` (a generic demo `/items/{id}` router) is defined but never registered in `main.py` — dead scaffolding from project init.
- `app/tests/` exists but contains no test files.
- `data/cookies.txt` holds session-auth cookies used by `download_crop_img.py` — verify it's gitignored before committing anything in that folder.

## Frontend

See [nusescholars/developer_guide.md](nusescholars/developer_guide.md) for the frontend's data pipeline, app structure, and CI/CD documentation.
