# Databricks Auto Loader — Free Edition project

A fully documented beginner project for **CSV → Databricks Auto Loader → Bronze Delta table**, with **three real CSV example files** and Databricks-ready notebooks.

## What you'll prove

1. Load the first CSV: **3** rows.
2. Add a second CSV and restart with the **same checkpoint**: **6** rows total.
3. Restart without any new files: still **6** rows (no file-level reprocessing).
4. Add the third CSV: **9** rows, **3** distinct files, **$2,435** total amount.

## Project layout

```text
databricks-autoloader-free-edition/
├── README.md
├── .gitignore
├── data/
│   ├── orders_1.csv
│   ├── orders_2.csv
│   └── orders_3.csv
├── notebooks/
│   ├── autoloader_complete_demo.ipynb   # preferred import format
│   └── autoloader_complete_demo.py      # Databricks source notebook
└── docs/
    └── WALKTHROUGH.md
```

## Prerequisites

- Databricks Free Edition workspace with **serverless** notebook compute.
- Permission to create a Unity Catalog schema and managed Volume in your catalog.
- No S3 bucket, cloud subscription, API keys, or Python package install needed.
- GitHub account only if you want to publish the repository.

> **Catalog name:** Example code uses `workspace`. Run `SHOW CATALOGS` in a Databricks SQL cell if yours differs and edit `catalog_name` in notebook cell 1. You might also use an existing schema and Volume if creation is disallowed.

## Fastest way to execute

1. Download this repository ZIP, extract it, and in Databricks choose **Workspace → Import** (or create a new notebook and import the `.ipynb`). The UI may vary by workspace.
2. Open `notebooks/autoloader_complete_demo.ipynb`, select **Serverless** compute.
3. Review `catalog_name` in the first Python cell; change it if necessary.
4. Run each cell **one at a time in sequence**, starting at the beginning.
5. Watch the table go from 3 → 6 → 6 → 9 rows.
6. The last cell contains a deliberately **commented-out reset**. Uncomment it only when you want to delete the demo's table, checkpoint, and staged input files.

The notebook embeds the same tiny demo CSV contents as `data/*.csv`, allowing the staging cells to work regardless of the workspace's working directory. **The CSV files are genuine separate files in the repository**, useful for uploads, review, and practicing Git. Production projects should read actual incoming data from a storage source rather than embed fixtures.

## Expected results

| Source | Count | Sum(amount) |
| --- | ---: | ---: |
| orders_1.csv | 3 | 1300 |
| orders_2.csv | 3 | 540 |
| orders_3.csv | 3 | 595 |
| **Total** | **9** | **2435** |

## GitHub: publish the full process

### Option A — using GitHub's website (easiest)

1. Visit GitHub, create a **new public or private repository** called `databricks-autoloader-free-edition` **without** automatically creating a README.
2. On the repo page, click **Add file → Upload files**. Drag the **contents of the extracted project folder**, preserving `notebooks/`, `data/`, and `docs/`.
3. Commit with message `Add Databricks Auto Loader incremental ingestion demo`.
4. Add screenshots of the 3 → 6 → 6 → 9 outputs, and notes on your own run, if you want a portfolio walkthrough. Do **not** upload access tokens, customer data, or checkpoints.

### Option B — local Git command line

```bash
cd databricks-autoloader-free-edition
git init
git add .
git commit -m "Add Databricks Auto Loader incremental ingestion demo"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/databricks-autoloader-free-edition.git
git push -u origin main
```

Create the empty remote GitHub repository before the push. GitHub authentication might prompt you to sign in or use an authorized credential manager.

### Option C — Databricks Git folders

Once the repo exists on GitHub, you can link a Databricks **Git folder** to its URL if Git integration is available in your workspace. Databricks formerly called this feature **Repos**. Open the notebook from that folder and commit/push future changes through the Databricks Git interface. If Git folders aren't available in your Free Edition workspace, **Option A or B** still works.

## Important behavior

- `cloudFiles` uses **directory listing** in this example. This avoids cloud notification infrastructure.
- `availableNow=True` is suited to a finite test on serverless compute; the query processes available files and ends.
- Keep the **same checkpoint path** across incremental runs; do not delete it between batches.
- Auto Loader's file-level tracking doesn't remove business-level duplicates if separate CSVs happen to contain the same order ID.
- This project doesn't commit runtime-generated Unity Catalog Volume data, Delta tables, or checkpoints. Those are Databricks resources, **not Git artifacts**.
- The notebook is an instructional lab, not an idempotent production workflow; reset it before repeating all cells from scratch.

## Troubleshooting

| Problem | What to check |
| --- | --- |
| `CATALOG_NOT_FOUND` | Run `SHOW CATALOGS`; edit `catalog_name`. |
| Cannot create Volume | Permissions: use an existing writable managed Volume, adjusting setup paths. |
| Source file already exists | Do not overwrite it; continue from the next cell or deliberately reset. |
| Table has unexpected rows | A prior run may have reused table or checkpoint. Inspect both; reset only demo assets. |
| Infinite streaming trigger unsupported | Ensure code uses `.trigger(availableNow=True)` and serverless. |
| `cloudFiles` unsupported | Verify that the selected compute supports Databricks Auto Loader. |

## Official Databricks references

- [Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader)
- [Auto Loader and Unity Catalog](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/unity-catalog)
- [Streaming on serverless](https://docs.databricks.com/aws/en/compute/serverless/streaming)
- [Databricks Git folders](https://docs.databricks.com/aws/en/repos)
- [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)

## Suggested repository title for your portfolio

**Databricks Auto Loader — Incremental CSV Ingestion with Unity Catalog, Delta Lake & Checkpointing (Free Edition)**
