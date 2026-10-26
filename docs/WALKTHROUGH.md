# Demo walkthrough / experiment log

## 1. Setup

- Databricks workspace: Free Edition
- Compute: Serverless
- Catalog: `workspace` (or your selected catalog)
- Schema: `autoloader_demo`
- Managed Volume: `autoloader_files`
- Source: `/Volumes/<catalog>/autoloader_demo/autoloader_files/input/`
- Target: `<catalog>.autoloader_demo.orders_bronze`

## 2. Initial ingestion

- Staged `orders_1.csv`, ran Auto Loader with `availableNow`.
- Expected records: 3.
- Screenshot to add: input Volume and 3-row Delta table.

## 3. Incremental ingestion

- Staged `orders_2.csv` only; reused checkpoint.
- Expected total records: 6.
- Screenshot to add: 6-row result with source_file.

## 4. Restart test

- Added no new file; reran Auto Loader with same checkpoint.
- Expected total records: 6.
- Screenshot to add: PASS message.

## 5. Third file and validation

- Staged `orders_3.csv`; reused checkpoint.
- Expected total records: 9; distinct orders: 9; source files: 3; sum(amount): 2435.
- Screenshot to add: validation outputs and checkpoint directory.

## Notes

Auto Loader remembers file processing state, not business primary keys. Keep source data,
checkpoint state, and the destination table outside Git. Don't commit sensitive output.
