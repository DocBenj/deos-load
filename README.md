# deos-load

A Python-based ETL utility to validate, upload, and manage CSV data for ClickHouse Cloud.

## 🚀 Features
- **Validation:** Checks DEOS datasets for schema consistency.
- **Efficient Upload:** Uses `clickhouse-connect` to stream data directly to ClickHouse.
- **Security:** Fully integrated with `.env` for credential management.
- **Cleanup:** Automated scripts to remove local CSVs after successful upload.

---

## 🛠️ Setup Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/DocBenj/deos-load.git