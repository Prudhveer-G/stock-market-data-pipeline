## Stock Market Data Pipeline

---

### Overview

This project implements a Python-based ETL pipeline to ingest daily stock market data into a MySQL database for structured analysis. The focus is on building a simple, repeatable, and maintainable data workflow.

---

### Dataset

- Daily stock price data for multiple tickers
- Data used for structured ingestion and analysis
- Not intended for real-time or trading use

---

### Technical Architecture & Data Flow

The pipeline follows a staged ETL process:

1. Extract daily stock data using Python  
2. Clean and validate records  
3. Transform data into relational format  
4. Load data into MySQL tables  
5. Query data using SQL for analysis  

Each stage operates independently to allow re-runs without manual cleanup.

---

### Tech Stack

- Python  
- Pandas  
- MySQL  
- SQL  

---

### Current Status

- ETL structure defined  
- Schema designed  
- Core pipeline logic under development  

---

### Notes

This project emphasizes correctness, repeatability, and clarity over performance or automation complexity.
