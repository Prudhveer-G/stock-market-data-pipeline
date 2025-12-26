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

The pipeline follows a simple local ETL workflow:

1. Extract stock data using Python
2. Transform raw data into a structured DataFrame
3. Load records into MySQL using a relational schema
4. Query stored data using SQL for analysis

Each stage is modular to allow independent testing and re-runs.


---

### Tech Stack

- Python  
- Pandas  
- MySQL  
- SQL  

---

### Current Status

- End-to-end ETL pipeline implemented
- Loads daily stock data into MySQL
- Supports repeatable local execution
 

---

### Notes

This project emphasizes correctness, repeatability, and clarity over performance or automation complexity.
