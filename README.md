# 📖 Benchmarking MySQL vs Neo4j

## 📌 Description
This project done by Antonio Serra and Lorenzo Pizzi benchmarks **MySQL** and **Neo4j** by executing a set of predefined queries, measuring their performance, and comparing results.  
You can change the queries by changing the two variables QUERIES in Neo4j.py and MySql.py giving the same name to the query to be compared.
It can run queries **with or without indexes**, save the outputs as CSV files, and generate reports and comparative plots.

---

## ⚙️ Project Structure
- **Application.py** → Main entry point. Handles CLI arguments, launches benchmarks, compares results, and triggers plots.  
- **Config.py** → Global configuration (`USE_INDEXES`, `RESULTS_ROOT`).  
- **MySql.py** → Executes benchmark queries on MySQL and writes results to CSV.  
- **Neo4j.py** → Executes benchmark queries on Neo4j and writes results to CSV.  
- **GeneraGrafici.py** → Loads results and generates comparative plots.  
- **indexes_mysql / indexes_neo4j** → Variables containing the SQL and Cypher index definitions to create/drop depending on the run mode.  

---

## 📂 Output Structure
Results are stored in two possible root folders:
- `results/` → execution **without indexes**  
- `results_with_indexes/` → execution **with indexes**  

Each root folder contains:
- `mysql/` → MySQL benchmark CSVs  
- `neo4j/` → Neo4j benchmark CSVs  
- `reports/` → comparison reports (CSV diffs and summary)  
- `plots/` → generated comparative plots  

---

## ▶️ Usage

### 1. Setup Environment
```bash
python -m venv venv
source venv/bin/activate   # Linux / macOS
venv\Scripts\activate      # Windows
pip install -r requirements.txt
```

### 2. Run without indexes:

python Application.py --run

### 3. Run with indexes:

python Application.py --run --use_index

### 4. Compare Existing Results Only without re-running benchmarks

python Application.py

## Plots
After execution, comparative plots will be generated automatically and saved under:

results/plots/                # when running without indexes
results_with_indexes/plots/   # when running with indexes

The plots display the average execution times of MySQL vs Neo4j for each query.
