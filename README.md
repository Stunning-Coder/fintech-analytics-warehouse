# 🏦 Fintech Analytics Warehouse

A personal project demonstrating an end-to-end data analytics stack built with 
**Python**, **PostgreSQL**, and **dbt** to model and automate key fintech KPIs.

---

## 🚀 Project Overview

This project simulates a modern data stack for a fintech company.  
The goal is to build a reliable and automated **single source of truth** for business 
intelligence and analytics.

It demonstrates the flow of data from raw sources to a structured analytics warehouse, 
where key metrics are pre-calculated and ready for visualization tools (like Metabase, 
Looker, or Tableau).

### 🔄 Data Flow

[Python Ingestion Scripts] > [PostgreSQL Raw Tables] > [dbt Staging Models] > [dbt Marts / KPI Models] > [BI Dashboard or SQL Queries]

---


---

## 🛠️ Tech Stack

| Layer | Tool | Description |
|-------|------|--------------|
| **Orchestration & Ingestion** | Python (`pandas`, `sqlalchemy`, `psycopg2`) | Loads and simulates raw fintech data |
| **Data Warehouse** | PostgreSQL | Stores raw and transformed models |
| **Transformation** | dbt (Data Build Tool) | Cleans, joins, and models analytics-ready tables |

---

## ⚙️ Setup & Usage

### 1. Clone the repository
```bash
git clone https://github.com/yourusername/fintech-analytics-warehouse.git
cd fintech-analytics-warehouse
```
### 2. Create and activate a virtual environment
```bash
python -m venv venv
source venv/bin/activate
```
### 3. Install dependencies
```bash
pip install - r requirements.txt
```
### 4. Configure environment variables
```bash
DATABASE_URL=postgresql+psycopg2://YourUser:YourPassWord@YourHostName:5432/fintech_dw
```
### 5. Run data ingestion
```bash 
python ingestion_scripts/load_raw_data.py
```
### 6. Run dbt transformations
```bash
cd my_dbt_project
dbt run
```
### Generate dbt docs
dbt docs generate && dbt docs serve

## ✨ Key Features & Components

### 1. Data Models (dbt)

The dbt project transforms raw data into a series of modular, easy-to-query models.

- **Staging Models:** Clean and lightly prepare raw data (e.g., cast types, rename 
columns).  
- **Intermediate Models:** Perform common joins and aggregations used by multiple 
downstream models.  
- **Mart Models:** Final, business-facing models that power KPIs and dashboards.  

---

### 2. Automated KPIs

The warehouse automatically computes critical fintech metrics, such as:

| **KPI** | **Description** |
|----------|-----------------|
| **User Retention** | Cohort-based retention tracking |
| **AUM (Assets Under Management)** | Daily/weekly snapshot of assets |
| **Transaction Velocity** | Frequency and volume of user transactions |

## Project Structure
```bash
fintech_analytics_warehouse/
├── ingestion_scripts/
│   └── load_raw_data.py        # Python script for data ingestion
├── my_dbt_project/
│   ├── models/
│   │   ├── staging/            # Staging models (cleaning)
│   │   │   ├── stg_users.sql
│   │   │   └── stg_transactions.sql
│   │   ├── marts/              # Final analytics models (KPIs)
│   │   │   ├── dim_users.sql
│   │   │   ├── fct_transactions.sql
│   │   │   └── kpi_retention.sql
│   │   └── sources.yml         # Raw sources definition
│   ├── dbt_project.yml         # dbt project configuration
│   └── profiles.yml            # dbt connection profiles (local)
└── README.md
```
## 🧭 Future Enhancements

- Add CI/CD automation using **GitHub Actions** for dbt runs  
- Integrate **Metabase** or **Apache Superset** for visual analytics  
- Add synthetic **API ingestion** for transactions and market data  
- Implement **data quality tests** using `dbt-utils`  
- Containerize the stack using **Docker Compose** (Postgres + dbt + Metabase)  

---

## 🧑‍💻 Author

**James Essiet**  
Data & Analytics Professional | Database Developer  

📧 [Email Me](mailto:jamesessiet1@gmail.com)  
🌐 [LinkedIn](https://linkedin.com/in/james-essiet) • 
[GitHub](https://github.com/Stunning-Coder)

---

## 📜 License

**ANM License © 2025 James Essiet**

---

## 🙌 Acknowledgements

Inspired by **dbt Labs’** best practices and the **modern data stack community**.  
Built for **learning, experimentation, and portfolio demonstration**.
