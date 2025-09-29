# 🩺 The Jaffle Clinic

Welcome! This repo is a lightweight data modeling exercise designed to assess your approach to:
- Deduplication and data cleaning  
- Building modular, reusable, and maintainable dbt models  
- Designing and documenting data transformations  
- Writing meaningful tests  

You'll be working with sample seed data representing healthcare practitioner licenses across multiple states.

---

## 🧾 The Task

Using the four seed files provided:
- `california.csv`  
- `massachusetts.csv`  
- `washington.csv`  
- `misc.csv`  

...your goal is to build two final tables:
- `licenses`  
- `practitioners`  

The expected schema for these two models is defined in `models/marts/schema.yml`. You **must** include all fields listed in the schema, but you are welcome (and encouraged) to add additional fields/tables if you feel that it would provide value.

You're also encouraged to:
- Refactor any existing models and schemas within the project as long as the final tables have **at least** the fields defined in `models/marts/schema.yml`
- Create any number of intermediate models, tests, and/or documentation.
- Use packages wherever you feel it's necessary.

This is your opportunity to show your thought process and engineering style.

---

## ✅ Deliverables
By the end of the exercise, you should have:
1. A dbt project that builds final `licenses` and `practitioners` models as described in `models/marts/schema.yml`  
2. Any supporting models or tests you deemed necessary 
3. A short write-up (in this README or as a separate file) answering the following:
   1. Describe your overall approach. What assumptions did you make and talk a bit about any new tests, fields, or models you introduced and why  
   2. How many Registered Nurses are there?
   3. What's the breakdown of license counts by state?
   4. How many licenses will have expired after `2025-12-31`?

---

## 📦 Project Structure

```
jaffle-clinic/
├── analyses/
├── data-tests/
├── macros/
├── models/
│   ├── marts/
│   │   ├── licenses.sql
│   │   ├── practitioners.sql
│   │   └── schema.yml                 # Desired schema for tables in models/marts/
│   └── staging/
│       ├── source.yml                 # Contains some context for tables/fields in models/staging/
│       ├── stg_california.sql
│       ├── stg_massachusetts.sql
│       ├── stg_misc.sql
│       └── stg_washington.sql
├── seeds/                             # All sample data stored here as seeds
│   └── jaffle-data/
│       ├── california.csv
│       ├── massachusetts.csv
│       ├── misc.csv
│       └── washington.csv
├── .gitignore
├── dbt_project.yml
└── README.md
```