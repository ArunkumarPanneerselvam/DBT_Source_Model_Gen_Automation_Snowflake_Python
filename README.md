# DBT_Source_Model_Gen_Automation_Snowflake_Python
Automating Your Local DBT &amp; Snowflake Playground with Python

# Snowflake DBT Playground with Automated Source Generation

![Python](https://img.shields.io/badge/python-3.13-blue)
![dbt](https://img.shields.io/badge/dbt-1.0-orange)
![Snowflake](https://img.shields.io/badge/snowflake-cloud%20data%20warehouse-lightblue)
![License](https://img.shields.io/badge/license-MIT-green)

---

## Overview

This project automates the creation of dbt source YAML and SQL model files for Snowflake databases by extracting metadata via the Snowflake API — freeing users from manual file creation.

Build your Snowflake playground faster, with scripts:  
- Pull metadata from Snowflake  
- Generate correct dbt source files  
- Prepare ready-to-use dbt models

---

## Features

- Connects securely to Snowflake using environment variables  
- Supports multiple schemas via configuration  
- Creates directory structure matching dbt conventions  
- Generates YAML & SQL files per table/view  
- Compatible with dbt Core and Snowflake adapter  

---

## Getting Started

### Prerequisites

- Python 3.13+  
- Snowflake account (trial or production)  
- Installed packages:  
  `dbt-core`, `dbt-snowflake`, `snowflake-connector-python`, `pandas`
