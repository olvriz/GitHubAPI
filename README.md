# GitHub PR Insights Repository

This repository contains two main files that work together to automate the collection and analysis of Pull Request (PR) data from GitHub repositories. These tools aim to provide insights into PR metrics such as approval time, review patterns, and SLA compliance.

## Overview

This project leverages **Databricks** and **Apache Airflow** to retrieve and process GitHub Pull Request data. It integrates with the GitHub API to analyze repository activity, providing detailed metrics such as:

- Time taken for PR approval after specific labels are applied.
- PR review comments, changes, and requests count.
- SLA metrics and other actionable insights.

---

## Files

### 1. Databricks Notebook (`databricks_notebook.py`)

This notebook uses **PySpark** and the **GitHub API** to extract, process, and analyze PR data.

#### Key Features:
- Retrieves closed PRs from a specified GitHub repository.
- Calculates SLA and time-to-approval metrics.
- Filters and processes data into a structured Spark DataFrame for further analysis.

#### Key Variables:
- `github_token`: Personal access token for GitHub authentication.
- `repositorio`: Repository name passed as a parameter.
- `perfil`: GitHub workspace name.
- `itens`: Number of PRs to fetch, limited to 150 by default.

---

### 2. Airflow DAG (`airflow_dag.py`)

The Airflow DAG orchestrates the execution of the Databricks notebook for multiple repositories.

#### Key Features:
- Dynamically iterates through a list of repositories.
- Configures a cooldown function to manage GitHub API rate limits.
- Handles retry policies and execution timeouts.

#### Key Variables:
- `git_url`: GitHub repository URL for the Databricks notebook source.
- `git_branch`: Branch to fetch the notebook from.
- `dbw_iris_restapi_cluster_id`: Databricks cluster ID for execution.

---

## Setup Instructions

### Prerequisites

1. **Databricks**:
   - Configure a cluster and retrieve its ID (`cluster_id`).
2. **Apache Airflow**:
   - Install Airflow with required providers: `apache-airflow-providers-databricks`.
3. **GitHub API Token**:
   - Generate a Personal Access Token in GitHub and store it securely (e.g., Azure Key Vault).

---

### Steps

## 1. Clone the repository and set up the environment:
git clone <repository_url>
cd <repository_folder>

# Install required dependencies for Airflow
`pip install apache-airflow apache-airflow-providers-databricks`

Ensure PySpark and other dependencies are available for Databricks
If using a specific environment manager (e.g., conda), activate it first
Example: conda activate <your_environment_name>

## 2. Update the Databricks notebook file (databricks_notebook.py):
 Replace placeholders with your actual values:
 - Replace `github_token` with your GitHub personal access token
 - Replace `perfil` with your GitHub workspace name

## Update the Airflow DAG file (airflow_dag.py):
### Replace placeholders with your actual values:
 - `git_url`: URL of the GitHub repository hosting the notebook
 - `git_branch`: Branch containing the notebook
 - `dbw_iris_restapi_cluster_id`: ID of your Databricks cluster

--
## 3. Setup Airflow

# Add the Airflow DAG to your DAGs folder
cp airflow_dag.py <airflow_dags_folder>

# Start Airflow services
airflow scheduler &
airflow webserver

# Access the Airflow web UI to enable and trigger the DAG
# URL: http://localhost:8080

### Example schema:

| repository | created_at | merged_at | approver | pr_id | labels | sla (seconds) | num_comments | num_changes | ...

Airflow DAG Logs
Includes:
- Task execution details
- Cooldown handling for GitHub API rate limits
- Error logs for troubleshooting

