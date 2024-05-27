# NBA-Real-time-Data-Analytics-Pipeline

## Description:
This repository contains the code and setup instructions for a real-time NBA data analytics pipeline.

## Project Overview:
The project addresses the challenge of processing and visualizing real-time NBA game data. It includes a pipeline that ingests data using the nba_api, processes it with Apache Kafka and Apache Spark, stores it in InfluxDB, and visualizes it with Grafana.

## Key Components:

1. Data Ingestion:
  Uses Apache Kafka to ingest real-time NBA data.
  Scripts for setting up Kafka and ingesting data from the nba_api.

2. Data Processing:
  Utilizes Apache Spark for transforming and processing the data.
  Includes a schema definition and processing scripts.

3. Data Storage:
  Stores processed data in InfluxDB, a time-series database.
  Instructions for setting up InfluxDB and storing data.

4. Data Visualization:
  Configures Grafana to visualize the real-time data.
  Steps to set up and use Grafana dashboards.

Setup Instructions:

Detailed instructions for setting up the environment, including installing and configuring Java, Kafka, Spark, InfluxDB, and Grafana.
Python scripts and commands for running the data pipeline.

Impact:
The pipeline enhances game analysis, optimizes player performance, and boosts fan engagement by providing real-time, actionable insights from live NBA game data.

How to Use:

Follow the setup instructions to configure your environment.
Use the provided Python scripts to start the Kafka producer and Spark jobs.
Visualize the real-time data using Grafana dashboards.

Contributions:
We welcome contributions and improvements. Please fork the repository and submit pull requests with your enhancements.

