# World Happiness Analysis and Real-Time Dashboard

This project analyzes the World Happiness Report data from 2015 to 2019, implementing a data pipeline with ETL processes, Kafka streaming, and a real-time dashboard visualization.

## Project Structure

```
├── docker-compose.yml          # Docker configuration for services
├── ETL.py                     # ETL script for data processing
├── dashboard/
│   ├── backend.py            # Dashboard backend server
│   └── dashboard.html        # Dashboard frontend interface
├── data/
│   ├── 2015.csv             # Annual happiness data files
│   ├── 2016.csv
│   ├── 2017.csv
│   ├── 2018.csv
│   ├── 2019.csv
│   └── happiness_merged_2015-2019.csv  # Merged dataset
├── kafka/
│   ├── consumer.py          # Kafka consumer implementation
│   └── producer.py          # Kafka producer implementation
└── notebooks/
    ├── EDA.ipynb           # Exploratory Data Analysis
    ├── ETL.ipynb           # ETL process notebook
    └── model_regresion.ipynb # Regression model analysis
```

## Features

- Data Processing Pipeline using ETL methodology
- Real-time data streaming with Apache Kafka
- Interactive dashboard for data visualization
- Exploratory Data Analysis (EDA)
- Regression modeling for happiness score prediction

## Prerequisites

- Python 3.x
- Docker and Docker Compose
- Apache Kafka
- Required Python packages (install using `pip install -r requirements.txt`):
  - pandas
  - numpy
  - kafka-python
  - flask
  - plotly
  - scikit-learn

## Setup and Installation

1. Clone the repository:
```bash
git clone 'https://github.com/anniecorrea/WORKSHOP3.git'
cd WORKSHOP3
```

2. Start the Kafka services using Docker Compose:
```bash
docker-compose up -d
```

3. Run the ETL process:
```bash
python ETL.py
```

4. Start the Kafka producer and consumer:
```bash
python kafka/producer.py
python kafka/consumer.py
```

5. Launch the dashboard:
```bash
python dashboard/backend.py
```

## Components Description

### ETL Process
The ETL (Extract, Transform, Load) process handles:
- Data extraction from annual CSV files (2015-2019)
- Data transformation and cleaning
- Loading processed data into a unified dataset

### Kafka Streaming
- **Producer**: Streams processed happiness data
- **Consumer**: Receives and processes streaming data for real-time analysis

### Dashboard
- Interactive visualization of happiness metrics
- Real-time updates through Kafka integration
- Comparative analysis across years and countries

### Analysis Notebooks
- **EDA.ipynb**: Exploratory analysis of happiness data
- **model_regresion.ipynb**: Regression analysis and predictions

## Usage

1. Access the dashboard through your web browser:
```
http://localhost:5000
```

2. View real-time data updates and visualizations
3. Explore different metrics and countries' happiness scores
4. Analyze trends and patterns in the World Happiness Report data


## Acknowledgments

- World Happiness Report for providing the dataset
- Contributors and maintainers of this project
