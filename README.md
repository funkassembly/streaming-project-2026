# UXSIM Real-Time Traffic Streaming Project

This project implements a real-time traffic data streaming pipeline using **UXsim**, **Redpanda**, **Apache Spark Structured Streaming**, **MongoDB**, and **FastAPI**.

UXsim is used to simulate vehicle traffic. Vehicle data is sent to Redpanda, processed in real time by Spark Structured Streaming, and stored in MongoDB. A REST API built with FastAPI provides access to the stored traffic statistics.

## Architecture

```text
UXsim
  |
  | JSON vehicle records
  v
Redpanda
  |
  | Kafka-compatible stream
  v
Spark Structured Streaming
  |
  |-- Raw vehicle data
  |-- Statistics per link and timestep
  |-- 30-second window statistics
  v
MongoDB
  ^
  |
FastAPI REST API
```

## Technologies

- Python
- UXsim
- Redpanda
- Apache Spark Structured Streaming
- MongoDB
- FastAPI
- Docker / Docker Compose

## Project Files

```text
streaming_project/
├── run_uxsim.py
├── spark_consumer_PC.py
├── api.py
├── docker-compose.yml
├── requirements.txt
├── .gitignore
└── README.md
```

### `run_uxsim.py`

Creates the UXsim traffic simulation and acts as the Kafka producer.

Only moving vehicles are sent to the `uxsim` Redpanda topic as JSON records.

The delay between simulation timesteps can be configured using the `--interval` argument.

Example:

```bash
python3 run_uxsim.py --interval 1
```

### `spark_consumer_PC.py`

Uses Spark Structured Streaming to subscribe to the `uxsim` Redpanda topic.

The JSON messages are parsed using a predefined Spark schema.

For each simulation timestep and road link, Spark calculates:

- vehicle count (`vcount`)
- average speed (`vspeed`)
- minimum speed (`min_speed`)
- maximum speed (`max_speed`)

The program also calculates statistics over 30-second simulation-time windows.

### `api.py`

Provides a REST API using FastAPI for accessing the traffic statistics stored in MongoDB.

## Setup

### 1. Create the Python virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Start Docker

When using Docker Engine inside WSL:

```bash
sudo service docker start
```

### 4. Start the Docker services

```bash
docker compose up -d
```

Check the running containers:

```bash
docker compose ps
```

The project uses containers for:

- Redpanda
- MongoDB
- Spark Master
- Spark Worker
- Spark submit job

## Running the Project

### Start the Spark consumer

The Spark job is started through Docker Compose.

Its output can be monitored with:

```bash
docker logs -f spark-job
```

### Start the UXsim producer

Open another terminal:

```bash
cd ~/streaming_project
source .venv/bin/activate
python3 run_uxsim.py --interval 1
```

The `--interval` parameter specifies the number of real seconds to wait between UXsim simulation timesteps.

For example:

```bash
python3 run_uxsim.py --interval 0.5
```

replays the simulation faster than:

```bash
python3 run_uxsim.py --interval 2
```

## Redpanda

The producer sends vehicle records to the topic:

```text
uxsim
```

Messages can be inspected using:

```bash
docker exec -it redpanda rpk topic consume uxsim --num 5
```

Each message contains vehicle information such as:

```json
{
  "name": "8",
  "dn": 5,
  "orig": "E1",
  "dest": "W1",
  "t": 5,
  "link": "E1I4",
  "x": 0.0,
  "s": -1.0,
  "v": 50.0
}
```

## MongoDB

The Spark application stores data in the `traffic` database.

The main collections are:

```text
traffic
├── raw_data
├── stats
└── window_stats
```

### `raw_data`

Contains the original vehicle records received from Redpanda.

### `stats`

Contains statistics grouped by simulation timestep and road link.

Fields include:

```text
t
link
vcount
vspeed
min_speed
max_speed
```

### `window_stats`

Contains traffic statistics grouped into 30-second simulation-time windows.

Fields include:

```text
window_start
link
vcount
vspeed
min_speed
max_speed
```

MongoDB can be opened with:

```bash
docker exec -it mongo mongosh
```

Then:

```javascript
use traffic
show collections
```

Example:

```javascript
db.raw_data.findOne()
```

## MongoDB Aggregation

Average, minimum, and maximum speed for every link over the stored simulation interval can be calculated with:

```javascript
db.raw_data.aggregate([
  {
    $group: {
      _id: "$link",
      average_speed: { $avg: "$v" },
      min_speed: { $min: "$v" },
      max_speed: { $max: "$v" },
      vehicle_records: { $sum: 1 }
    }
  },
  {
    $project: {
      _id: 0,
      link: "$_id",
      average_speed: 1,
      min_speed: 1,
      max_speed: 1,
      vehicle_records: 1
    }
  },
  {
    $sort: {
      average_speed: -1
    }
  }
])
```

## REST API

Start the FastAPI application from the Python virtual environment:

```bash
fastapi dev api.py
```

The API is available at:

```text
http://localhost:8000
```

Interactive API documentation is available at:

```text
http://localhost:8000/docs
```

Available endpoints include:

```text
GET /
GET /health
GET /stats
GET /stats/{link}
GET /window-stats
GET /summary
```

For example:

```text
GET /stats/N1I1
```

returns statistics for the `N1I1` road link.

The `/summary` endpoint performs a MongoDB aggregation and returns overall traffic statistics for each link.

## Bonus Features

The project also implements the optional extensions:

1. Minimum and maximum vehicle speed per link.
2. 30-second windowed traffic aggregations.
3. REST API for accessing MongoDB traffic statistics.

## Stopping the Project

Stop the producer with:

```text
Ctrl+C
```

Stop the Docker services with:

```bash
docker compose down
```

The containers can later be started again with:

```bash
docker compose up -d
```
