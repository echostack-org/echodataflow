# Implementing Logging with Kafka and Elastic Stack

## Overview
Echodataflow integrates with Kafka to send worker messages to a Kafka topic, which can then be routed to any logging destination. In this example, we will set up an [Elastic Stack](https://www.elastic.co/) (Elasticsearch, Logstash, Kibana, Beats) with Kafka integration to analyze and visualize logs in real-time.

### Setting Up the Infrastructure
Dockerized Elastic Stack with Kafka Integration

This setup includes:

- `Elasticsearch` for powerful search and data analytics
- `Logstash` for log processing and ingestion into Elasticsearch
- `Kibana` for data visualization with dashboards and analytics
- `Apache Kafka` with Zookeeper for reliable log streaming
- `Schema Registry` for managing Kafka data schemas
- `Confluent Control Center` for Kafka cluster management and monitoring
- `Metricbeat` for monitoring system metrics and Docker containers

**Prerequisites**
Ensure you have Docker and Docker Compose installed on your machine. A basic understanding of Docker, the ELK Stack, and Apache Kafka is recommended.

### Quick Start

1. Clone the Repository

```bash
git clone https://github.com/Sohambutala/StreamLogging.git
cd StreamLogging
```

2. Start the Services

```bash
docker-compose up -d
# This command starts all the services defined in the docker-compose.yml file in detached mode.
```

3. Access the Services

- `Kibana`: Open http://localhost:5601 to access Kibana's web interface.
- `Confluent Control Center`: Visit http://localhost:9021 to manage and monitor your Kafka cluster.
- `Elasticsearch`: Accessible at http://localhost:9200.

4. Monitor with Metricbeat

Metricbeat is configured to collect metrics from Docker containers and the host system. Check the Metricbeat dashboards in Kibana for insights.

### Configuration
You can customize the configurations for each service by modifying their respective configuration files in the repository:

- `Elasticsearch`: ./elasticsearch/elasticsearch.yml
- `Logstash`: ./logstash/config/logstash.yml and the pipeline files in ./logstash/pipeline/
- `Kibana`: ./kibana/kibana.yml
- `Metricbeat`: ./metricbeat/metricbeat.yml

### Stopping the Services
To stop all services and remove containers, networks, and volumes created by docker-compose up, run:

```bash
docker-compose down -v
```

## Configuring Kafka Logging in Echodataflow
Configure your Kafka server in the datastore.yaml file for echodataflow.

### Example datastore.yaml

```yaml
name: Bell_M._Shimada-SH1707-EK60
sonar_model: EK60
raw_regex: (.*)-?D(?P<date>\w{1,8})-T(?P<time>\w{1,6})
args:
  urlpath: s3://ncei-wcsd-archive/data/raw/Bell_M._Shimada/SH1707/EK60/{{file_name}}.raw
  parameters:
    file_name: Summer2017-D20170623-T233948
  storage_options:
    anon: true
  json_export: true
output:
  urlpath: ./echodataflow-output
  retention: true
  overwrite: true
# Kafka Config
logging:
  kafka:
    topic: echodataflow_logs
    servers:
    - localhost:9092
```
