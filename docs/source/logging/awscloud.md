# Implementing the Logging System

The logging system in echodataflow is implemented using Python’s built-in logging module. The logger is configured to send logs to various destinations based on the configuration provided. Adding CloudWatch logging for your echodataflow application is straightforward.

## AWS CloudWatch
### Setting Up AWS CloudWatch
First, configure AWS CloudWatch by following the instructions provided in the (AWS CloudWatch Getting Setup Guide)[https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/GettingSetup.html].

**Note**: You also need to set up the AWS CLI and ensure your Python environment has the `watchtower` package installed.

You can find the steps for setting up the AWS CLI (here)[https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html].

To install the watchtower package, run:

```bash
pip install watchtower
```

### Example

#### Configuring AWS CloudWatch Logging
Create a logging YAML file to configure AWS CloudWatch logging for echodataflow. Below is an example configuration:

```yaml
version: 1
disable_existing_loggers: False
formatters:
  json:
    format: "[%(asctime)s] %(process)d %(levelname)s %(name)s:%(funcName)s:%(lineno)s - %(message)s"
  plaintext:
    format: "[%(asctime)s] %(process)d %(levelname)s %(name)s:%(funcName)s:%(lineno)s - %(message)s"
handlers:
  echodataflow_watchtower:
    class: watchtower.CloudWatchLogHandler
    formatter: json
    level: DEBUG
    log_group_name: echodataflow_logs
    log_stream_name: echodataflow_stream
    send_interval: 10
    create_log_group: False
loggers:
  echodataflow:
    level: DEBUG
    propagate: False
    handlers: [echodataflow_watchtower]
```

#### Integrating the Logging Configuration
Finally, pass this YAML configuration file along with the dataset and pipeline configuration when initializing echodataflow.

Here is an example of how to integrate the logging configuration into your echodataflow initialization:

```python
# Example initialization of echodataflow
dataset_config = 'path_to_dataset_config'
pipeline_config = 'path_to_pipeline_config'
logging_config = 'path_to_logging_config_yaml'
options = {}  # Add your options here

data = echodataflow_start(dataset_config=dataset_config, pipeline_config=pipeline_config, logging_config=logging_config, options=options)
```
