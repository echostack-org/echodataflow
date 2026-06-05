# Docker

Using Docker to manage and deploy Echodataflow ensures a consistent and reproducible environment, simplifying the setup and configuration process.

This guide will walk you through the steps to pull the Echodataflow Docker image from Docker Hub, run it with the necessary network settings, and configure it to interact with Prefect for workflow orchestration.

## Prerequisites
Before you begin, ensure that you have the following installed on your host machine:

- [`Docker`](https://www.docker.com/get-started/): The platform for developing, shipping, and running applications in containers.

## Pulling the Docker Image
To download the latest version of the Echodataflow Docker image from Docker Hub, use the following command:

```bash
docker pull blackdranzer/echodataflow
```

## Running the Docker Image
To run the Echodataflow image with network settings configured to interact with Prefect, use this command:

```bash
docker run --network="host" -e PREFECT_API_URL=http://host.docker.internal:4200/api blackdranzer/echodataflow
```

### Important Note on Configuration
The `PREFECT_API_URL` environment variable is essential for connecting the Docker container to a Prefect dashboard, enabling the monitoring and triggering of flows externally. This URL can be configured in two primary ways:

- Local Prefect Server:

Start a local server using the command:

```bash
prefect server start
```

Ensure that Prefect is installed on your host machine, which can be done following these [installation instructions](https://docs.prefect.io/latest/getting-started/installation/).

- Prefect Cloud: Utilize a Cloud API URL if you prefer cloud services. Be mindful of the [API rate limits](https://docs.prefect.io/latest/cloud/rate-limits/) associated with your Prefect Cloud account, which vary by plan.

## Additional Resources
For more detailed guidance on deploying Prefect with Docker, refer to the [Prefect Docker Deployment Guide](https://docs.prefect.io/latest/guides/docker/#building-a-docker-image).
