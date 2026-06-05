# Prerequisites

Before you begin, make sure you have the following prerequisites in place:

- An AWS account with necessary permissions to create EC2 instances and S3 buckets.
- A private key (.pem) file for SSH access to your EC2 instance.
- Basic familiarity with the command-line interface (CLI).

## Notebook Setup

In this notebook, we'll go through the following steps:

1. Create an EC2 Instance: Set up an AWS EC2 instance to run your Echodataflow pipeline.
2. Connect to EC2 using SSH: Establish a secure connection to your EC2 instance using SSH.
3. Install Echodataflow: Create a virtual environment, clone the Echodataflow repository, and install the package.
4. Initialize Echodataflow and Prefect: Configure your Echodataflow and Prefect environments.
5. Create an S3 Bucket: Set up an AWS S3 bucket to store your processed data.
6. Store AWS Credentials: Store your AWS credentials securely for access.
7. Create Credential Blocks: Set up credential blocks for secure access within Prefect.
8. Open Jupyter Notebook: Launch Jupyter Notebook to execute your Echodataflow pipeline.
9. Setting Up: Setting up source S3.
10. Getting Data: Get data from the source S3.
11. Preparing Files: Preparing files for processing.
12. Processing with echodataflow: Execute the Pipeline.

Now, let's dive into each step to deploy your Echodataflow pipeline on AWS!
