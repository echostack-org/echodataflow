# AWS Installation and Setup

## Step 1: Create an EC2 Instance
Refer to the [AWS EC2] (https://docs.aws.amazon.com/efs/latest/ug/gs-step-one-create-ec2-resources.html) documentation for a step-by-step guide on creating an EC2 instance. Make sure you have the private key (.pem) file for SSH access.

---

## Step 2: Connect to EC2 using SSH
Connect to your EC2 instance using SSH. You can follow the instructions in the [AWS SSH] (https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/connect-linux-inst-ssh.html) documentation. You'll need the private key (.pem) file.

---

## Step 3: Install Echodataflow

### Step 3.1: Make a virtual environment

To keep your Echodataflow environment isolated, it's recommended to create a virtual environment using Conda or Python's built-in venv module. Here's an example using Conda:

```bash
conda create --name echodataflow-env
conda activate echodataflow-env
```

Or, using Python's venv:

```bash
python -m venv echodataflow-env
source echodataflow-env/bin/activate  # On Windows, use `echodataflow-env\Scripts\activate`
```
### Step 3.2: Clone the Repository
Now that you have a virtual environment set up, you can clone the Echodataflow project repository to your local machine using the following command:

```bash
git clone <echodataflow_repo>
```

### Step 3.3: Install the Package
Navigate to the project directory you've just cloned and install the Echodataflow package. The -e flag is crucial as it enables editable mode, which is especially helpful during development and testing. Now, take a moment and let the echodataflow do its thing while you enjoy your coffee.

```bash
cd <project_directory>
pip install -e .
```

---

## Step 4: Initialize Echodataflow and Prefect

To kickstart your journey with Echodataflow and Prefect, follow these simple initialization steps:

#### 4.1 Initializing Echodataflow
Begin by initializing Echodataflow with the following command:

```bash
echodataflow init
```

This command sets up the groundwork for your Echodataflow environment, preparing it for seamless usage.

#### 4.2 Initializing Prefect
For Prefect, initialization involves a few extra steps, including secure authentication. Enter the following command to initiate the Prefect authentication process:

- If you have a Prefect Cloud account, provide your Prefect API key to securely link your account. Type your API key when prompted and press Enter.

```bash
prefect cloud login
```

- If you don't have a Prefect Cloud account yet, you can use local prefect account. This is especially useful for those who are just starting out and want to explore Prefect without an account.

```bash
prefect profiles create echodataflow-local
```


The initialization process will ensure that both Echodataflow and Prefect are properly set up and ready for you to dive into your cloud-based workflows.

---

## Step 5: Create a S3 bucket to store the output

Create an S3 bucket to store the processed output. Refer to the [AWS S3] (https://docs.aws.amazon.com/quickstarts/latest/s3backup/step-1-create-bucket.html) documentation for guidance. Add the S3 URI to datastore.yaml in the same directory as this notebook under the urlpath key in the output section:

```yaml
# ...rest of the cofiguration
output: # Output arguments
  urlpath: <YOUR_S3_URI> # Destination data URL parameters
  overwrite: true
  storage_options:
    block_name: echodataflow-aws-credentials
    type: AWS
```

---

## Step 6: Store AWS Credentials

Edit the ~/.echodataflow/credentials.ini file and add your AWS Key and Secret.

```bash
nano ~/.echodataflow/credentials.ini

# add the following and save:
[echodataflow-aws-credentials]
aws_access_key_id=my-aws-key
aws_secret_access_key=my-aws-secret
provider=AWS
```

---

## Step 7: Create Credential blocks

Once you have stored the credentials in the ini file, call the below command to create a block securedly stored in your prefect account. For more about blocks refer [Blocks] (https://github.com/echostack-org/echodataflow/blob/dev/docs/configuration/blocks.md).

```bash
echodataflow load-credentials
```

---

## Step 8: Jupyter Notebook
Open Jupyter Notebook using terminal in the same activated environment

```bash
jupyter notebook
```
