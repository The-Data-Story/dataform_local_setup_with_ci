# Dataform Local Setup with CI

Streamlined Google Cloud Dataform Development: Fully equipped with a local environment, CI integration, schema tests, and environment switching for efficient development workflows.

## Features

   - CI/CD with GitHub Workflows:
      - Preconfigured GitHub workflow for schema testing.
      - Detects schema changes, including added/dropped columns, type changes, and new tables.
      - Automatically triggers on PRs or manual dispatch.

   - Sample Data Transfer:
      - Script for transferring sample data to a BigQuery test project.
      - Handles partitioned and incremental data loads.

   - Local Development Environment:
      - Dockerized devcontainer setup for consistent development.
      - Includes scripts to switch between development and production environments.

   - Schema Tests:
      - Python-based schema testing integrated into the workflow.
      - Validates table structures and BigQuery configurations.

## Directory Structure
```bash
  dataform_local_setup_with_ci
  ├── .devcontainer
  │   ├── Dockerfile                      # Defines the Docker image and development environment setup.
  │   └── devcontainer.json               # Configuration file for launching the VS Code devcontainer.
  ├── .github
  │   └── workflows
  │       └── test_dataform.yml           # GitHub Actions workflow for automated schema testing.
  ├── definitions
  │   ├── intermediate
  │   │   └── order_summary.sqlx          # SQLX file defining intermediate transformations for order summaries.
  │   ├── report
  │   │   └── revenue_report.sqlx         # SQLX file for generating revenue analysis reports.
  │   ├── sources
  │   │   └── orders.sqlx                 # Source table definition for order data.
  │   └── staging
  │       └── staging_orders.sqlx         # Staging logic for transforming order data.
  ├── includes
  │   └── helpers.js                      # JavaScript helper functions for use in Dataform workflows.
  ├── src
  │   ├── exampleData
  │   │   ├── config.json                 # Configuration file for sample data transfer workflows.
  │   │   └── export_and_load.py          # Python script for exporting and loading sample data into BigQuery.
  │   ├── local_run_commands
  │   │   ├── dataform_exec               # Bash script for running Dataform commands with environment validation.
  │   │   ├── switch_env                  # Script to switch between development and production environments.
  │   │   ├── workflow_settings_dev.yaml  # Configuration file for development workflows.
  │   │   └── workflow_settings_prod.yaml # Configuration file for production workflows.
  │   ├── tests
  │   │   └── schema_test.py              # Python script for schema validation tests.
  │   ├── config.json                     # General configuration file for local setup.
  │   └── validate_settings.sh            # Shell script for validating workflow settings and configurations.
  ├── .gitignore                          # Specifies files and directories to exclude from version control.
  ├── README.md                           # Main documentation for setting up and using the project.
  ├── requirements.txt                    # Python dependencies required for testing and scripts.
  └── workflow_settings.yaml              # Default workflow configuration file.
```


---

## **Setup Instructions**

1. Fork and Clone the Repository

   - Fork the Repository:

      - Navigate to the repository and click Fork to create your own copy.

   - Clone Your Forked Repository:
      ```bash
      git clone https://github.com/<your-username>/dataform_local_setup_with_ci.git
      cd dataform_local_setup_with_ci
      ```

2. Prerequisites
  
    Ensure the following are installed:

   - Git: Version control system.

   - Docker or OrbStack: For running the devcontainer.

   - Visual Studio Code (VS Code):

      - Install the Dev Containers extension.

   - Google Cloud Account:

      - Two GCP projects with billing enabled.
      - A shared service account between both projects.

3. Set Up GCP Projects

   - Create Two GCP Projects:

      - In the Google Cloud Console, create two projects (e.g., dev-project and prod-project).
      - Enable billing for both projects.

   - Create a Service Account:

      - Assign the following roles:
        ```bash
        BigQuery Data Editor
        BigQuery Data Viewer
        BigQuery Job User
        ```

   - Share the Service Account:

      - Add the service account as a member to both projects and grant the required roles.

   - Download and Configure Credentials:

      - Download the JSON key for the service account and save it as GCPkey.json in the root directory.

4. Build and Start the Devcontainer

   - Open the Repository in VS Code:
      ```bash
      code .
      ```

   - Reopen in Dev Container:

      - VS Code will detect the .devcontainer setup.
      - Click Reopen in Container to build and initialize the environment.

   - Verify Container Setup:

      Run the following commands inside the container:
      ```bash
      node -v            # Check Node.js version
      python3 --version  # Check Python version
      dataform help      # Verify Dataform CLI
      ```

5. Configure Environment Variables (credentials for Google Cloud - e.g. for the example data transfer)

   - Export Variables Inside the Container:

      ```bash
      export GOOGLE_APPLICATION_CREDENTIALS=GCPkey.json
      ```
   - Configure `.gitignore`
      
      Ensure sensitive credential files are excluded from version control. Add the following lines to your `.gitignore` file:
      ```bash
      # Sensitive credentials
      node_modules/
      GCPkey.json
      .df-credentials.json
      ```

6. Initialize Dataform Credentials

    To set up your local environment for interacting with BigQuery, initialize the credentials for Dataform:

    ```bash
    dataform init-creds
    ```

      - Follow the prompts e.g.:
          - Select **EU** as your region.
          - Choose the **JSON service account** option.
          - Provide the file path to your **JSON key** (e.g., `GCPkey.json`).

---

## **Example Data Load to Test/Dev envionment**

The exampleData module contains configurations and scripts to facilitate data export and load workflows in BigQuery. Follow these steps to run the data load example:

### Step 1: Configure Your Google Cloud Credentials

Ensure your Google Cloud credentials are set up (as explained in the installation section).

### Step 2: Set Up Example Configuration

Update the src/exampleData/config.json file to match your source and target BigQuery projects, datasets, and tables. For example:
```bash
{
  "tables": [
    {
      "source_project": "prod-project",       // Source GCP project where the original table exists
      "source_dataset": "source_dataset1",    // Dataset in the source project
      "source_table": "event_20240201",       // Table to be transferred from the source
      "target_project": "dev-project",        // Target GCP project where data will be transferred
      "location": "EU",                       // BigQuery dataset location (e.g., US, EU)
      "partition_size": 10000,                // Size of each partition (rows per partition)
      "max_rows": 39990000                    // Maximum rows to transfer (used for partitioning)
    },
    {
      "source_project": "prod-project",       // Another table with the same transfer logic
      "source_dataset": "source_dataset2",
      "source_table": "orders",
      "target_project": "dev-project",
      "location": "EU",
      "partition_size": 100,
      "max_rows": 500000
    }
  ]
}
```

### How It Works

The script transfers partitioned data from a source BigQuery table (prod) to a target table (dev) efficiently.

  - Key Steps:

	  1.	Reads configuration from config.json.
	  2.	Validates required fields (source & target projects, dataset, table).
	  3.	Checks if the target table exists and asks to overwrite if necessary.
	  4.	Creates the destination dataset if missing.
	  5.	Partitions the source data to optimize transfer.
	  6.	Loads data in chunks to the target table.
	  7.	Drops temporary partitioned tables to clean up.

  - Naming Logic:

	  - The target dataset name is auto-generated using:
      
        ```bash
        target_dataset = f"{source_project.replace('-', '_')}__{source_dataset}"
        ```

	      Example: prod-project → prod_project__source_dataset1 in dev-project

        •	The target table retains the same name as the source table.

	  - Final Example in BigQuery

        ```bash
        Source:        prod-project.source_dataset1.event_20240201
        Temp Table:    prod-project.source_dataset1.event_20240201_partitioned # After the load, this will be dropped to clean up.
        Destination:   dev-project.prod_project__source_dataset1.event_20240201
        ```

### Step 3: Run the Data Export and Load Script

Use the provided export_and_load.py script to prepare the partitioned data and load it into the destination BigQuery table:
```bash
/usr/bin/python3 /dataform/src/exampleData/export_and_load.py
```


---

## **Running Dataform Commands Using dataform_exec and switch_env Scripts**

The local development environment includes scripts to switch between development and production environments and run Dataform commands with environment validation.

### Using dataform_exec Script

The dataform_exec script simplifies running Dataform commands with environment validation to prevent accidental execution in the wrong setup.

  - Usage:

    ```bash
    dataform_exec [mode] [additional_arguments]
    ```

  - Available Modes:

    | Mode       | Description                                       |
    |------------|---------------------------------------------------|
    | compile    | Compiles and validates the Dataform project.      |
    | test       | Runs unit tests after validating the environment. |
    | run        | Executes Dataform workflows after validation.     |
    | help       | Displays usage information.                       |


  - Examples:

    ```bash
    dataform_exec compile                # Compile and validate Dataform code
    dataform_exec test                   # Run Dataform unit tests
    dataform_exec run --dry-run          # Simulate workflow execution without modifying BigQuery
    dataform_exec run --full-refresh     # Run Dataform with a full refresh
    dataform_exec test --vars=foo=bar    # Run tests with variable overrides
    ```

  - Environment Validation:

    - The script checks the active environment in workflow_settings.yaml.

    - If the expected environment (dev) does not match, the script aborts execution to prevent mistakes.


### Using switch_env Script

The switch_env script toggles the Dataform workflow settings between development and production environments.


  - Environment Configuration: 

    The src/local_run_commands/workflow_settings_prod.yaml and src/local_run_commands/workflow_settings_dev.yaml files define the Dataform environment settings for production and development. These settings control project names, dataset locations, and environment-specific variables.


    1. Key Differences Between Prod and Dev:

        | Setting	                | Production (Prod)	        | Development (Dev)                       |
        |-------------------------|---------------------------|-----------------------------------------|
        |defaultProject	          | prod_project	            | dev_project
        |defaultDataset	          | dataform_model	          | prod_project__dataform_model
        |defaultAssertionDataset	| dataform_model_assertions	| prod_project__dataform_model_assertions
        |environment	            | prod	                    | dev
        |schema	                  | dataform_model	          | prod_project__dataform_model
        |source1_dataset	        | analytics_351111111	      | prod_project__analytics_351111111

    2. Naming Conventions

        Dataset and Table Naming Rules:

          - Why Use Prefixed Datasets in Development?

              1.	Prevents overwriting production data.
              2.	Allows parallel testing in a safe, isolated environment.
              3.	Makes debugging easier while maintaining the same table structure as production.

          - Example:

            ```bash
            Production Dataset:   prod_project.dataform_model
            Development Dataset:  dev_project.prod_project__dataform_model
            ```
          
          - The same applies to source datasets:

            ```bash
            Production Source Dataset:   prod_project.analytics_351111111
            Development Source Dataset:  dev_project.prod_project__analytics_351111111
            ```

        Production Settings (workflow_settings_prod.yaml)
          
          ```bash
          defaultProject: prod_project  # Main production project
          defaultLocation: EU  # Location where the datasets reside
          defaultDataset: dataform_model  # Dataset for transformed tables
          defaultAssertionDataset: dataform_model_assertions  # Dataset for assertion (test) results
          dataformCoreVersion: 3.0.0  # Dataform core version used
          vars:
            environment: prod  # Environment identifier
            schema: dataform_model  # Schema name for transformations
            project: prod_project  # Project where transformations run
            source1_dataset: analytics_351111111  # Source dataset in production
          ```

        Development Settings (workflow_settings_dev.yaml)

          ```bash
          defaultProject: dev_project  # Development project where transformations run
          defaultLocation: EU  # Same location as production for consistency
          defaultDataset: prod_project__dataform_model  # Prefixed dataset to prevent conflicts
          defaultAssertionDataset: prod_project__dataform_model_assertions  # Separate assertion dataset for development
          dataformCoreVersion: 3.0.0  # Same Dataform version as production
          vars:
            environment: dev  # Environment identifier
            schema: prod_project__dataform_model  # Schema name for development (prefixed to avoid conflicts)
            project: dev_project  # Dev project where transformations run
            source1_dataset: prod_project__analytics_351111111  # Prefixed dataset to separate dev from prod
          ```

  - Usage:

    ```bash
    switch_env <dev|prod|help>
    ```

  - Available Modes:

    | Mode       | Description                              |
    |------------|------------------------------------------|
    | dev        | Switches to the development environment. |
    | prod       | Switches to the production environment.  |
    | help       | Displays help information.               |


  - Examples:

    ```bash
    switch_env dev   # Switch to development mode
    switch_env prod  # Switch to production mode
    switch_env help  # Show usage instructions
    ```

  - How It Works:

    - Copies the respective config file to workflow_settings.yaml:

      ```bash
      src/local_run_commands/workflow_settings_dev.yaml  →  workflow_settings.yaml
      src/local_run_commands/workflow_settings_prod.yaml →  workflow_settings.yaml
      ```


---

## **Schema Testing in CI/CD Pipeline**

Schema testing is automatically triggered in **GitHub Actions** to validate **table structures, schema changes, and BigQuery configurations** before merging to production.

1. Supported Table Types

    - The schema test script currently supports:

        ✅ type: "view" → Validates schema consistency for views.

        ✅ type: "table" → Ensures table schema remains intact.

        ✅ type: "incremental" → Checks incremental table structure changes.

        ❌ Operations (e.g., DELETE, MERGE) are not yet supported.


2. Adding Google Cloud Credentials in GitHub Secrets

To authenticate with Google Cloud, store the service account key in GitHub Secrets:

  - Step 1: Add Secret in GitHub:

    1.	Navigate to GitHub → Your Repo → Settings → Secrets and Variables → Actions.

    2.	Click “New repository secret”.

    3.	Name it **GCPKEY**.

    4.	Paste the GCP service account JSON key and save.

  - Step 2: Make sure the same secret name (GCPKEY) is referenced in the GitHub Actions workflow (/.github/workflows/test_dataform.yml):

    ```bash
      - name: Set up Google Cloud authentication
      run: echo "${{ secrets.GCPKEY }}" > /tmp/gcpkey.json
    ```

3. How Schema Tests Are Triggered in CI/CD:

    - Schema validation is automatically triggered when:

        ✔ A pull request (PR) is created or updated.

        ✔ A push to dev branch includes the **runTest** commit message.


4. Schema Test Results:

    - Detects schema changes from test_log.json and formats results, including:

        -	Added columns.
        -	Dropped columns.
        -	Type changes.
        -	New tables.

    - Commits and pushes results only if:

        -	Warnings, errors, or schema changes are detected.
        -	Automatically skips commit if no changes are found.

### Example Commit Message:
```bash
Test results:

Schema Changes:
Processing table: project.dataform__report__analytics.paid_campaigns
    Added columns: 
      + tesfield (STRING)
    Dropped columns: 
      - cost (FLOAT)
    Type changes: 
      ~ SOURCE (STRING → INTEGER)

New Table Detected: project.dataform__staging__analytics.test_new_table
    Columns: 
      + test (STRING),
      + date (DATE)

Warnings:
Warning details go here...
```


