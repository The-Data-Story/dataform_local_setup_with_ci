import json
from google.cloud import bigquery
import sys, os


def prepare_partitioned_data(config_path: str):
    """
    Prepares partitioned data in BigQuery, transfers each partition to a single destination table,
    and ensures no temporary partitioned table is left in the source project.

    Args:
        config_path (str): Path to the configuration JSON file.
    """
    # Load configuration
    try:
        with open(config_path, "r") as config_file:
            config = json.load(config_file)
    except FileNotFoundError:
        print(f"ERROR: Configuration file '{config_path}' not found.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"ERROR: Configuration file '{config_path}' is not a valid JSON file.")
        sys.exit(1)

    client = bigquery.Client()

    for table_config in config["tables"]:
        # Extract mandatory configuration parameters
        source_project = table_config.get("source_project")
        source_dataset = table_config.get("source_dataset")
        source_table = table_config.get("source_table")
        target_project = table_config.get("target_project")
        location = table_config.get("location", "EU")  # Default to "US" if not specified

        # Validate mandatory parameters
        missing_params = []
        if not source_project:
            missing_params.append("source_project")
        if not source_dataset:
            missing_params.append("source_dataset")
        if not source_table:
            missing_params.append("source_table")
        if not target_project:
            missing_params.append("target_project")

        if missing_params:
            print(f"ERROR: Missing mandatory parameters in table configuration: {', '.join(missing_params)}")
            print(f"Configuration details: {table_config}")
            sys.exit(1)

        # Extract optional parameters
        partition_size = table_config.get("partition_size", 10000)
        max_rows = table_config.get("max_rows", 39990000)  # Default to 39,990,000

        # Generate the destination dataset and table names
        target_dataset = f"{source_project.replace('-', '_')}__{source_dataset}"  # Auto-generate destination dataset name
        target_table = source_table  # Use the source table name as the target table name
        target_table_ref = f"{target_project}.{target_dataset}.{target_table}"

        # Construct source and partitioned table references
        source_table_ref = f"{source_project}.{source_dataset}.{source_table}"
        partitioned_table_ref = f"{source_project}.{source_dataset}.{source_table}_partitioned"

        # Step 1: Check if the destination table exists
        print(f"Checking if destination table {target_table_ref} exists...")
        destination_table_exists = False
        try:
            client.get_table(target_table_ref)
            destination_table_exists = True
        except Exception as e:
            lines = str(e).split("\n")
            first_line = lines[0] if lines else "Error message unavailable"
            print(f"Error: {first_line}")
            pass

        if destination_table_exists:
            response = input(
                f"Destination table {target_table_ref} already exists. Do you want to overwrite it? (y/n): "
            ).strip().lower()
            if response != 'y':
                print(f"Skipping further actions for {target_table_ref}...")
                continue  # Skip to the next table configuration

        # Step 2: Ensure the destination dataset exists
        print(f"Ensuring destination dataset {target_dataset} exists in project {target_project}...")
        dataset_ref = bigquery.Dataset(f"{target_project}.{target_dataset}")
        dataset_ref.location = location  # Use location from config
        try:
            client.get_dataset(dataset_ref)
        except Exception as e:
            lines = str(e).split("\n")
            first_line = lines[0] if lines else "Error message unavailable"
            print(f"Error: {first_line}")
            print(f"Creating destination dataset {target_dataset} in location {location}...")
            client.create_dataset(dataset_ref)

        try:
            # Step 3: Create a temporary partitioned table only if necessary
            if not destination_table_exists or response == 'y':
                partition_query = f"""
                CREATE OR REPLACE TABLE `{partitioned_table_ref}`
                PARTITION BY RANGE_BUCKET(partition_id, GENERATE_ARRAY(0, {max_rows // partition_size}, 1))
                AS
                SELECT
                    * except(row_num),
                    RANGE_BUCKET(row_num, GENERATE_ARRAY({partition_size}, {max_rows}, {partition_size})) AS partition_id
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS row_num
                    FROM
                        `{source_table_ref}`
                )
                WHERE
                    row_num <= {max_rows};
                """
                print(f"Creating temporary partitioned table for {source_table} using RANGE_BUCKET...")
                client.query(partition_query, location=location).result()
                print(f"Temporary partitioned table created: {partitioned_table_ref}")

                # Dynamically calculate the current row count from the table
                try:
                    print(f"Fetching row count for table: {partitioned_table_ref}...")
                    query = f"SELECT COUNT(*) AS row_count FROM {partitioned_table_ref}"
                    row_count_result = client.query(query, location="EU").result()
                    row_count = next(row_count_result).row_count
                    print(f"Current row count: {row_count}")
                except Exception as e:
                    print(f"ERROR: Failed to fetch row count: {str(e)}")
                    sys.exit(1)

                # Use the lesser value between the configured max_rows and the actual row count
                max_rows = min(max_rows, row_count)
                print(f"Using max_rows: {max_rows}")

                # Step 4: Create or overwrite the destination table schema
                print(f"Creating destination table {target_table_ref} with the correct schema...")
                destination_table_schema_query = f"""
                CREATE OR REPLACE TABLE `{target_table_ref}`
                AS SELECT * except(partition_id) FROM `{partitioned_table_ref}` WHERE FALSE;  -- Create table with the same schema
                """
                client.query(destination_table_schema_query, location=location).result()

                # Step 5: Export each partition to the destination table
                num_partitions = max(1, (max_rows + partition_size - 1) // partition_size)  # Always at least one partition
                print("num_partitions:", str(num_partitions))
                for partition_id in range(num_partitions):
                    transfer_query = f"""
                    INSERT INTO `{target_table_ref}`
                    SELECT * except(partition_id) FROM `{partitioned_table_ref}`
                    WHERE partition_id = {partition_id};
                    """
                    print(f"Loading partition {partition_id} of {source_table} into {target_table_ref}...")
                    client.query(transfer_query, location=location).result()

                print(f"Data transfer for {source_table} complete!")

        except Exception as e:
            print(f"ERROR: An unexpected error occurred during processing: {e}")
            sys.exit(1)

        finally:
            # Step 6: Drop the temporary partitioned table if it was created
            try:
                print(f"Ensuring temporary table {partitioned_table_ref} is dropped...")
                drop_table_query = f"DROP TABLE IF EXISTS `{partitioned_table_ref}`"
                client.query(drop_table_query, location=location).result()
                print(f"Temporary table {partitioned_table_ref} dropped.")
            except Exception as e:
                print(f"ERROR: Failed to drop temporary table {partitioned_table_ref}: {e}")

    print("All tables processed successfully!")


# Example usage
if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_file_name = "config.json"  # Path to your configuration file
    config_file_path = os.path.join(script_dir, config_file_name)
    prepare_partitioned_data(config_file_path)