import json
import os, sys
import time
from google.cloud import bigquery
from typing import List, Dict, Set
from google.api_core.exceptions import NotFound

warning_messages = []
error_messages = []

def last_char_not_semicolon(input_string):
    # Remove all trailing whitespace, newlines, and tabs
    stripped_string = input_string.rstrip()
    # Check if the last character is a semicolon
    return not stripped_string.endswith(";")

def parse_error_message(exception: Exception) -> str:
    """Extract and clean the error message from a BigQuery exception."""
    try:
        # Attempt to parse BigQuery-specific error structure
        if hasattr(exception, "errors") and exception.errors:
            error_details = exception.errors[0]  # Get the first error
            return error_details.get("message", str(exception))
        # Fallback to string representation if no specific structure found
        return str(exception)
    except Exception:
        # Return the full exception as a fallback
        return str(exception)
    
# Function to get project and dataset IDs from Dataform JSON
def get_project_and_dataset_ids(json_path: str) -> List[Dict[str, str]]:
    with open(json_path, "r") as f:
        data = json.load(f)
    project_dataset_ids = []
    for table in data.get("tables", []):
        target = table.get("target", {})
        project_dataset_ids.append({
            "project_id": target.get("database", ""),
            "dataset_id": target.get("schema", "")
        })
    return project_dataset_ids

# Function to generate a random suffix for temp table names (optional)
def generate_temp_suffix() -> str:
    import random
    import string
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))

# Function to build the SQL query for creating tables with partitioning and preOps
def build_sql_query(table: Dict, test_table_map: Dict[str, Dict[str, str]]) -> str:
    target = table.get("target", {})
    query = table.get("query", "")
    pre_ops = table.get("preOps", [])
    post_ops = table.get("postOps", [])
    incremental_query = table.get("incrementalQuery", "")
    incremental_pre_ops = table.get("incrementalPreOps", [])
    incremental_post_ops = table.get("incrementalpostOps", [])

    # Replace table references in the query with test table names if available
    for original_table, test_versions in test_table_map.items():
        original_query_table = f"`{original_table}`"
        test_query_table = f"`{test_versions['test_table']}`"
        
        if query:
            query = query.replace(original_query_table, test_query_table)
        if incremental_query:
            incremental_query = incremental_query.replace(original_query_table, test_query_table)

    # Generate different random suffixes for temp tables
    temp_suffix_sql = generate_temp_suffix()  # Suffix for SQL query
    temp_suffix_incremental = generate_temp_suffix()  # Suffix for Incremental query

    temp_table_name_sql = f"{target['name']}_test_{temp_suffix_sql}"
    temp_table_name_incremental = f"{target['name']}_test_{temp_suffix_incremental}"

    # Generate the 'CREATE TABLE' clause for both queries
    create_table_clause_sql = f"CREATE TABLE `{target['database']}.{target['schema']}.{temp_table_name_sql}`"
    create_table_clause_incremental = f"CREATE TABLE `{target['database']}.{target['schema']}.{temp_table_name_incremental}`"

    # Add partitioning clause if partitionBy is present for both queries
    if "bigquery" in table and "partitionBy" in table["bigquery"]:
        partition_column = table["bigquery"]["partitionBy"]
        partition_clause = f"PARTITION BY {partition_column}"
        create_table_clause_sql = f"{create_table_clause_sql} {partition_clause}"
        create_table_clause_incremental = f"{create_table_clause_incremental} {partition_clause}"

    pre_ops_str = ""
    post_ops_str = ""
    sql_query = ""

    if pre_ops:
        if last_char_not_semicolon("\n".join(pre_ops)):
            pre_ops_str = "\n".join(pre_ops) + ";\n"
        else:
            pre_ops_str = "\n".join(pre_ops)

    if post_ops:
        if last_char_not_semicolon("\n".join(post_ops)):
            post_ops_str = "\n".join(post_ops) + ";\n"
        else:
            post_ops_str = "\n".join(post_ops)

    if query:
        if last_char_not_semicolon(query):
            sql_query = create_table_clause_sql + "\nAS\n" + query + ";\n"
        else:
            sql_query = create_table_clause_sql + "\nAS\n" + query
    
    sql_query = pre_ops_str + " " + sql_query + " " + post_ops_str

    # Wrap the queries in BEGIN and END with semicolons separating statements
    if query:
        wrapped_sql_query = f"BEGIN\n{sql_query}\nEND;"
    else:
        wrapped_sql_query = ''

    incremental_pre_ops_str = ""
    incremental_post_ops_str = ""
    incremental_sql_query = ""

    if incremental_pre_ops:
        if last_char_not_semicolon("\n".join(incremental_pre_ops)):
            incremental_pre_ops_str = "\n".join(incremental_pre_ops) + ";\n"
        else:
            incremental_pre_ops_str = "\n".join(incremental_pre_ops)

    if incremental_post_ops:
        if last_char_not_semicolon("\n".join(incremental_post_ops)):
            incremental_post_ops_str = "\n".join(incremental_post_ops) + ";\n"
        else:
            incremental_post_ops_str = "\n".join(incremental_post_ops)

    if incremental_query:
        if last_char_not_semicolon(incremental_query):
            incremental_sql_query = create_table_clause_incremental + "\nAS\n" + incremental_query + ";\n"
        else:
            incremental_sql_query = create_table_clause_incremental + "\nAS\n" + incremental_query
    
    incremental_sql_query = incremental_pre_ops_str + " " + incremental_sql_query + " " + incremental_post_ops_str

    # Wrap the queries in BEGIN and END with semicolons separating statements
    if incremental_query:
        wrapped_incremental_query = f"BEGIN\n{incremental_sql_query}\nEND;"
    else:
        wrapped_incremental_query = ''

    return wrapped_sql_query, wrapped_incremental_query, target['database'], target['schema'], temp_table_name_sql, temp_table_name_incremental

# Function to check if the query alters the schema (add/drop columns, create tables)
def is_schema_altering(query: str) -> bool:
    schema_keywords = ['CREATE', 'ALTER', 'DROP', 'ADD', 'PARTITION']
    return any(keyword in query.upper() for keyword in schema_keywords)

# Function to get the current schema of a table (for comparison)
def get_current_schema(client: bigquery.Client, project_id: str, dataset_id: str, table_id: str):
    try:
        table_ref = client.get_table(f"{project_id}.{dataset_id}.{table_id}")
        return {field.name: field.field_type for field in table_ref.schema}  # Return a dict with column names and types
    except NotFound:
        return {}  # Return an empty dictionary if the table doesn't exist

# Function to check if a table exists
def table_exists(client: bigquery.Client, project_id: str, dataset_id: str, table_id: str) -> bool:
    try:
        client.get_table(f"{project_id}.{dataset_id}.{table_id}")
        return True
    except NotFound:
        return False

# Function to perform a dry run and get the estimated size of the query
def get_query_size_estimate(client: bigquery.Client, query: str) -> int:
    job_config = bigquery.QueryJobConfig(dry_run=True, use_legacy_sql=False)  # Enable dry run
    dry_run_job = client.query(query, job_config=job_config)
    return dry_run_job.total_bytes_processed

# Function to compare schemas and identify added, dropped, or type-changed columns
# Function to compare schemas and identify added, dropped, or type-changed columns
def compare_schemas(schema_before: Dict[str, str], schema_after: Dict[str, str], table_id: str):
    added_columns = set(schema_after.keys()).difference(schema_before.keys())
    dropped_columns = set(schema_before.keys()).difference(schema_after.keys())
    type_changed_columns = []

    for col in schema_before:
        if col in schema_after and schema_before[col] != schema_after[col]:
            type_changed_columns.append((col, schema_before[col], schema_after[col]))

    changes_report = []
    if added_columns:
        changes_report.append(f"        Added columns:")
        for col in added_columns:
            field_type = schema_after.get(col, "UNKNOWN")
            changes_report.append(f"            + {col} ({field_type})")

    if dropped_columns:
        changes_report.append(f"        Dropped columns:")
        for col in dropped_columns:
            field_type = schema_before.get(col, "UNKNOWN")
            changes_report.append(f"            - {col} ({field_type})")

    if type_changed_columns:
        changes_report.append(f"        Type changed columns:")
        for col, old_type, new_type in type_changed_columns:
            changes_report.append(f"            ~ {col} (Old type: {old_type}, New type: {new_type})")

    if changes_report:
        warning_message = ""
        if schema_before == {}:
            print(f"    New Table detected: {table_id},\n    Schema for New Table:")
            warning_message += f"New Table detected: {table_id},\n    Schema for New Table:\n"
        else:
            print(f"    Schema Change Report for Dataform Table:")
            warning_message += f"Processing table: {table_id}\n    Schema Change Report for Dataform Table:\n"
        for line in changes_report:
            print(line)
            warning_message += line+"\n"
        warning_messages.append(warning_message)
        print(f"::warning::{warning_message}")  # GitHub CI warning annotation

def store_test_tables(test_tables_file: str, created_tables: List[str]):
    """
    Store the created test tables into a JSON file for later use.

    Args:
        test_tables_file (str): Path to the file where test tables will be stored.
        created_tables (List[str]): List of created test table names.
    """
    try:
        # Load existing data from the file if it exists
        if os.path.exists(test_tables_file):
            with open(test_tables_file, "r") as f:
                existing_tables = json.load(f)
        else:
            existing_tables = []

        # Merge new test tables with the existing ones
        updated_tables = list(set(existing_tables + created_tables))

        # Save back to the file
        with open(test_tables_file, "w") as f:
            json.dump(updated_tables, f, indent=4)

        print(f"Test tables successfully stored in {test_tables_file}.")
    except Exception as e:
        print(f"Error storing test tables: {e}")


def process_table(client, table, processed_tables: set[str], data: Dict, created_tables: List[str], test_table_map: Dict[str, Dict[str, str]]):
    target = table.get("target", {})
    project_id = target.get("database", "")
    dataset_id = target.get("schema", "")
    table_name = target.get("name", "")
    table_id = f"{project_id}.{dataset_id}.{table_name}"

    if table_id in processed_tables:
        return  # Skip already processed tables

    # Process dependencies first
    dependencies = table.get("dependencyTargets", [])
    for dependency in dependencies:
        dep_table = next((t for t in data.get("tables", []) if t.get("target") == dependency), None)
        if dep_table:
            dep_table_id = f"{dependency['database']}.{dependency['schema']}.{dependency['name']}"
            #print(f"Processing dependency table: {dep_table_id}")
            process_table(client, dep_table, processed_tables, data, created_tables, test_table_map)

    try:
        print(f"Processing table: {table_id}")
        processed_tables.add(table_id)

        # Capture the initial schema
        schema_before = get_current_schema(client, project_id, dataset_id, table_name)

        # Build queries
        #combined_query, combined_incremental_query, target_database, target_schema, temp_table_name_sql, temp_table_name_incremental = build_sql_query(table)
        combined_query, combined_incremental_query, target_database, target_schema, temp_table_name_sql, temp_table_name_incremental = build_sql_query(table, test_table_map)

        # Skip combined_query if combined_incremental_query is available
        if combined_incremental_query:
            #print(f"Skipping combined query for {table_name} because incremental query is available.")
            try:
                query_job = client.query(combined_incremental_query)
                query_job.result()  # Wait for query to complete
                #print(f"Incremental query executed successfully for {table_name}")
            except Exception as e:
                # Extract and clean the error message
                error_message = parse_error_message(e)
                error_messages.append(error_message)
                print(f"::error::{error_message}")  # GitHub CI error annotation


            schema_after = get_current_schema(client, project_id, dataset_id, temp_table_name_incremental)
            compare_schemas(schema_before, schema_after, table_id)
            # Add created tables to the cleanup list
            created_tables.append(f"{target_database}.{target_schema}.{temp_table_name_incremental}")
            # Store new test tables in the map
            test_table_map[table_id] = {
                "test_table": f"{target_database}.{target_schema}.{temp_table_name_incremental}",
            }
        else:
            if combined_query:
                try:
                    query_job = client.query(combined_query)
                    query_job.result()  # Wait for query to complete
                    #print(f"Query executed successfully for {table_name}")
                except Exception as e:
                    # Extract and clean the error message
                    error_message = parse_error_message(e)
                    error_messages.append(error_message)
                    print(f"::error::{error_message}")  # GitHub CI error annotation

                schema_after = get_current_schema(client, project_id, dataset_id, temp_table_name_sql)
                compare_schemas(schema_before, schema_after, table_id)
                # Add created tables to the cleanup list
                created_tables.append(f"{target_database}.{target_schema}.{temp_table_name_sql}")
                # Store new test tables in the map
                test_table_map[table_id] = {
                    "test_table": f"{target_database}.{target_schema}.{temp_table_name_sql}",
                }
            
    except Exception as e:
        print(f"Error processing table {table_name}: {e}")

# Main logic to generate and run queries
def main():
    # Start timer
    start_time = time.time()

    dataform_json_path = "src/tests/compiled_queries/result.json"  # Path to your Dataform JSON file
    client = bigquery.Client()  # Initialize BigQuery client

    with open(dataform_json_path, "r") as f:
        data = json.load(f)  # Load Dataform JSON

    processed_tables = set()  # Set to track already processed tables
    created_tables = []  # List to track temporary tables for cleanup
    test_table_map = {}  # Dictionary to map original tables to test tables

    try:
        # Process each table in the Dataform JSON
        for table in data.get("tables", []):
            process_table(client, table, processed_tables, data, created_tables, test_table_map)

    except Exception as e:
        print(f"Error in processing: {e}")

    finally:
        # Clean up temporary tables
        print("\nCleaning up temporary tables...")
        for created_table in created_tables:
            try:
                print(f"Dropping table: {created_table}")
                client.query(f"DROP TABLE IF EXISTS `{created_table}`").result()
            except Exception as e:
                # Extract and clean the error message
                error_message = parse_error_message(e)
                error_messages.append(error_message)
                print(f"::error::{error_message}")  # GitHub CI error annotation

        # Calculate and print total runtime
        end_time = time.time()
        elapsed_time = end_time - start_time
        hours, remainder = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        print(f"\nTotal test runtime: {int(hours):02}:{int(minutes):02}:{int(seconds):02}")


        # Print summary
        if warning_messages:
            print("\nWarnings:")
            for warning in warning_messages:
                print(f"  - {warning}")

        if error_messages:
            print("\nErrors:")
            for error in error_messages:
                print(f"  - {error}")

       
       # List to store warnings and errors
        log_messages = {
            "warnings": [],
            "errors": []
        }

        print(warning_messages)
        print(error_messages)
        log_messages["warnings"] = warning_messages
        log_messages["errors"] = error_messages

        print(log_messages)

        print("create the file:")
        # Write the log messages to a file
        with open("/tmp/test_log.json", "w") as log_file:
            json.dump(log_messages, log_file)

        print("read the file:")
        # Read back the file to ensure it was written correctly
        with open("/tmp/test_log.json", "r") as log_file:
            content = json.load(log_file)
            print("File contents:", json.dumps(content, indent=4))  # Pretty-print the content
       
       
        # Exit with appropriate code
        if error_messages:
            print("Errors!" + f"\nTotal test runtime: {int(hours):02}:{int(minutes):02}:{int(seconds):02}")
            sys.exit(1)  # Exit with error
        elif warning_messages:
            print("Warnings!" + f"\nTotal test runtime: {int(hours):02}:{int(minutes):02}:{int(seconds):02}")
            sys.exit(0)  # Warnings only, but no critical errors
        else:
            print("All tests passed successfully!" + f"\nTotal test runtime: {int(hours):02}:{int(minutes):02}:{int(seconds):02}")
            sys.exit(0)  # Exit with success

if __name__ == "__main__":
    main()
