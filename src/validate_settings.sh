#!/bin/bash

# Path to the config file
CONFIG_FILE="$(dirname "$0")/config.json"

# Ensure the config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "ERROR: $CONFIG_FILE not found."
    exit 1
fi

# Extract configuration values from the JSON file
MAIN_BRANCH=$(jq -r '.main_branch' "$CONFIG_FILE")
DEV_ALLOWED_BRANCHES=$(jq -r '.dev_allowed_branches[]' "$CONFIG_FILE")
PROD_ALLOWED_BRANCHES=$(jq -r '.prod_allowed_branches[]' "$CONFIG_FILE")

# Arguments: settings file, current branch
SETTINGS_FILE="$1"
BRANCH_NAME="$2"

# Ensure the settings file exists
if [ ! -f "$SETTINGS_FILE" ]; then
    echo "ERROR: $SETTINGS_FILE not found."
    exit 1
fi

# Extract the environment value from the YAML file
ENVIRONMENT=$(grep -E "^\s*environment:" "$SETTINGS_FILE" | awk '{print $2}')

# Validation logic for dev environment
if [[ "$ENVIRONMENT" == "dev" ]]; then
    if [[ "$BRANCH_NAME" == "$MAIN_BRANCH" ]]; then
        echo "ERROR: 'environment' is set to 'dev', but commits to the '$MAIN_BRANCH' branch are not allowed."
        exit 1
    fi
fi

# Validation logic for prod environment
if [[ "$ENVIRONMENT" == "prod" ]]; then
    if [[ "$BRANCH_NAME" != "$MAIN_BRANCH" ]]; then
        echo "ERROR: 'environment' is set to 'prod', but commits are only allowed to the '$MAIN_BRANCH' branch."
        exit 1
    fi
fi

# If everything is valid
echo "Validation passed for $SETTINGS_FILE."
