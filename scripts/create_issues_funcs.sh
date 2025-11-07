#!/bin/bash

# GitHub Issues Creation Functions
# Utility functions for creating GitHub issues with standardized formatting

# Function to create parent issue
create_parent_issue() {
    local title="$1"
    local body="$2"

    echo "Creating parent issue: $title"
    gh issue create \
        --title "$title" \
        --body "$body"
}

# Function to create child issue
create_child_issue() {
    local title="$1"
    local body="$2"

    echo "Creating child issue: $title"
    gh issue create \
        --title "$title" \
        --body "$body"
}

# Function to create issue from file
# Usage: create_issue_from_file "TITLE" BODY_FILE_PATH [parent|child]
create_issue_from_file() {
    local title="$1"
    local body_file="$2"
    local issue_type="${3:-child}"  # Default to child if not specified

    if [[ ! -f "$body_file" ]]; then
        echo "Error: Body file '$body_file' not found"
        return 1
    fi

    local body
    body=$(cat "$body_file")

    case "$issue_type" in
        "parent")
            create_parent_issue "$title" "$body"
            ;;
        "child")
            create_child_issue "$title" "$body"
            ;;
        *)
            echo "Error: Issue type must be 'parent' or 'child'"
            return 1
            ;;
    esac
}