#!/usr/bin/env python3
"""
dbt YAML Column Ordering Script

Automatically reorders column definitions in dbt YAML schema files to match
the column order in corresponding SQL model files. Handles dbt/Jinja templating,
SQL comments, and preserves all column properties (descriptions, tests, meta, etc.).

Usage:
    python order_columns.py <path-to-sql-or-yaml-file> [--dialect DIALECT]

Example:
    python order_columns.py models/staging/stg_users.sql
    python order_columns.py models/staging/stg_users.sql --dialect snowflake
"""

import sys
import re
import argparse
from typing import List, Dict, Any
from pathlib import Path
import sqlglot
from ruamel.yaml import YAML

DEFAULT_DIALECT = "bigquery"


def clean_sql(sql: str) -> str:
    """
    Remove dbt/Jinja templating and SQL comments from SQL string.

    Strips out dbt config blocks, Jinja expressions/statements/comments,
    inline SQL comments, multiline SQL comments, and config blocks before
    WITH or SELECT statements.

    Args:
        sql: Raw SQL string from dbt model file.

    Returns:
        Cleaned SQL string ready for parsing.
    """
    sql = re.sub(r"\{\{\s*ref\([^)]*\)\s*\}\}", "dummy_table", sql)
    sql = re.sub(r"\{\{\s*source\([^)]*\)\s*\}\}", "dummy_table", sql)
    sql = re.sub(r"\{\{[^}]*\}\}", "dummy_macro", sql)
    sql = re.sub(r"\{%[\s\S]*?%\}", "", sql)
    sql = re.sub(r"\{#[\s\S]*?#\}", "", sql)
    sql = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
    sql = re.sub(r"/\*[\s\S]*?\*/", "", sql)
    sql = re.sub(r"^.*?(?=WITH|SELECT)", "", sql, flags=re.DOTALL | re.IGNORECASE)
    return sql


def extract_sql_columns(sql: str, dialect: str) -> List[str]:
    """
    Extract column names in order from SQL SELECT statement.

    Uses sqlglot to parse the SQL and extract column names or aliases
    from the SELECT clause in the order they appear.

    Args:
        sql: Cleaned SQL string.
        dialect: SQL dialect to use for parsing.

    Returns:
        List of column names in the order they appear in SELECT.

    Raises:
        sqlglot.errors.ParseError: If SQL cannot be parsed.
    """
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    return [col.alias_or_name for col in parsed.expressions]


def read_yaml_file(filepath: str) -> Dict[str, Any]:
    """
    Read and parse YAML file.

    Args:
        filepath: Path to YAML file.

    Returns:
        Parsed YAML content as dictionary.

    Raises:
        FileNotFoundError: If file doesn't exist.
        yaml.YAMLError: If YAML is invalid.
    """
    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.default_flow_style = False
    yaml.width = 4096  # Prevent unwanted line wrapping

    with open(filepath, "r") as f:
        return yaml.load(f)


def write_yaml_file(filepath: str, data: Dict[str, Any]) -> None:
    """
    Write data to YAML file.
    Preserves key order and uses block style for readability.

    Args:
        filepath: Path to YAML file.
        data: Dictionary to write to YAML.

    Raises:
        IOError: If file cannot be written.
    """
    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.default_flow_style = False
    yaml.width = 4096
    yaml.indent(mapping=2, sequence=2, offset=0)

    with open(filepath, "w") as f:
        yaml.dump(data, f)


def reorder_yaml_columns(yaml_file: str, sql_columns: List[str]) -> bool:
    """
    Reorder YAML column definitions to match SQL column order.

    Reads the YAML schema file, reorders the columns list to match the
    order from the SQL file, and writes back. Preserves all column
    properties including descriptions, tests, meta, and tags.

    Args:
        yaml_file: Path to dbt YAML schema file.
        sql_columns: List of column names in SQL order.

    Returns:
        True if columns were reordered, False if already in correct order.

    Raises:
        KeyError: If YAML structure is invalid.
        IndexError: If no models found in YAML.
    """
    config = read_yaml_file(yaml_file)

    if "models" not in config or not config["models"]:
        raise ValueError(f"No models found in {yaml_file}")

    model = config["models"][0]

    if "columns" not in model:
        raise ValueError(f"No columns found in model in {yaml_file}")

    # Create lookup dict preserving all column properties
    col_dict: Dict[str, Dict[str, Any]] = {col["name"]: col for col in model["columns"]}

    # Get current order
    current_order = [col["name"] for col in model["columns"]]

    # Get new order (only columns that exist in YAML)
    new_order = [name for name in sql_columns if name in col_dict]

    # Check if order changed
    if current_order == new_order:
        return False

    # Reorder to match SQL
    model["columns"] = [col_dict[name] for name in new_order]

    write_yaml_file(yaml_file, config)
    return True


def main() -> None:
    """
    Main entry point for the script.
    Accepts one or more SQL or YAML file paths as command line arguments,
    extracts columns from the SQL model, and reorders the corresponding
    YAML schema file to match.
    """
    parser = argparse.ArgumentParser(
        description="Reorder dbt YAML columns to match SQL column order"
    )
    parser.add_argument("files", nargs="+", help="Path(s) to SQL or YAML file(s)")
    parser.add_argument(
        "--dialect",
        default=DEFAULT_DIALECT,
        choices=[
            "bigquery",
            "snowflake",
            "postgres",
            "redshift",
            "databricks",
            "duckdb",
        ],
        help=f"SQL dialect (default: {DEFAULT_DIALECT})",
    )

    args = parser.parse_args()
    dialect = args.dialect
    any_file_reordered = False

    for input_file in args.files:
        try:
            # Determine SQL and YAML file paths from the input file
            if input_file.endswith((".yml", ".yaml")):
                yaml_file_path = Path(input_file)
                sql_file_path = yaml_file_path.with_suffix(".sql")
            else:
                sql_file_path = Path(input_file)
                yaml_file_path = sql_file_path.with_suffix(".yml")

            # Check if both paired files exist before proceeding
            if not sql_file_path.exists():
                # Silently skip if the corresponding SQL file doesn't exist
                continue
            if not yaml_file_path.exists():
                # Silently skip if there's no schema file to reorder
                continue

            # Read SQL file
            sql = sql_file_path.read_text()

            # Clean and parse SQL to get column order
            cleaned_sql = clean_sql(sql)
            sql_columns = extract_sql_columns(cleaned_sql, dialect)

            if not sql_columns:
                # Silently skip if no columns were found in the SQL
                continue

            # Reorder the YAML file and set the flag if changes were made
            if reorder_yaml_columns(str(yaml_file_path), sql_columns):
                print(f"✓ Reordered {len(sql_columns)} columns in {yaml_file_path}")
                any_file_reordered = True

        except Exception as e:
            print(f"Error processing {input_file}: {e}", file=sys.stderr)
            continue

    # After processing all files, exit with the appropriate code
    if any_file_reordered:
        sys.exit(1)  # Exit 1 to signal to pre-commit that files were modified
    else:
        sys.exit(0)  # Exit 0 for success with no changes


if __name__ == "__main__":
    main()
