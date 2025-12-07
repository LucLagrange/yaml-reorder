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
import yaml

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
    with open(filepath) as f:
        return yaml.safe_load(f)


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
    with open(filepath, "w") as f:
        yaml.dump(data, f, sort_keys=False, default_flow_style=False)


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

    Accepts either a SQL or YAML file path as command line argument,
    extracts columns from the SQL model, and reorders the corresponding
    YAML schema file to match.
    """
    parser = argparse.ArgumentParser(
        description="Reorder dbt YAML columns to match SQL column order"
    )
    parser.add_argument("file", help="Path to SQL or YAML file")
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
    input_file = args.file
    dialect = args.dialect

    try:
        # Handle both .sql and .yml inputs
        if input_file.endswith(".yml") or input_file.endswith(".yaml"):
            yaml_file = input_file
            sql_file = input_file.replace(".yml", ".sql").replace(".yaml", ".sql")
        else:
            sql_file = input_file
            yaml_file = input_file.replace(".sql", ".yml")

        # Check if files exist
        if not Path(sql_file).exists():
            print(f"Error: SQL file not found: {sql_file}", file=sys.stderr)
            sys.exit(1)

        if not Path(yaml_file).exists():
            print(f"Warning: YAML file not found: {yaml_file}", file=sys.stderr)
            print("Skipping (no schema file to reorder)", file=sys.stderr)
            sys.exit(0)

        # Read SQL file
        try:
            with open(sql_file) as f:
                sql = f.read()
        except IOError as e:
            print(f"Error: Cannot read SQL file {sql_file}: {e}", file=sys.stderr)
            sys.exit(1)

        # Clean and parse SQL
        try:
            cleaned_sql = clean_sql(sql)
            sql_columns = extract_sql_columns(cleaned_sql, dialect)
        except sqlglot.errors.ParseError as e:
            print(f"Error: Cannot parse SQL in {sql_file}", file=sys.stderr)
            print(f"Parse error: {e}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"Error: Unexpected error while parsing SQL: {e}", file=sys.stderr)
            sys.exit(1)

        if not sql_columns:
            print(f"Warning: No columns found in {sql_file}", file=sys.stderr)
            sys.exit(0)

        # Reorder YAML
        try:
            was_reordered = reorder_yaml_columns(yaml_file, sql_columns)
            if was_reordered:
                print(f"✓ Reordered {len(sql_columns)} columns in {yaml_file}")
                sys.exit(1)  # Fail so user can review changes and re-commit
            else:
                print(f"✓ Nothing to reorder in {yaml_file} (already in correct order)")
        except FileNotFoundError:
            print(f"Error: YAML file not found: {yaml_file}", file=sys.stderr)
            sys.exit(1)
        except yaml.YAMLError as e:
            print(f"Error: Invalid YAML in {yaml_file}: {e}", file=sys.stderr)
            sys.exit(1)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        except IOError as e:
            print(f"Error: Cannot write to {yaml_file}: {e}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(
                f"Error: Unexpected error while processing YAML: {e}", file=sys.stderr
            )
            sys.exit(1)

        sys.exit(0)

    except KeyboardInterrupt:
        print("\nInterrupted by user", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"Error: Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

