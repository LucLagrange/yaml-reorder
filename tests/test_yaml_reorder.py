import pytest
from pathlib import Path
import shutil
import tempfile
from yaml_reorder import (
    extract_sql_columns,
    clean_sql,
    reorder_yaml_columns,
    read_yaml_file,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestSimpleReorder:
    """Test basic column reordering functionality"""

    def test_extract_columns_from_simple_select(self):
        """Test that we correctly extract columns from a simple SELECT"""
        sql_file = FIXTURES_DIR / "simple.sql"

        with open(sql_file) as f:
            sql = f.read()

        cleaned = clean_sql(sql)
        columns = extract_sql_columns(cleaned, dialect="bigquery")

        assert columns == ["id", "email", "created_at", "name"]

    def test_reorder_yaml_to_match_sql(self):
        """Test that YAML columns are reordered to match SQL"""
        # Create a temporary copy of the YAML file to avoid modifying the fixture
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as tmp:
            yaml_file = FIXTURES_DIR / "simple.yml"
            shutil.copy(yaml_file, tmp.name)
            tmp_yaml = tmp.name

        try:
            # Read original order
            original = read_yaml_file(tmp_yaml)
            original_names = [col["name"] for col in original["models"][0]["columns"]]
            assert original_names == ["name", "created_at", "id", "email"]

            # Reorder
            sql_columns = ["id", "email", "created_at", "name"]
            was_reordered = reorder_yaml_columns(tmp_yaml, sql_columns)

            assert was_reordered is True, "Should return True when reordering happens"

            # Check new order
            reordered = read_yaml_file(tmp_yaml)
            new_names = [col["name"] for col in reordered["models"][0]["columns"]]
            assert new_names == ["id", "email", "created_at", "name"]

            # Verify descriptions are preserved
            id_col = next(
                c for c in reordered["models"][0]["columns"] if c["name"] == "id"
            )
            assert id_col["description"] == "Primary key"

        finally:
            # Clean up temp file
            Path(tmp_yaml).unlink()
