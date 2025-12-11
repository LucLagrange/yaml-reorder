import pytest
import subprocess
import sys
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


class TestMultipleFileHandling:
    """Tests for handling multiple file inputs by running the script as a subprocess."""

    def test_processes_multiple_files_from_fixtures(self, tmp_path):
        """
        Verify that the script can process multiple fixture files in one run.
        It copies fixtures to a temporary directory to avoid modifying them.
        """
        # 1. --- SETUP ---
        # Copy the fixture files to a temporary directory so the script can modify them.
        shutil.copy(FIXTURES_DIR / "multi_1.sql", tmp_path)
        shutil.copy(FIXTURES_DIR / "multi_1.yml", tmp_path)
        shutil.copy(FIXTURES_DIR / "multi_2.sql", tmp_path)
        shutil.copy(FIXTURES_DIR / "multi_2.yml", tmp_path)

        sql1_path = tmp_path / "multi_1.sql"
        sql2_path = tmp_path / "multi_2.sql"
        yml1_path = tmp_path / "multi_1.yml"
        yml2_path = tmp_path / "multi_2.yml"

        # 2. --- EXECUTION ---
        # Run the script, passing both SQL fixture paths as arguments.
        script_path = Path(__file__).parent.parent / "yaml_reorder.py"
        process = subprocess.run(
            [sys.executable, str(script_path), str(sql1_path), str(sql2_path)],
            capture_output=True,
            text=True,
        )

        # 3. --- ASSERTION ---
        # Check that the script exited with code 1 (signaling a change)
        assert (
            process.returncode == 1
        ), "Script should exit with 1 when files are changed"

        # Verify Pair 1 is now correctly ordered
        reordered_yml1 = read_yaml_file(str(yml1_path))
        new_order1 = [col["name"] for col in reordered_yml1["models"][0]["columns"]]
        assert new_order1 == ["id", "name"], "Pair 1 should be reordered"

        # Verify Pair 2 is also correctly ordered
        reordered_yml2 = read_yaml_file(str(yml2_path))
        new_order2 = [col["name"] for col in reordered_yml2["models"][0]["columns"]]
        assert new_order2 == [
            "event_id",
            "event_name",
            "event_date",
        ], "Pair 2 should be reordered"
