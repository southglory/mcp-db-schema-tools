"""
Tests for SchemaConverter
"""

import pytest
import json
import tempfile
from pathlib import Path
from src.mcp_db_schema_tools.schema_converter import SchemaConverter


@pytest.fixture
def converter():
    return SchemaConverter()


@pytest.fixture
def sample_schema():
    return {
        "database": {
            "name": "test_db",
            "type": "sqlite",
            "version": "1.0.0",
            "description": "Test database"
        },
        "tables": {
            "users": {
                "description": "User information",
                "columns": {
                    "id": {
                        "type": "INTEGER",
                        "primary_key": True,
                        "auto_increment": True
                    },
                    "email": {
                        "type": "VARCHAR(255)",
                        "unique": True,
                        "nullable": False
                    },
                    "status": {
                        "type": "ENUM",
                        "values": ["active", "inactive"],
                        "default": "active"
                    }
                },
                "indexes": [
                    {
                        "name": "idx_users_email",
                        "columns": ["email"],
                        "unique": True
                    }
                ]
            }
        },
        "seed_data": {
            "users": [
                {
                    "email": "test@example.com",
                    "status": "active"
                }
            ]
        }
    }


def test_json_to_sql_basic(converter, sample_schema):
    """Test basic JSON to SQL conversion"""
    sql = converter.json_to_sql(sample_schema)
    
    assert "CREATE TABLE IF NOT EXISTS users" in sql
    assert "id INTEGER PRIMARY KEY AUTOINCREMENT" in sql
    assert "email VARCHAR(255) NOT NULL UNIQUE" in sql
    assert "CHECK (status IN ('active', 'inactive'))" in sql
    assert "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email" in sql
    assert "INSERT OR IGNORE INTO users" in sql


def test_merge_schemas_basic(converter, tmp_path):
    """Test basic schema merging"""
    # Create test schema files
    schema1 = {
        "tables": {
            "users": {"columns": {"id": {"type": "INTEGER"}}},
            "posts": {"columns": {"id": {"type": "INTEGER"}}}
        }
    }
    
    schema2 = {
        "tables": {
            "comments": {"columns": {"id": {"type": "INTEGER"}}}
        }
    }
    
    file1 = tmp_path / "schema1.json"
    file2 = tmp_path / "schema2.json"
    
    file1.write_text(json.dumps(schema1))
    file2.write_text(json.dumps(schema2))
    
    merged = converter.merge_schemas([str(file1), str(file2)])
    
    assert "users" in merged["tables"]
    assert "posts" in merged["tables"] 
    assert "comments" in merged["tables"]


def test_column_sql_generation(converter):
    """Test column SQL generation"""
    # Test primary key with auto increment
    col_info = {"type": "INTEGER", "primary_key": True, "auto_increment": True}
    sql = converter._generate_column_sql("id", col_info)
    assert sql == "id INTEGER PRIMARY KEY AUTOINCREMENT"
    
    # Test varchar with not null and unique
    col_info = {"type": "VARCHAR(255)", "nullable": False, "unique": True}
    sql = converter._generate_column_sql("email", col_info)
    assert sql == "email VARCHAR(255) NOT NULL UNIQUE"
    
    # Test enum with check constraint
    col_info = {"type": "ENUM", "values": ["active", "inactive"], "default": "active"}
    sql = converter._generate_column_sql("status", col_info)
    assert "CHECK (status IN ('active', 'inactive'))" in sql
    assert "DEFAULT 'active'" in sql