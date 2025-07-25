"""
Schema Converter - JSON ↔ SQL conversion logic
"""

import json
import sqlite3
import re
import glob
from pathlib import Path
from typing import Dict, List, Any, Optional


class SchemaConverter:
    """Handles conversion between JSON schemas and SQL DDL"""
    
    def __init__(self):
        self.type_mapping = {
            "INTEGER": "INTEGER",
            "VARCHAR": "VARCHAR",
            "TEXT": "TEXT", 
            "DATETIME": "DATETIME",
            "DATE": "DATE",
            "BOOLEAN": "BOOLEAN",
            "JSON": "TEXT",  # SQLite stores JSON as TEXT
            "ENUM": "TEXT"   # SQLite stores ENUM as TEXT with CHECK constraint
        }

    def json_to_sql(self, schema: Dict[str, Any]) -> str:
        """Convert JSON schema to SQL DDL statements"""
        sql_statements = []
        
        # Add header comment
        db_info = schema.get("database", {})
        sql_statements.append(f"-- {db_info.get('description', 'Database Schema')}")
        sql_statements.append(f"-- Generated from JSON schema")
        sql_statements.append(f"-- Database: {db_info.get('name', 'unknown')}")
        sql_statements.append(f"-- Version: {db_info.get('version', '1.0.0')}")
        sql_statements.append("")

        # Create tables
        sql_statements.append("-- ===== TABLES =====")
        for table_name, table_info in schema.get("tables", {}).items():
            sql_statements.append("")
            sql_statements.append(self._generate_table_sql(table_name, table_info))

        # Create indexes
        sql_statements.append("\n-- ===== INDEXES =====")
        for table_name, table_info in schema.get("tables", {}).items():
            if "indexes" in table_info:
                for index in table_info["indexes"]:
                    sql_statements.append(self._generate_index_sql(table_name, index))

        # Insert seed data
        if "seed_data" in schema:
            sql_statements.append("\n-- ===== SEED DATA =====")
            for table_name, records in schema["seed_data"].items():
                for record in records:
                    sql_statements.append(self._generate_insert_sql(table_name, record))

        return "\n".join(sql_statements)

    def sql_to_json(self, db_path: str) -> Dict[str, Any]:
        """Extract JSON schema from SQLite database"""
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        
        try:
            schema = {
                "database": {
                    "name": Path(db_path).stem,
                    "type": "sqlite",
                    "version": "1.0.0",
                    "description": f"Schema extracted from {db_path}",
                    "extracted_at": self._get_current_timestamp()
                },
                "tables": {},
                "relationships": []
            }

            # Get all tables
            cursor = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """)
            tables = [row["name"] for row in cursor.fetchall()]

            # Extract each table schema
            for table_name in tables:
                table_schema = self._extract_table_schema(conn, table_name)
                schema["tables"][table_name] = table_schema

            # Extract relationships
            schema["relationships"] = self._extract_relationships(conn, tables)

            return schema

        finally:
            conn.close()

    def merge_schemas(self, schema_files: List[str]) -> Dict[str, Any]:
        """Merge multiple JSON schema files"""
        # Expand glob patterns
        all_files = []
        for pattern in schema_files:
            if "*" in pattern:
                all_files.extend(glob.glob(pattern))
            else:
                all_files.append(pattern)

        unified_schema = {
            "database": {
                "name": "merged_schema",
                "type": "sqlite",
                "version": "1.0.0",
                "description": "Merged from multiple schema files"
            },
            "tables": {},
            "relationships": [],
            "seed_data": {}
        }

        # Load and merge each schema
        for file_path in all_files:
            if not Path(file_path).exists():
                continue
                
            with open(file_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)

            # Merge tables (with conflict resolution)
            for table_name, table_def in schema.get("tables", {}).items():
                if table_name in unified_schema["tables"]:
                    # Handle conflict - prefer admin schemas
                    if "admin" in file_path.lower():
                        unified_schema["tables"][table_name] = table_def
                else:
                    unified_schema["tables"][table_name] = table_def

            # Merge relationships
            for rel in schema.get("relationships", []):
                if rel not in unified_schema["relationships"]:
                    unified_schema["relationships"].append(rel)

            # Merge seed data
            for table_name, data in schema.get("seed_data", {}).items():
                if table_name not in unified_schema["seed_data"]:
                    unified_schema["seed_data"][table_name] = []
                unified_schema["seed_data"][table_name].extend(data)

        return unified_schema

    def _generate_table_sql(self, table_name: str, table_info: Dict[str, Any]) -> str:
        """Generate CREATE TABLE SQL statement"""
        lines = []
        lines.append(f"-- {table_info.get('description', table_name)}")
        lines.append(f"CREATE TABLE IF NOT EXISTS {table_name} (")

        # Generate columns
        column_lines = []
        foreign_keys = []

        for col_name, col_info in table_info["columns"].items():
            column_line = self._generate_column_sql(col_name, col_info)
            column_lines.append(f"    {column_line}")

            # Collect foreign keys
            if "foreign_key" in col_info:
                fk = col_info["foreign_key"]
                fk_line = f"    FOREIGN KEY ({col_name}) REFERENCES {fk['table']}({fk['column']})"
                if "on_delete" in fk:
                    fk_line += f" ON DELETE {fk['on_delete']}"
                if "on_update" in fk:
                    fk_line += f" ON UPDATE {fk['on_update']}"
                foreign_keys.append(fk_line)

        # Combine columns and foreign keys
        all_lines = column_lines + foreign_keys
        lines.append(",\n".join(all_lines))
        lines.append(");")

        return "\n".join(lines)

    def _generate_column_sql(self, col_name: str, col_info: Dict[str, Any]) -> str:
        """Generate column definition SQL"""
        parts = [col_name]

        # Type
        col_type = col_info["type"]
        if col_type.startswith("VARCHAR") or col_type.startswith("CHAR"):
            parts.append(col_type)
        else:
            parts.append(self.type_mapping.get(col_type, "TEXT"))

        # Primary key
        if col_info.get("primary_key"):
            parts.append("PRIMARY KEY")
            if col_info.get("auto_increment"):
                parts.append("AUTOINCREMENT")

        # Not null
        if col_info.get("nullable") == False:
            parts.append("NOT NULL")

        # Unique
        if col_info.get("unique"):
            parts.append("UNIQUE")

        # Default
        if "default" in col_info:
            default_val = col_info["default"]
            if default_val == "CURRENT_TIMESTAMP":
                parts.append(f"DEFAULT {default_val}")
            elif isinstance(default_val, bool):
                parts.append(f"DEFAULT {1 if default_val else 0}")
            elif isinstance(default_val, str):
                parts.append(f"DEFAULT '{default_val}'")
            else:
                parts.append(f"DEFAULT {default_val}")

        # CHECK constraint for ENUM
        if col_info["type"] == "ENUM" and "values" in col_info:
            values_str = ", ".join(f"'{v}'" for v in col_info["values"])
            parts.append(f"CHECK ({col_name} IN ({values_str}))")

        return " ".join(parts)

    def _generate_index_sql(self, table_name: str, index_info: Dict[str, Any]) -> str:
        """Generate CREATE INDEX SQL statement"""
        unique = "UNIQUE " if index_info.get("unique") else ""
        columns = ", ".join(index_info["columns"])
        return f"CREATE {unique}INDEX IF NOT EXISTS {index_info['name']} ON {table_name} ({columns});"

    def _generate_insert_sql(self, table_name: str, record: Dict[str, Any]) -> str:
        """Generate INSERT SQL statement"""
        columns = ", ".join(record.keys())
        values = []
        for value in record.values():
            if isinstance(value, str):
                values.append(f"'{value}'")
            elif value is None:
                values.append("NULL")
            else:
                values.append(str(value))
        values_str = ", ".join(values)
        return f"INSERT OR IGNORE INTO {table_name} ({columns}) VALUES ({values_str});"

    def _extract_table_schema(self, conn: sqlite3.Connection, table_name: str) -> Dict[str, Any]:
        """Extract schema for a single table"""
        # Get table info
        cursor = conn.execute(f"PRAGMA table_info({table_name})")
        columns_info = cursor.fetchall()

        # Get CREATE TABLE statement for CHECK constraints
        cursor = conn.execute("""
            SELECT sql FROM sqlite_master 
            WHERE type='table' AND name=?
        """, (table_name,))
        create_sql = cursor.fetchone()
        create_sql_text = create_sql["sql"] if create_sql else ""

        table_schema = {
            "description": f"{table_name} table",
            "columns": {}
        }

        for col in columns_info:
            col_name = col["name"]
            col_type_info = self._parse_sql_type(col["type"])

            column_def = {
                **col_type_info,
                "nullable": not bool(col["notnull"]),
                "primary_key": bool(col["pk"])
            }

            # Handle default values
            if col["dflt_value"] is not None:
                default_val = col["dflt_value"]
                if default_val.startswith("'") and default_val.endswith("'"):
                    default_val = default_val[1:-1]
                elif default_val.upper() == "CURRENT_TIMESTAMP":
                    default_val = "CURRENT_TIMESTAMP"
                elif default_val.isdigit():
                    default_val = int(default_val)
                column_def["default"] = default_val

            # Extract ENUM values from CHECK constraints
            enum_values = self._extract_enum_values(create_sql_text, col_name)
            if enum_values:
                column_def["type"] = "ENUM"
                column_def["values"] = enum_values

            table_schema["columns"][col_name] = column_def

        # Get indexes
        indexes = self._extract_table_indexes(conn, table_name)
        if indexes:
            table_schema["indexes"] = indexes

        return table_schema

    def _extract_table_indexes(self, conn: sqlite3.Connection, table_name: str) -> List[Dict[str, Any]]:
        """Extract indexes for a table"""
        cursor = conn.execute(f"PRAGMA index_list({table_name})")
        index_list = cursor.fetchall()

        indexes = []
        for idx in index_list:
            if not idx["name"].startswith('sqlite_'):  # Skip system indexes
                # Get index columns
                cursor2 = conn.execute(f"PRAGMA index_info({idx['name']})")
                columns = [col["name"] for col in cursor2.fetchall()]

                indexes.append({
                    "name": idx["name"],
                    "unique": bool(idx["unique"]),
                    "columns": columns
                })

        return indexes

    def _extract_relationships(self, conn: sqlite3.Connection, tables: List[str]) -> List[Dict[str, Any]]:
        """Extract foreign key relationships"""
        relationships = []

        for table_name in tables:
            cursor = conn.execute(f"PRAGMA foreign_key_list({table_name})")
            fk_list = cursor.fetchall()

            for fk in fk_list:
                relationships.append({
                    "name": f"{table_name}_to_{fk['table']}",
                    "from": f"{table_name}.{fk['from']}",
                    "to": f"{fk['table']}.{fk['to']}",
                    "type": "many-to-one",
                    "on_delete": fk["on_delete"],
                    "on_update": fk["on_update"]
                })

        return relationships

    def _parse_sql_type(self, sql_type: str) -> Dict[str, Any]:
        """Parse SQL type to JSON format"""
        sql_type = sql_type.upper().strip()

        # VARCHAR(255) pattern
        if match := re.match(r'VARCHAR\((\d+)\)', sql_type):
            return {
                "type": f"VARCHAR({match.group(1)})",
                "max_length": int(match.group(1))
            }

        # Basic type mapping
        type_mapping = {
            "INTEGER": {"type": "INTEGER"},
            "TEXT": {"type": "TEXT"},
            "REAL": {"type": "REAL"},
            "BLOB": {"type": "BLOB"},
            "DATETIME": {"type": "DATETIME"},
            "DATE": {"type": "DATE"},
            "BOOLEAN": {"type": "BOOLEAN"}
        }

        return type_mapping.get(sql_type, {"type": "TEXT"})

    def _extract_enum_values(self, sql: str, column_name: str) -> Optional[List[str]]:
        """Extract ENUM values from CHECK constraint"""
        pattern = rf"CHECK\s*\(\s*{column_name}\s+IN\s*\(\s*([^)]+)\s*\)\s*\)"
        match = re.search(pattern, sql, re.IGNORECASE)

        if match:
            values_str = match.group(1)
            values = re.findall(r"'([^']+)'", values_str)
            return values

        return None

    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format"""
        from datetime import datetime
        return datetime.now().isoformat()