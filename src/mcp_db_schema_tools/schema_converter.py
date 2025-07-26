"""
Schema Converter - JSON ↔ SQL conversion logic
"""

import json
import sqlite3
import re
import glob
from pathlib import Path
from typing import Dict, List, Any, Optional

try:
    import psycopg2
    POSTGRESQL_AVAILABLE = True
except ImportError:
    POSTGRESQL_AVAILABLE = False

try:
    import mysql.connector
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False


class SchemaConverter:
    """Handles conversion between JSON schemas and SQL DDL"""
    
    def __init__(self):
        # Database-specific type mappings
        self.db_type_mappings = {
            "sqlite": {
                "INTEGER": "INTEGER",
                "VARCHAR": "VARCHAR",
                "TEXT": "TEXT", 
                "DATETIME": "DATETIME",
                "DATE": "DATE",
                "BOOLEAN": "BOOLEAN",
                "JSON": "TEXT",  # SQLite stores JSON as TEXT
                "ENUM": "TEXT"   # SQLite stores ENUM as TEXT with CHECK constraint
            },
            "postgresql": {
                "INTEGER": "INTEGER",
                "VARCHAR": "VARCHAR",
                "TEXT": "TEXT",
                "DATETIME": "TIMESTAMP",
                "DATE": "DATE", 
                "BOOLEAN": "BOOLEAN",
                "JSON": "JSONB",  # PostgreSQL native JSON type
                "ENUM": "TEXT"    # Can use CREATE TYPE for enums
            },
            "mysql": {
                "INTEGER": "INT",
                "VARCHAR": "VARCHAR",
                "TEXT": "TEXT",
                "DATETIME": "DATETIME", 
                "DATE": "DATE",
                "BOOLEAN": "TINYINT(1)",  # MySQL uses TINYINT for BOOLEAN
                "JSON": "JSON",           # MySQL 5.7+ native JSON type
                "ENUM": "ENUM"            # MySQL native ENUM type
            }
        }
        
        # Legacy support - defaults to SQLite
        self.type_mapping = self.db_type_mappings["sqlite"]

    def json_to_sql(self, schema: Dict[str, Any], db_type: str = "sqlite") -> str:
        """Convert JSON schema to SQL DDL statements"""
        # Set type mapping for the target database
        if db_type not in self.db_type_mappings:
            raise ValueError(f"Unsupported database type: {db_type}. Supported: {list(self.db_type_mappings.keys())}")
        
        self.current_db_type = db_type
        self.type_mapping = self.db_type_mappings[db_type]
        
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
            sql_statements.append(self._generate_table_sql(table_name, table_info, db_type))

        # Create indexes
        sql_statements.append("\n-- ===== INDEXES =====")
        for table_name, table_info in schema.get("tables", {}).items():
            if "indexes" in table_info:
                for index in table_info["indexes"]:
                    sql_statements.append(self._generate_index_sql(table_name, index))

        # Insert seed data
        if "seed_data" in schema:
            sql_statements.append("\n-- ===== SEED DATA =====")
            processed_seed_data = self._process_seed_data(schema["seed_data"], schema.get("tables", {}))
            for table_name, records in processed_seed_data.items():
                for record in records:
                    sql_statements.append(self._generate_insert_sql(table_name, record))

        return "\n".join(sql_statements)

    def sql_to_json(self, db_path: str, db_type: str = "sqlite") -> Dict[str, Any]:
        """Extract JSON schema from database with multi-DB support"""
        if db_type == "sqlite":
            return self._extract_sqlite_schema(db_path)
        elif db_type == "postgresql":
            return self._extract_postgresql_schema(db_path)
        elif db_type == "mysql":
            return self._extract_mysql_schema(db_path)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    def _extract_sqlite_schema(self, db_path: str) -> Dict[str, Any]:
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
                table_schema = self._extract_sqlite_table_schema(conn, table_name)
                schema["tables"][table_name] = table_schema

            # Extract relationships
            schema["relationships"] = self._extract_sqlite_relationships(conn, tables)

            return schema

        finally:
            conn.close()
    
    def _extract_postgresql_schema(self, connection_string: str) -> Dict[str, Any]:
        """Extract JSON schema from PostgreSQL database"""
        if not POSTGRESQL_AVAILABLE:
            raise ImportError("psycopg2 is required for PostgreSQL support. Install with: pip install psycopg2-binary")
        
        conn = psycopg2.connect(connection_string)
        
        try:
            schema = {
                "database": {
                    "name": "postgresql_db",
                    "type": "postgresql",
                    "version": "1.0.0",
                    "description": f"Schema extracted from PostgreSQL",
                    "extracted_at": self._get_current_timestamp()
                },
                "tables": {},
                "relationships": []
            }

            # Get all tables
            cursor = conn.cursor()
            cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' 
                ORDER BY table_name
            """)
            tables = [row[0] for row in cursor.fetchall()]

            # Extract each table schema
            for table_name in tables:
                table_schema = self._extract_postgresql_table_schema(conn, table_name)
                schema["tables"][table_name] = table_schema

            # Extract relationships
            schema["relationships"] = self._extract_postgresql_relationships(conn, tables)

            return schema

        finally:
            conn.close()
    
    def _extract_mysql_schema(self, connection_config: str) -> Dict[str, Any]:
        """Extract JSON schema from MySQL database"""
        if not MYSQL_AVAILABLE:
            raise ImportError("mysql-connector-python is required for MySQL support. Install with: pip install mysql-connector-python")
        
        # Parse connection string
        import re
        match = re.match(r'mysql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)', connection_config)
        if not match:
            raise ValueError("Invalid MySQL connection string format. Expected: mysql://user:password@host:port/database")
        
        user, password, host, port, database = match.groups()
        
        conn = mysql.connector.connect(
            host=host,
            port=int(port),
            user=user,
            password=password,
            database=database
        )
        
        try:
            schema = {
                "database": {
                    "name": database,
                    "type": "mysql",
                    "version": "1.0.0",
                    "description": f"Schema extracted from MySQL database: {database}",
                    "extracted_at": self._get_current_timestamp()
                },
                "tables": {},
                "relationships": []
            }

            # Get all tables
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]

            # Extract each table schema
            for table_name in tables:
                table_schema = self._extract_mysql_table_schema(conn, table_name)
                schema["tables"][table_name] = table_schema

            # Extract relationships
            schema["relationships"] = self._extract_mysql_relationships(conn, tables)

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

    def _generate_table_sql(self, table_name: str, table_info: Dict[str, Any], db_type: str = "sqlite") -> str:
        """Generate CREATE TABLE SQL statement"""
        lines = []
        lines.append(f"-- {table_info.get('description', table_name)}")
        
        # Database-specific CREATE TABLE syntax
        if db_type == "mysql":
            lines.append(f"CREATE TABLE IF NOT EXISTS {table_name} (")
        else:  # sqlite, postgresql
            lines.append(f"CREATE TABLE IF NOT EXISTS {table_name} (")

        # Generate columns
        column_lines = []
        foreign_keys = []

        for col_name, col_info in table_info["columns"].items():
            column_line = self._generate_column_sql(col_name, col_info, db_type)
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
        
        # Database-specific table options
        if db_type == "mysql":
            lines.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;")
        else:  # sqlite, postgresql
            lines.append(");")

        return "\n".join(lines)

    def _generate_column_sql(self, col_name: str, col_info: Dict[str, Any], db_type: str = "sqlite") -> str:
        """Generate column definition SQL"""
        parts = [col_name]

        # Type
        col_type = col_info["type"]
        if col_type.startswith("VARCHAR") or col_type.startswith("CHAR"):
            parts.append(col_type)
        else:
            parts.append(self.type_mapping.get(col_type, "TEXT"))

        # Primary key and auto increment - database specific
        if col_info.get("primary_key"):
            if db_type == "postgresql" and col_info.get("auto_increment"):
                # PostgreSQL uses SERIAL for auto-increment
                if "INTEGER" in parts[1].upper():
                    parts[1] = "SERIAL"
                parts.append("PRIMARY KEY")
            elif db_type == "mysql" and col_info.get("auto_increment"):
                parts.append("PRIMARY KEY")
                parts.append("AUTO_INCREMENT")
            else:  # sqlite
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

        # ENUM handling - database specific
        if col_info["type"] == "ENUM" and "values" in col_info:
            if db_type == "mysql":
                # MySQL native ENUM support
                values_str = ", ".join(f"'{v}'" for v in col_info["values"])
                parts[1] = f"ENUM({values_str})"
            else:  # sqlite, postgresql
                # Use CHECK constraint
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

    def _extract_sqlite_table_schema(self, conn: sqlite3.Connection, table_name: str) -> Dict[str, Any]:
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
        indexes = self._extract_sqlite_table_indexes(conn, table_name)
        if indexes:
            table_schema["indexes"] = indexes

        return table_schema

    def _extract_sqlite_table_indexes(self, conn: sqlite3.Connection, table_name: str) -> List[Dict[str, Any]]:
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

    def _extract_sqlite_relationships(self, conn: sqlite3.Connection, tables: List[str]) -> List[Dict[str, Any]]:
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
    
    def _extract_postgresql_table_schema(self, conn, table_name: str) -> Dict[str, Any]:
        """Extract schema for a PostgreSQL table"""
        cursor = conn.cursor()
        
        # Get column information
        cursor.execute("""
            SELECT column_name, data_type, is_nullable, column_default, 
                   character_maximum_length, numeric_precision, numeric_scale
            FROM information_schema.columns 
            WHERE table_name = %s AND table_schema = 'public'
            ORDER BY ordinal_position
        """, (table_name,))
        columns_info = cursor.fetchall()
        
        # Get primary key info
        cursor.execute("""
            SELECT column_name
            FROM information_schema.key_column_usage k
            JOIN information_schema.table_constraints t ON k.constraint_name = t.constraint_name
            WHERE t.table_name = %s AND t.constraint_type = 'PRIMARY KEY'
        """, (table_name,))
        primary_keys = [row[0] for row in cursor.fetchall()]
        
        table_schema = {
            "description": f"{table_name} table",
            "columns": {}
        }
        
        for col_info in columns_info:
            col_name, data_type, is_nullable, default_val, max_length, precision, scale = col_info
            
            # Map PostgreSQL types to our standard types
            if data_type == 'character varying':
                col_type = f"VARCHAR({max_length})" if max_length else "VARCHAR(255)"
            elif data_type == 'integer':
                col_type = "INTEGER"
            elif data_type == 'text':
                col_type = "TEXT"
            elif data_type == 'boolean':
                col_type = "BOOLEAN"
            elif data_type == 'timestamp without time zone':
                col_type = "DATETIME"
            elif data_type == 'date':
                col_type = "DATE"
            elif data_type == 'jsonb':
                col_type = "JSON"
            else:
                col_type = data_type.upper()
            
            column_def = {
                "type": col_type,
                "nullable": is_nullable == 'YES',
                "primary_key": col_name in primary_keys
            }
            
            if max_length and data_type == 'character varying':
                column_def["max_length"] = max_length
            
            if default_val:
                if default_val.startswith("'") and default_val.endswith("'"):
                    column_def["default"] = default_val[1:-1]
                elif default_val == "CURRENT_TIMESTAMP":
                    column_def["default"] = "CURRENT_TIMESTAMP"
                elif default_val.startswith('nextval'):
                    column_def["auto_increment"] = True
                else:
                    column_def["default"] = default_val
            
            table_schema["columns"][col_name] = column_def
        
        # Get indexes
        indexes = self._extract_postgresql_indexes(conn, table_name)
        if indexes:
            table_schema["indexes"] = indexes
        
        return table_schema
    
    def _extract_postgresql_indexes(self, conn, table_name: str) -> List[Dict[str, Any]]:
        """Extract indexes for a PostgreSQL table"""
        cursor = conn.cursor()
        cursor.execute("""
            SELECT indexname, indexdef
            FROM pg_indexes 
            WHERE tablename = %s AND schemaname = 'public'
        """, (table_name,))
        
        indexes = []
        for row in cursor.fetchall():
            index_name, index_def = row
            # Skip primary key indexes
            if '_pkey' not in index_name:
                unique = 'UNIQUE' in index_def.upper()
                # Extract column names from index definition (simplified)
                import re
                cols_match = re.search(r'\(([^)]+)\)', index_def)
                columns = [cols_match.group(1)] if cols_match else [table_name + "_id"]
                
                indexes.append({
                    "name": index_name,
                    "unique": unique,
                    "columns": columns
                })
        
        return indexes
    
    def _extract_postgresql_relationships(self, conn, tables: List[str]) -> List[Dict[str, Any]]:
        """Extract foreign key relationships for PostgreSQL"""
        cursor = conn.cursor()
        relationships = []
        
        for table_name in tables:
            cursor.execute("""
                SELECT 
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name,
                    rc.update_rule,
                    rc.delete_rule
                FROM information_schema.key_column_usage kcu
                JOIN information_schema.referential_constraints rc ON kcu.constraint_name = rc.constraint_name
                JOIN information_schema.constraint_column_usage ccu ON rc.unique_constraint_name = ccu.constraint_name
                WHERE kcu.table_name = %s
            """, (table_name,))
            
            for row in cursor.fetchall():
                col_name, ref_table, ref_col, update_rule, delete_rule = row
                relationships.append({
                    "name": f"{table_name}_to_{ref_table}",
                    "from": f"{table_name}.{col_name}",
                    "to": f"{ref_table}.{ref_col}",
                    "type": "many-to-one",
                    "on_delete": delete_rule.upper(),
                    "on_update": update_rule.upper()
                })
        
        return relationships
    
    def _extract_mysql_table_schema(self, conn, table_name: str) -> Dict[str, Any]:
        """Extract schema for a MySQL table"""
        cursor = conn.cursor()
        
        # Get column information
        cursor.execute(f"DESCRIBE {table_name}")
        columns_info = cursor.fetchall()
        
        table_schema = {
            "description": f"{table_name} table",
            "columns": {}
        }
        
        for col_info in columns_info:
            field, type_str, null, key, default, extra = col_info
            
            # Parse MySQL type to our standard format
            col_type = self._parse_mysql_type(type_str)
            
            column_def = {
                "type": col_type["type"],
                "nullable": null == 'YES',
                "primary_key": key == 'PRI'
            }
            
            if "max_length" in col_type:
                column_def["max_length"] = col_type["max_length"]
            
            if "values" in col_type:
                column_def["values"] = col_type["values"]
            
            if default is not None:
                if default == "CURRENT_TIMESTAMP":
                    column_def["default"] = "CURRENT_TIMESTAMP"
                else:
                    column_def["default"] = default
            
            if extra == 'auto_increment':
                column_def["auto_increment"] = True
            
            table_schema["columns"][field] = column_def
        
        # Get indexes
        indexes = self._extract_mysql_indexes(conn, table_name)
        if indexes:
            table_schema["indexes"] = indexes
        
        return table_schema
    
    def _extract_mysql_indexes(self, conn, table_name: str) -> List[Dict[str, Any]]:
        """Extract indexes for a MySQL table"""
        cursor = conn.cursor()
        cursor.execute(f"SHOW INDEX FROM {table_name}")
        
        indexes = {}
        for row in cursor.fetchall():
            table, non_unique, key_name, seq_in_index, column_name, collation, cardinality, sub_part, packed, null, index_type, comment, index_comment = row
            
            # Skip primary key
            if key_name == 'PRIMARY':
                continue
            
            if key_name not in indexes:
                indexes[key_name] = {
                    "name": key_name,
                    "unique": non_unique == 0,
                    "columns": []
                }
            
            indexes[key_name]["columns"].append(column_name)
        
        return list(indexes.values())
    
    def _extract_mysql_relationships(self, conn, tables: List[str]) -> List[Dict[str, Any]]:
        """Extract foreign key relationships for MySQL"""
        cursor = conn.cursor()
        relationships = []
        
        for table_name in tables:
            cursor.execute("""
                SELECT 
                    COLUMN_NAME,
                    REFERENCED_TABLE_NAME,
                    REFERENCED_COLUMN_NAME,
                    UPDATE_RULE,
                    DELETE_RULE
                FROM information_schema.KEY_COLUMN_USAGE 
                WHERE TABLE_NAME = %s 
                AND REFERENCED_TABLE_NAME IS NOT NULL
            """, (table_name,))
            
            for row in cursor.fetchall():
                col_name, ref_table, ref_col, update_rule, delete_rule = row
                relationships.append({
                    "name": f"{table_name}_to_{ref_table}",
                    "from": f"{table_name}.{col_name}",
                    "to": f"{ref_table}.{ref_col}",
                    "type": "many-to-one",
                    "on_delete": delete_rule,
                    "on_update": update_rule
                })
        
        return relationships
    
    def _parse_mysql_type(self, type_str: str) -> Dict[str, Any]:
        """Parse MySQL type string to our standard format"""
        import re
        
        # Handle ENUM
        if type_str.startswith('enum'):
            values_match = re.search(r"enum\(([^)]+)\)", type_str)
            if values_match:
                values_str = values_match.group(1)
                values = [v.strip("'\"") for v in values_str.split(',')]
                return {"type": "ENUM", "values": values}
        
        # Handle VARCHAR
        if 'varchar' in type_str:
            length_match = re.search(r'varchar\((\d+)\)', type_str)
            if length_match:
                return {"type": f"VARCHAR({length_match.group(1)})", "max_length": int(length_match.group(1))}
        
        # Handle other types
        type_mapping = {
            'int': "INTEGER",
            'tinyint(1)': "BOOLEAN",
            'text': "TEXT",
            'datetime': "DATETIME",
            'date': "DATE",
            'json': "JSON"
        }
        
        for mysql_type, std_type in type_mapping.items():
            if mysql_type in type_str.lower():
                return {"type": std_type}
        
        return {"type": type_str.upper()}

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
    
    def _process_seed_data(self, seed_data: Dict[str, List[Dict[str, Any]]], tables: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Process seed data and automatically generate IDs where needed"""
        processed_data = {}
        
        for table_name, records in seed_data.items():
            if table_name not in tables:
                # Table doesn't exist, skip
                processed_data[table_name] = records
                continue
                
            table_columns = tables[table_name].get("columns", {})
            primary_key_column = None
            
            # Find primary key column
            for col_name, col_info in table_columns.items():
                if col_info.get("primary_key"):
                    primary_key_column = col_name
                    break
            
            processed_records = []
            
            for i, record in enumerate(records):
                new_record = record.copy()
                
                # Auto-generate ID if missing and primary key is INTEGER
                if (primary_key_column and 
                    primary_key_column not in new_record and
                    table_columns[primary_key_column].get("type") == "INTEGER"):
                    new_record[primary_key_column] = i + 1
                
                processed_records.append(new_record)
            
            processed_data[table_name] = processed_records
        
        return processed_data
    
    def create_database_with_schema(self, schema: Dict[str, Any], db_path: str, db_type: str = "sqlite", include_seed_data: bool = False) -> Dict[str, Any]:
        """Create database from schema with multi-DB support"""
        if db_type == "sqlite":
            return self._create_sqlite_database(schema, db_path, include_seed_data)
        elif db_type == "postgresql":
            return self._create_postgresql_database(schema, db_path, include_seed_data)
        elif db_type == "mysql":
            return self._create_mysql_database(schema, db_path, include_seed_data)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    def _create_sqlite_database(self, schema: Dict[str, Any], db_path: str, include_seed_data: bool = False) -> Dict[str, Any]:
        """Create SQLite database"""
        # Generate SQL DDL
        sql_statements = self.json_to_sql(schema, "sqlite")
        
        # Create database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            # Execute DDL statements with better error handling
            statements = []
            current_statement = ""
            
            for line in sql_statements.split('\n'):
                line = line.strip()
                if line and not line.startswith('--'):
                    current_statement += line + "\n"
                    if line.endswith(';'):
                        statements.append(current_statement.strip())
                        current_statement = ""
            
            # Add any remaining statement
            if current_statement.strip():
                statements.append(current_statement.strip())
            
            # Execute each statement individually with error handling
            for i, statement in enumerate(statements):
                if statement:
                    try:
                        cursor.execute(statement)
                        conn.commit()  # Commit after each statement
                    except Exception as e:
                        print(f"Error executing statement {i+1}: {statement[:100]}...")
                        print(f"Error: {e}")
                        raise
            
            # Insert seed data if requested
            seed_count = 0
            if include_seed_data and "seed_data" in schema:
                try:
                    for table_name, records in schema["seed_data"].items():
                        # Check if table exists first
                        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                        if not cursor.fetchone():
                            print(f"Warning: Table {table_name} not found, skipping seed data")
                            continue
                            
                        for record in records:
                            columns = ", ".join(record.keys())
                            placeholders = ", ".join(["?" for _ in record])
                            values = list(record.values())
                            
                            insert_sql = f"INSERT OR IGNORE INTO {table_name} ({columns}) VALUES ({placeholders})"
                            cursor.execute(insert_sql, values)
                            seed_count += 1
                    conn.commit()
                except Exception as seed_error:
                    print(f"Warning: Seed data insertion failed: {seed_error}")
                    # Continue without seed data
            
            # Get table info
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = [row[0] for row in cursor.fetchall() if row[0] != 'sqlite_sequence']
            
            return {
                "success": True,
                "db_type": "sqlite",
                "db_path": db_path,
                "tables_created": len(tables),
                "seed_records": seed_count,
                "tables": tables
            }
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def _create_postgresql_database(self, schema: Dict[str, Any], connection_string: str, include_seed_data: bool = False) -> Dict[str, Any]:
        """Create PostgreSQL database"""
        if not POSTGRESQL_AVAILABLE:
            raise ImportError("psycopg2 is required for PostgreSQL support. Install with: pip install psycopg2-binary")
        
        # Generate SQL DDL
        sql_statements = self.json_to_sql(schema, "postgresql")
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()
        
        try:
            # Execute DDL statements
            for statement in sql_statements.split(';'):
                statement = statement.strip()
                if statement and not statement.startswith('--'):
                    cursor.execute(statement)
            
            # Insert seed data if requested
            seed_count = 0
            if include_seed_data and "seed_data" in schema:
                for table_name, records in schema["seed_data"].items():
                    for record in records:
                        columns = ", ".join(record.keys())
                        placeholders = ", ".join(["%s" for _ in record])
                        values = list(record.values())
                        
                        insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
                        cursor.execute(insert_sql, values)
                        seed_count += 1
            
            conn.commit()
            
            # Get table info
            cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' ORDER BY table_name
            """)
            tables = [row[0] for row in cursor.fetchall()]
            
            return {
                "success": True,
                "db_type": "postgresql",
                "connection_string": connection_string,
                "tables_created": len(tables),
                "seed_records": seed_count,
                "tables": tables
            }
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def _create_mysql_database(self, schema: Dict[str, Any], connection_config: str, include_seed_data: bool = False) -> Dict[str, Any]:
        """Create MySQL database"""
        if not MYSQL_AVAILABLE:
            raise ImportError("mysql-connector-python is required for MySQL support. Install with: pip install mysql-connector-python")
        
        # Parse connection string to config dict
        # Format: mysql://user:password@host:port/database
        import re
        match = re.match(r'mysql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)', connection_config)
        if not match:
            raise ValueError("Invalid MySQL connection string format. Expected: mysql://user:password@host:port/database")
        
        user, password, host, port, database = match.groups()
        
        # Generate SQL DDL
        sql_statements = self.json_to_sql(schema, "mysql")
        
        # Connect to MySQL
        conn = mysql.connector.connect(
            host=host,
            port=int(port),
            user=user,
            password=password,
            database=database,
            autocommit=False
        )
        cursor = conn.cursor()
        
        try:
            # Execute DDL statements
            for statement in sql_statements.split(';'):
                statement = statement.strip()
                if statement and not statement.startswith('--'):
                    cursor.execute(statement)
            
            # Insert seed data if requested
            seed_count = 0
            if include_seed_data and "seed_data" in schema:
                for table_name, records in schema["seed_data"].items():
                    for record in records:
                        columns = ", ".join(record.keys())
                        placeholders = ", ".join(["%s" for _ in record])
                        values = list(record.values())
                        
                        insert_sql = f"INSERT IGNORE INTO {table_name} ({columns}) VALUES ({placeholders})"
                        cursor.execute(insert_sql, values)
                        seed_count += 1
            
            conn.commit()
            
            # Get table info
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
            
            return {
                "success": True,
                "db_type": "mysql",
                "connection_config": f"mysql://{user}:***@{host}:{port}/{database}",
                "tables_created": len(tables),
                "seed_records": seed_count,
                "tables": tables
            }
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def compare_with_backend_models(self, db_schema: Dict[str, Any], model_paths: List[str]) -> Dict[str, Any]:
        """Compare database schema with backend models"""
        comparison_result = {
            "missing_tables": [],
            "missing_columns": {},
            "type_mismatches": {},
            "missing_indexes": {},
            "suggestions": []
        }
        
        # This would analyze SQLAlchemy models and compare with DB schema
        # For now, we'll provide a basic structure that can be extended
        
        try:
            import ast
            import os
            
            # Extract model information from Python files
            model_info = {}
            
            for model_path in model_paths:
                if os.path.exists(model_path):
                    with open(model_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    # Basic AST parsing to extract class definitions
                    try:
                        tree = ast.parse(content)
                        for node in ast.walk(tree):
                            if isinstance(node, ast.ClassDef):
                                # This is a simplified extraction
                                # In a full implementation, we'd parse SQLAlchemy Column definitions
                                class_name = node.name
                                if hasattr(node, 'bases') and any('Base' in str(base) for base in node.bases):
                                    model_info[class_name] = {
                                        "file": model_path,
                                        "fields": []  # Would extract Column definitions here
                                    }
                    except:
                        pass
            
            # Compare with database schema
            db_tables = set(db_schema.get("tables", {}).keys())
            model_tables = set(model_info.keys())
            
            comparison_result["missing_tables"] = list(model_tables - db_tables)
            comparison_result["extra_tables"] = list(db_tables - model_tables)
            
            if comparison_result["missing_tables"]:
                comparison_result["suggestions"].append(
                    f"Consider running database migrations to add missing tables: {', '.join(comparison_result['missing_tables'])}"
                )
            
            if comparison_result["extra_tables"]:
                comparison_result["suggestions"].append(
                    f"Database contains extra tables not in models: {', '.join(comparison_result['extra_tables'])}"
                )
            
        except ImportError:
            comparison_result["suggestions"].append("Install required packages for model comparison (ast)")
        except Exception as e:
            comparison_result["suggestions"].append(f"Error during model comparison: {str(e)}")
        
        return comparison_result

    def generate_from_text(self, business_requirements: str, database_name: str = "generated_db") -> Dict[str, Any]:
        """Generate JSON database schema from business requirements text"""
        import re
        from datetime import datetime
        
        # Initialize schema structure
        schema = {
            "database": {
                "name": database_name,
                "type": "sqlite",
                "version": "1.0.0",
                "description": f"Generated from business requirements",
                "generated_at": datetime.now().isoformat()
            },
            "tables": {},
            "relationships": []
        }
        
        # Parse text to extract entities and relationships
        entities = self._extract_entities(business_requirements)
        relationships = self._extract_relationships_from_text(business_requirements, entities)
        
        # Create tables from entities
        for entity in entities:
            table_name = self._entity_to_table_name(entity["name"])
            schema["tables"][table_name] = self._generate_table_schema(entity)
        
        # Add relationship tables for many-to-many relationships
        for rel in relationships:
            if rel["type"] == "many-to-many":
                junction_table = self._create_junction_table(rel)
                schema["tables"][junction_table["name"]] = junction_table["schema"]
                
                # Add two many-to-one relationships instead
                schema["relationships"].extend(junction_table["relationships"])
            else:
                schema["relationships"].append(rel)
        
        return schema
    
    def _extract_entities(self, text: str) -> List[Dict[str, Any]]:
        """Extract entities (tables) from business requirements text"""
        entities = []
        
        # Common entity patterns in Korean and English
        entity_patterns = [
            r'사용자|유저|user[s]?',
            r'그룹|group[s]?', 
            r'스터디\s*그룹|study\s*group[s]?',
            r'스터디\s*룸|룸|study\s*room[s]?',
            r'강의|lecture[s]?',
            r'평가|evaluation[s]?',
            r'좋아요|like[s]?',
            r'토큰|token[s]?',
            r'리프레시\s*토큰|refresh\s*token[s]?',
            r'투표|vote[s]?',
            r'댓글|comment[s]?',
            r'신고|report[s]?',
            r'관리자|admin[s]?',
            r'설정|setting[s]?',
            r'알림|notice[s]?|notification[s]?',
            r'통계|statistic[s]?'
        ]
        
        # Standard entities that commonly appear
        standard_entities = [
            {
                "name": "users",
                "description": "Platform users with authentication",
                "attributes": ["id", "google_id", "email", "name", "profile_picture", "created_at", "updated_at"]
            },
            {
                "name": "study_groups", 
                "description": "Study groups led by captains",
                "attributes": ["id", "name", "description", "captain_id", "created_at", "updated_at"]
            },
            {
                "name": "study_rooms",
                "description": "Learning rooms within study groups", 
                "attributes": ["id", "study_group_id", "title", "goal", "genre", "description", "invite_code", "invite_password", "is_public", "created_at", "updated_at"]
            },
            {
                "name": "lectures",
                "description": "Individual video lectures",
                "attributes": ["id", "study_room_id", "title", "description", "thumbnail_url", "youtube_url", "content", "instructor_id", "lecture_order", "created_at", "updated_at"]
            }
        ]
        
        # Check which entities are mentioned in the text
        text_lower = text.lower()
        for entity in standard_entities:
            entity_name = entity["name"].replace("_", " ")
            if any(re.search(pattern, text_lower, re.IGNORECASE) for pattern in entity_patterns if entity_name.split("_")[0] in pattern):
                entities.append(entity)
        
        # Add conditional entities based on specific mentions
        if re.search(r'좋아요|like', text_lower, re.IGNORECASE):
            entities.append({
                "name": "lecture_likes",
                "description": "Lecture like relationships",
                "attributes": ["id", "lecture_id", "user_id", "created_at"]
            })
        
        if re.search(r'평가|evaluation|점수|score|rating', text_lower, re.IGNORECASE):
            entities.append({
                "name": "lecture_evaluations", 
                "description": "Lecture evaluations and ratings",
                "attributes": ["id", "lecture_id", "evaluator_id", "score", "comment", "created_at"]
            })
        
        if re.search(r'투표|vote|공개', text_lower, re.IGNORECASE):
            entities.append({
                "name": "public_course_votes",
                "description": "Voting for making courses public",
                "attributes": ["id", "study_room_id", "user_id", "vote", "created_at"]
            })
        
        if re.search(r'토큰|token|refresh', text_lower, re.IGNORECASE):
            entities.append({
                "name": "refresh_tokens",
                "description": "JWT refresh token management", 
                "attributes": ["id", "token", "user_id", "expires_at", "is_revoked", "created_at"]
            })
        
        if re.search(r'멤버|member|참여|join', text_lower, re.IGNORECASE):
            entities.append({
                "name": "study_group_members",
                "description": "Study group membership",
                "attributes": ["id", "study_group_id", "user_id", "role", "joined_at"]
            })
        
        if re.search(r'접근|access|권한|permission', text_lower, re.IGNORECASE):
            entities.append({
                "name": "study_room_access",
                "description": "Study room access permissions",
                "attributes": ["id", "study_room_id", "user_id", "granted_at"]
            })
        
        return entities
    
    def _extract_relationships_from_text(self, text: str, entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract relationships from business requirements text"""
        relationships = []
        entity_names = [e["name"] for e in entities]
        
        # Define common relationship patterns
        relationship_patterns = [
            # One-to-many patterns
            (r'study_groups', r'study_rooms', 'one-to-many'),
            (r'study_rooms', r'lectures', 'one-to-many'), 
            (r'users', r'study_groups', 'one-to-many'),  # captain relationship
            (r'users', r'lectures', 'one-to-many'),     # instructor relationship
            (r'users', r'refresh_tokens', 'one-to-many'),
            
            # Many-to-many patterns  
            (r'users', r'study_groups', 'many-to-many'),    # membership
            (r'users', r'study_rooms', 'many-to-many'),     # access
            (r'users', r'lectures', 'many-to-many'),        # likes/evaluations
        ]
        
        for from_table, to_table, rel_type in relationship_patterns:
            if from_table in entity_names and to_table in entity_names:
                if rel_type == 'one-to-many':
                    relationships.append({
                        "name": f"{to_table}_to_{from_table}",
                        "from": f"{to_table}.{from_table.rstrip('s')}_id",
                        "to": f"{from_table}.id", 
                        "type": "many-to-one",
                        "on_delete": "CASCADE" if "refresh_tokens" in to_table else "NO ACTION",
                        "on_update": "NO ACTION"
                    })
                elif rel_type == 'many-to-many':
                    # Will be handled by junction table creation
                    relationships.append({
                        "from_table": from_table,
                        "to_table": to_table,
                        "type": "many-to-many"
                    })
        
        return relationships
    
    def _entity_to_table_name(self, entity_name: str) -> str:
        """Convert entity name to table name"""
        return entity_name.lower().replace(" ", "_")
    
    def _generate_table_schema(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        """Generate table schema from entity definition"""
        table_schema = {
            "description": entity["description"],
            "columns": {},
            "indexes": []
        }
        
        # Generate columns
        for attr in entity["attributes"]:
            column_def = self._attribute_to_column(attr, entity["name"])
            table_schema["columns"][attr] = column_def
        
        # Add primary key index
        table_schema["indexes"].append({
            "name": f"ix_{entity['name']}_id",
            "unique": False,
            "columns": ["id"]
        })
        
        # Add foreign key indexes
        for attr in entity["attributes"]:
            if attr.endswith("_id") and attr != "id":
                table_schema["indexes"].append({
                    "name": f"ix_{entity['name']}_{attr}",
                    "unique": False,
                    "columns": [attr]
                })
        
        return table_schema
    
    def _attribute_to_column(self, attr: str, table_name: str) -> Dict[str, Any]:
        """Convert attribute to column definition"""
        # Primary key
        if attr == "id":
            return {
                "type": "INTEGER",
                "nullable": False,
                "primary_key": True,
                "auto_increment": True
            }
        
        # Foreign keys
        if attr.endswith("_id"):
            return {
                "type": "INTEGER", 
                "nullable": False,
                "primary_key": False
            }
        
        # Common column patterns
        column_patterns = {
            r'email': {"type": "VARCHAR(255)", "max_length": 255, "nullable": False, "unique": True},
            r'name|title': {"type": "VARCHAR(200)", "max_length": 200, "nullable": False},
            r'description|content': {"type": "TEXT", "nullable": True},
            r'url|link': {"type": "TEXT", "nullable": True},
            r'password|token': {"type": "VARCHAR(255)", "max_length": 255, "nullable": False},
            r'code': {"type": "VARCHAR(100)", "max_length": 100, "nullable": True},
            r'score|order|count': {"type": "INTEGER", "nullable": False},
            r'vote|is_|has_': {"type": "BOOLEAN", "nullable": False, "default": False},
            r'created_at|updated_at|expires_at|joined_at|granted_at': {"type": "DATETIME", "nullable": True, "default": "CURRENT_TIMESTAMP"},
            r'comment': {"type": "TEXT", "nullable": True}
        }
        
        for pattern, column_def in column_patterns.items():
            if re.search(pattern, attr, re.IGNORECASE):
                return column_def.copy()
        
        # Default column type
        return {
            "type": "TEXT",
            "nullable": True,
            "primary_key": False
        }
    
    def _create_junction_table(self, relationship: Dict[str, Any]) -> Dict[str, Any]:
        """Create junction table for many-to-many relationship"""
        from_table = relationship["from_table"]
        to_table = relationship["to_table"]
        
        # Generate junction table name
        junction_name = f"{from_table}_{to_table}_junction"
        if from_table == "study_group_members" or to_table == "study_group_members":
            junction_name = "study_group_members"
        elif from_table == "study_room_access" or to_table == "study_room_access":
            junction_name = "study_room_access"
        elif "like" in from_table or "like" in to_table:
            junction_name = "lecture_likes"
        elif "evaluation" in from_table or "evaluation" in to_table:
            junction_name = "lecture_evaluations"
        
        junction_schema = {
            "description": f"Junction table for {from_table} and {to_table}",
            "columns": {
                "id": {
                    "type": "INTEGER",
                    "nullable": False, 
                    "primary_key": True,
                    "auto_increment": True
                },
                f"{from_table.rstrip('s')}_id": {
                    "type": "INTEGER",
                    "nullable": False,
                    "primary_key": False
                },
                f"{to_table.rstrip('s')}_id": {
                    "type": "INTEGER", 
                    "nullable": False,
                    "primary_key": False
                },
                "created_at": {
                    "type": "DATETIME",
                    "nullable": True,
                    "default": "CURRENT_TIMESTAMP"
                }
            },
            "indexes": [
                {
                    "name": f"ix_{junction_name}_id",
                    "unique": False,
                    "columns": ["id"]
                }
            ]
        }
        
        # Create relationships for the junction table
        relationships = [
            {
                "name": f"{junction_name}_to_{from_table}",
                "from": f"{junction_name}.{from_table.rstrip('s')}_id",
                "to": f"{from_table}.id",
                "type": "many-to-one", 
                "on_delete": "CASCADE",
                "on_update": "NO ACTION"
            },
            {
                "name": f"{junction_name}_to_{to_table}",
                "from": f"{junction_name}.{to_table.rstrip('s')}_id", 
                "to": f"{to_table}.id",
                "type": "many-to-one",
                "on_delete": "CASCADE", 
                "on_update": "NO ACTION"
            }
        ]
        
        return {
            "name": junction_name,
            "schema": junction_schema,
            "relationships": relationships
        }