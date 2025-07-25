"""
Schema Validator - JSON schema integrity validation
"""

from typing import Dict, List, Any, Set


class SchemaValidator:
    """Validates JSON schema integrity and relationships"""
    
    def __init__(self):
        self.valid_types = {
            "INTEGER", "VARCHAR", "TEXT", "DATETIME", "DATE", 
            "BOOLEAN", "JSON", "ENUM", "REAL", "BLOB"
        }

    def validate_schema(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Validate complete schema and return validation result"""
        errors = []
        warnings = []
        
        # Basic structure validation
        if "database" not in schema:
            warnings.append("Missing 'database' section - using defaults")
        
        if "tables" not in schema:
            errors.append("Missing 'tables' section - schema must contain tables")
            return self._create_result(False, errors, warnings, schema)

        tables = schema["tables"]
        
        # Validate each table
        for table_name, table_info in tables.items():
            table_errors, table_warnings = self._validate_table(table_name, table_info, tables)
            errors.extend(table_errors)
            warnings.extend(table_warnings)

        # Validate relationships
        if "relationships" in schema:
            rel_errors, rel_warnings = self._validate_relationships(schema["relationships"], tables)
            errors.extend(rel_errors)
            warnings.extend(rel_warnings)

        # Validate seed data
        if "seed_data" in schema:
            seed_errors = self._validate_seed_data(schema["seed_data"], tables)
            errors.extend(seed_errors)

        # Cross-table validation
        cross_errors = self._validate_cross_references(tables)
        errors.extend(cross_errors)

        return self._create_result(len(errors) == 0, errors, warnings, schema)

    def _validate_table(self, table_name: str, table_info: Dict[str, Any], all_tables: Dict[str, Any]) -> tuple:
        """Validate a single table definition"""
        errors = []
        warnings = []

        # Check required fields
        if "columns" not in table_info:
            errors.append(f"Table '{table_name}': Missing 'columns' definition")
            return errors, warnings

        columns = table_info["columns"]
        
        if not columns:
            errors.append(f"Table '{table_name}': No columns defined")
            return errors, warnings

        # Validate each column
        primary_key_count = 0
        for col_name, col_info in columns.items():
            col_errors, col_warnings = self._validate_column(table_name, col_name, col_info, all_tables)
            errors.extend(col_errors)
            warnings.extend(col_warnings)
            
            if col_info.get("primary_key"):
                primary_key_count += 1

        # Check primary key constraints
        if primary_key_count == 0:
            warnings.append(f"Table '{table_name}': No primary key defined")
        elif primary_key_count > 1:
            errors.append(f"Table '{table_name}': Multiple primary keys defined")

        # Validate indexes
        if "indexes" in table_info:
            idx_errors = self._validate_indexes(table_name, table_info["indexes"], columns)
            errors.extend(idx_errors)

        return errors, warnings

    def _validate_column(self, table_name: str, col_name: str, col_info: Dict[str, Any], all_tables: Dict[str, Any]) -> tuple:
        """Validate a single column definition"""
        errors = []
        warnings = []

        # Check required type field
        if "type" not in col_info:
            errors.append(f"Table '{table_name}', column '{col_name}': Missing 'type' field")
            return errors, warnings

        col_type = col_info["type"]

        # Validate type
        base_type = col_type.split("(")[0]  # Remove size specs like VARCHAR(255)
        if base_type not in self.valid_types:
            errors.append(f"Table '{table_name}', column '{col_name}': Invalid type '{col_type}'")

        # Validate ENUM values
        if col_type == "ENUM":
            if "values" not in col_info:
                errors.append(f"Table '{table_name}', column '{col_name}': ENUM type requires 'values' field")
            elif not isinstance(col_info["values"], list) or len(col_info["values"]) == 0:
                errors.append(f"Table '{table_name}', column '{col_name}': ENUM values must be non-empty list")

        # Validate foreign key
        if "foreign_key" in col_info:
            fk_errors = self._validate_foreign_key(table_name, col_name, col_info["foreign_key"], all_tables)
            errors.extend(fk_errors)

        # Validate constraints
        if col_info.get("primary_key") and col_info.get("nullable") == True:
            errors.append(f"Table '{table_name}', column '{col_name}': Primary key cannot be nullable")

        if col_info.get("unique") and col_info.get("nullable") == False:
            # This is actually fine, just noting it
            pass

        # Check default value compatibility
        if "default" in col_info:
            default_warnings = self._validate_default_value(table_name, col_name, col_info)
            warnings.extend(default_warnings)

        return errors, warnings

    def _validate_foreign_key(self, table_name: str, col_name: str, fk_info: Dict[str, Any], all_tables: Dict[str, Any]) -> List[str]:
        """Validate foreign key reference"""
        errors = []

        # Check required fields
        if "table" not in fk_info:
            errors.append(f"Table '{table_name}', column '{col_name}': Foreign key missing 'table' field")
            return errors

        if "column" not in fk_info:
            errors.append(f"Table '{table_name}', column '{col_name}': Foreign key missing 'column' field")
            return errors

        ref_table = fk_info["table"]
        ref_column = fk_info["column"]

        # Check if referenced table exists
        if ref_table not in all_tables:
            errors.append(f"Table '{table_name}', column '{col_name}': Referenced table '{ref_table}' does not exist")
            return errors

        # Check if referenced column exists
        ref_table_columns = all_tables[ref_table].get("columns", {})
        if ref_column not in ref_table_columns:
            errors.append(f"Table '{table_name}', column '{col_name}': Referenced column '{ref_table}.{ref_column}' does not exist")

        # Validate ON DELETE/UPDATE actions
        valid_actions = {"CASCADE", "SET NULL", "RESTRICT", "NO ACTION"}
        if "on_delete" in fk_info and fk_info["on_delete"] not in valid_actions:
            errors.append(f"Table '{table_name}', column '{col_name}': Invalid ON DELETE action '{fk_info['on_delete']}'")
        
        if "on_update" in fk_info and fk_info["on_update"] not in valid_actions:
            errors.append(f"Table '{table_name}', column '{col_name}': Invalid ON UPDATE action '{fk_info['on_update']}'")

        return errors

    def _validate_indexes(self, table_name: str, indexes: List[Dict[str, Any]], columns: Dict[str, Any]) -> List[str]:
        """Validate table indexes"""
        errors = []
        index_names = set()

        for index in indexes:
            # Check required fields
            if "name" not in index:
                errors.append(f"Table '{table_name}': Index missing 'name' field")
                continue

            if "columns" not in index:
                errors.append(f"Table '{table_name}': Index '{index['name']}' missing 'columns' field")
                continue

            index_name = index["name"]
            
            # Check for duplicate index names
            if index_name in index_names:
                errors.append(f"Table '{table_name}': Duplicate index name '{index_name}'")
            index_names.add(index_name)

            # Check if indexed columns exist
            for col_name in index["columns"]:
                if col_name not in columns:
                    errors.append(f"Table '{table_name}', index '{index_name}': Column '{col_name}' does not exist")

        return errors

    def _validate_relationships(self, relationships: List[Dict[str, Any]], tables: Dict[str, Any]) -> tuple:
        """Validate relationship definitions"""
        errors = []
        warnings = []

        for rel in relationships:
            # Check required fields
            if "from" not in rel or "to" not in rel:
                errors.append("Relationship missing 'from' or 'to' field")
                continue

            # Parse relationship endpoints
            from_parts = rel["from"].split(".")
            to_parts = rel["to"].split(".")

            if len(from_parts) != 2 or len(to_parts) != 2:
                errors.append(f"Invalid relationship format: '{rel.get('from')}' -> '{rel.get('to')}'")
                continue

            from_table, from_col = from_parts
            to_table, to_col = to_parts

            # Check if tables exist
            if from_table not in tables:
                errors.append(f"Relationship references non-existent table '{from_table}'")
            if to_table not in tables:
                errors.append(f"Relationship references non-existent table '{to_table}'")

            # Check if columns exist
            if from_table in tables and from_col not in tables[from_table].get("columns", {}):
                errors.append(f"Relationship references non-existent column '{from_table}.{from_col}'")
            if to_table in tables and to_col not in tables[to_table].get("columns", {}):
                errors.append(f"Relationship references non-existent column '{to_table}.{to_col}'")

        return errors, warnings

    def _validate_seed_data(self, seed_data: Dict[str, Any], tables: Dict[str, Any]) -> List[str]:
        """Validate seed data against table definitions"""
        errors = []

        for table_name, records in seed_data.items():
            if table_name not in tables:
                errors.append(f"Seed data references non-existent table '{table_name}'")
                continue

            table_columns = tables[table_name].get("columns", {})
            
            for i, record in enumerate(records):
                if not isinstance(record, dict):
                    errors.append(f"Seed data for '{table_name}', record {i}: Must be a dictionary")
                    continue

                # Check for invalid columns
                for col_name in record.keys():
                    if col_name not in table_columns:
                        errors.append(f"Seed data for '{table_name}', record {i}: Column '{col_name}' does not exist")

                # Check for missing required columns
                for col_name, col_info in table_columns.items():
                    if (col_info.get("nullable") == False and 
                        "default" not in col_info and 
                        not col_info.get("auto_increment") and
                        col_name not in record):
                        errors.append(f"Seed data for '{table_name}', record {i}: Missing required column '{col_name}'")

        return errors

    def _validate_cross_references(self, tables: Dict[str, Any]) -> List[str]:
        """Validate cross-table references and detect circular dependencies"""
        errors = []
        
        # Build dependency graph
        dependencies = {}
        for table_name, table_info in tables.items():
            dependencies[table_name] = set()
            
            for col_name, col_info in table_info.get("columns", {}).items():
                if "foreign_key" in col_info:
                    ref_table = col_info["foreign_key"]["table"]
                    if ref_table != table_name:  # Ignore self-references
                        dependencies[table_name].add(ref_table)

        # Detect circular dependencies
        visited = set()
        rec_stack = set()
        
        def has_cycle(node):
            visited.add(node)
            rec_stack.add(node)
            
            for neighbor in dependencies.get(node, []):
                if neighbor not in visited:
                    if has_cycle(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True
            
            rec_stack.remove(node)
            return False

        for table_name in tables:
            if table_name not in visited:
                if has_cycle(table_name):
                    errors.append(f"Circular dependency detected involving table '{table_name}'")

        return errors

    def _validate_default_value(self, table_name: str, col_name: str, col_info: Dict[str, Any]) -> List[str]:
        """Validate default value compatibility with column type"""
        warnings = []
        
        col_type = col_info["type"]
        default_val = col_info["default"]

        # Type compatibility checks
        if col_type == "INTEGER" and not isinstance(default_val, (int, str)):
            warnings.append(f"Table '{table_name}', column '{col_name}': Default value may not be compatible with INTEGER type")
        
        elif col_type == "BOOLEAN" and default_val not in [True, False, "true", "false", 0, 1]:
            warnings.append(f"Table '{table_name}', column '{col_name}': Default value may not be compatible with BOOLEAN type")
        
        elif col_type == "ENUM":
            enum_values = col_info.get("values", [])
            if default_val not in enum_values:
                warnings.append(f"Table '{table_name}', column '{col_name}': Default value '{default_val}' not in ENUM values")

        return warnings

    def _create_result(self, is_valid: bool, errors: List[str], warnings: List[str], schema: Dict[str, Any]) -> Dict[str, Any]:
        """Create validation result dictionary"""
        tables = schema.get("tables", {})
        
        return {
            "is_valid": is_valid,
            "errors": errors,
            "warnings": warnings,
            "table_count": len(tables),
            "relationship_count": len(schema.get("relationships", [])),
            "index_count": sum(len(table.get("indexes", [])) for table in tables.values()),
            "total_columns": sum(len(table.get("columns", {})) for table in tables.values())
        }