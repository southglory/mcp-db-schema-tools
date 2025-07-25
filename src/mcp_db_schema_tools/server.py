"""
MCP Server for DB Schema Tools
"""

import json
import sqlite3
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

from mcp.server import Server, NotificationOptions
from mcp.server.stdio import stdio_server
from mcp.server.models import InitializationOptions
import mcp.types as types
from pydantic import BaseModel

from .schema_converter import SchemaConverter
from .schema_validator import SchemaValidator


class DBSchemaServer:
    def __init__(self):
        self.server = Server("mcp-db-schema-tools")
        self.converter = SchemaConverter()
        self.validator = SchemaValidator()
        self.setup_handlers()

    def setup_handlers(self):
        @self.server.list_tools()
        async def handle_list_tools() -> list[types.Tool]:
            """List available tools"""
            return [
                types.Tool(
                        name="json_to_sql",
                        description="Convert JSON schema to SQL DDL statements",
                        inputSchema={
                            "type": "object",
                            "properties": {
                                "schema_content": {
                                    "type": "string",
                                    "description": "JSON schema content or file path"
                                },
                                "output_file": {
                                    "type": "string", 
                                    "description": "Optional output SQL file path"
                                }
                            },
                            "required": ["schema_content"]
                        }
                    ),
                types.Tool(
                    name="sql_to_json",
                    description="Extract JSON schema from existing SQLite database",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "db_path": {
                                "type": "string",
                                "description": "Path to SQLite database file"
                            },
                            "output_file": {
                                "type": "string",
                                "description": "Optional output JSON file path"
                            }
                        },
                        "required": ["db_path"]
                    }
                ),
                types.Tool(
                    name="merge_schemas",
                    description="Merge multiple JSON schema files into one",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "schema_files": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "List of JSON schema file paths or patterns"
                            },
                            "output_file": {
                                "type": "string",
                                "description": "Output merged schema file path"
                            }
                        },
                        "required": ["schema_files"]
                    }
                ),
                types.Tool(
                    name="validate_schema",
                    description="Validate JSON schema integrity and relationships",
                    inputSchema={
                        "type": "object", 
                        "properties": {
                            "schema_content": {
                                "type": "string",
                                "description": "JSON schema content or file path"
                            }
                        },
                        "required": ["schema_content"]
                    }
                ),
                types.Tool(
                    name="create_database",
                    description="Create SQLite database from JSON schema",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "schema_content": {
                                "type": "string",
                                "description": "JSON schema content or file path"
                            },
                            "db_path": {
                                "type": "string",
                                "description": "Output SQLite database file path"
                            },
                            "include_seed_data": {
                                "type": "boolean",
                                "description": "Whether to insert seed data",
                                "default": False
                            }
                        },
                        "required": ["schema_content", "db_path"]
                    }
                )
            ]

        @self.server.call_tool()
        async def handle_call_tool(
            name: str, arguments: Dict[str, Any]
        ) -> list[types.TextContent]:
            """Handle tool calls"""
            try:
                if name == "json_to_sql":
                    return await self._handle_json_to_sql(arguments)
                elif name == "sql_to_json":
                    return await self._handle_sql_to_json(arguments)
                elif name == "merge_schemas":
                    return await self._handle_merge_schemas(arguments)
                elif name == "validate_schema":
                    return await self._handle_validate_schema(arguments)
                elif name == "create_database":
                    return await self._handle_create_database(arguments)
                else:
                    return [types.TextContent(type="text", text=f"Unknown tool: {name}")]
            except Exception as e:
                return [types.TextContent(type="text", text=f"Error: {str(e)}")]

    async def _handle_json_to_sql(self, arguments: Dict[str, Any]) -> list[types.TextContent]:
        """Convert JSON schema to SQL DDL"""
        schema_content = arguments["schema_content"]
        output_file = arguments.get("output_file")

        # Load schema (from file path or direct content)
        schema = self._load_schema(schema_content)
        
        # Convert to SQL
        sql_statements = self.converter.json_to_sql(schema)
        
        # Save to file if specified
        if output_file:
            Path(output_file).write_text(sql_statements, encoding="utf-8")
            result_text = f"✅ SQL DDL generated and saved to: {output_file}\n\n"
        else:
            result_text = "✅ SQL DDL generated:\n\n"
            
        result_text += f"```sql\n{sql_statements}\n```"
        
        return [types.TextContent(type="text", text=result_text)]

    async def _handle_sql_to_json(self, arguments: Dict[str, Any]) -> list[types.TextContent]:
        """Extract JSON schema from SQLite database"""
        db_path = arguments["db_path"]
        output_file = arguments.get("output_file")

        # Extract schema from database
        schema = self.converter.sql_to_json(db_path)
        
        # Convert to JSON string
        schema_json = json.dumps(schema, indent=2, ensure_ascii=False)
        
        # Save to file if specified
        if output_file:
            Path(output_file).write_text(schema_json, encoding="utf-8")
            result_text = f"✅ JSON schema extracted and saved to: {output_file}\n\n"
        else:
            result_text = "✅ JSON schema extracted:\n\n"
            
        result_text += f"```json\n{schema_json}\n```"
        
        return [types.TextContent(type="text", text=result_text)]

    async def _handle_merge_schemas(self, arguments: Dict[str, Any]) -> list[types.TextContent]:
        """Merge multiple JSON schema files"""
        schema_files = arguments["schema_files"]
        output_file = arguments.get("output_file", "merged_schema.json")

        # Load and merge schemas
        merged_schema = self.converter.merge_schemas(schema_files)
        
        # Convert to JSON string
        schema_json = json.dumps(merged_schema, indent=2, ensure_ascii=False)
        
        # Save merged schema
        Path(output_file).write_text(schema_json, encoding="utf-8")
        
        table_count = len(merged_schema.get("tables", {}))
        relationship_count = len(merged_schema.get("relationships", []))
        
        result_text = f"""✅ Schemas merged successfully!

📊 **Merge Summary:**
- Output file: {output_file}
- Total tables: {table_count}
- Total relationships: {relationship_count}
- Source files: {len(schema_files)}

📋 **Merged Tables:**
"""
        
        for table_name, table_info in merged_schema.get("tables", {}).items():
            col_count = len(table_info.get("columns", {}))
            desc = table_info.get("description", "")
            result_text += f"- **{table_name}**: {col_count} columns - {desc}\n"

        return [types.TextContent(type="text", text=result_text)]

    async def _handle_validate_schema(self, arguments: Dict[str, Any]) -> list[types.TextContent]:
        """Validate JSON schema integrity"""
        schema_content = arguments["schema_content"]
        
        # Load schema
        schema = self._load_schema(schema_content)
        
        # Validate schema
        validation_result = self.validator.validate_schema(schema)
        
        if validation_result["is_valid"]:
            result_text = "✅ **Schema validation passed!**\n\n"
            result_text += f"📊 **Schema Summary:**\n"
            result_text += f"- Tables: {validation_result['table_count']}\n"
            result_text += f"- Relationships: {validation_result['relationship_count']}\n"
            result_text += f"- Indexes: {validation_result['index_count']}\n"
        else:
            result_text = "❌ **Schema validation failed!**\n\n"
            result_text += f"🚨 **Errors found ({len(validation_result['errors'])}):**\n"
            for error in validation_result["errors"]:
                result_text += f"- {error}\n"
                
        if validation_result.get("warnings"):
            result_text += f"\n⚠️ **Warnings ({len(validation_result['warnings'])}):**\n"
            for warning in validation_result["warnings"]:
                result_text += f"- {warning}\n"
        
        return [types.TextContent(type="text", text=result_text)]

    async def _handle_create_database(self, arguments: Dict[str, Any]) -> list[types.TextContent]:
        """Create SQLite database from JSON schema"""
        schema_content = arguments["schema_content"]
        db_path = arguments["db_path"]
        include_seed_data = arguments.get("include_seed_data", False)

        # Load schema
        schema = self._load_schema(schema_content)
        
        # Generate SQL DDL
        sql_statements = self.converter.json_to_sql(schema)
        
        # Create database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            # Execute DDL statements
            for statement in sql_statements.split(';'):
                statement = statement.strip()
                if statement:
                    cursor.execute(statement)
            
            # Insert seed data if requested
            seed_count = 0
            if include_seed_data and "seed_data" in schema:
                for table_name, records in schema["seed_data"].items():
                    for record in records:
                        columns = ", ".join(record.keys())
                        placeholders = ", ".join(["?" for _ in record])
                        values = list(record.values())
                        
                        insert_sql = f"INSERT OR IGNORE INTO {table_name} ({columns}) VALUES ({placeholders})"
                        cursor.execute(insert_sql, values)
                        seed_count += 1
            
            conn.commit()
            
            # Get table info
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = [row[0] for row in cursor.fetchall() if row[0] != 'sqlite_sequence']
            
            result_text = f"✅ **Database created successfully!**\n\n"
            result_text += f"📁 **Database:** {db_path}\n"
            result_text += f"📊 **Tables created:** {len(tables)}\n"
            
            if include_seed_data:
                result_text += f"🌱 **Seed records inserted:** {seed_count}\n\n"
            
            result_text += f"📋 **Table List:**\n"
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                result_text += f"- **{table}**: {count} records\n"
                
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
        
        return [types.TextContent(type="text", text=result_text)]

    def _load_schema(self, schema_content: str) -> Dict[str, Any]:
        """Load schema from content or file path"""
        # Try to parse as JSON first
        try:
            return json.loads(schema_content)
        except json.JSONDecodeError:
            # Assume it's a file path
            schema_path = Path(schema_content)
            if schema_path.exists():
                return json.loads(schema_path.read_text(encoding="utf-8"))
            else:
                raise FileNotFoundError(f"Schema file not found: {schema_content}")


async def main():
    """Main entry point"""
    server_instance = DBSchemaServer()
    
    # Run the server using stdio transport
    async with stdio_server() as (read_stream, write_stream):
        await server_instance.server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="mcp-db-schema-tools",
                server_version="0.1.0",
                capabilities=server_instance.server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())