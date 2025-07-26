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
                        name="schema_json_to_sql",
                        description="Convert MCP DB schema JSON to SQL DDL statements",
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
                                },
                                "db_type": {
                                    "type": "string",
                                    "description": "Target database type (sqlite, postgresql, mysql)",
                                    "enum": ["sqlite", "postgresql", "mysql"],
                                    "default": "sqlite"
                                }
                            },
                            "required": ["schema_content"]
                        }
                    ),
                types.Tool(
                    name="extract_schema_from_db",
                    description="Extract MCP DB schema format from existing database",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "db_path": {
                                "type": "string",
                                "description": "Path to database file or connection string"
                            },
                            "output_file": {
                                "type": "string",
                                "description": "Optional output JSON file path"
                            },
                            "db_type": {
                                "type": "string",
                                "description": "Source database type (sqlite, postgresql, mysql)",
                                "enum": ["sqlite", "postgresql", "mysql"],
                                "default": "sqlite"
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
                    name="create_database_from_schema",
                    description="Create database from MCP DB schema format",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "schema_content": {
                                "type": "string",
                                "description": "JSON schema content or file path"
                            },
                            "db_path": {
                                "type": "string",
                                "description": "Output database file path or connection string"
                            },
                            "include_seed_data": {
                                "type": "boolean",
                                "description": "Whether to insert seed data",
                                "default": False
                            },
                            "db_type": {
                                "type": "string",
                                "description": "Target database type (sqlite, postgresql, mysql)",
                                "enum": ["sqlite", "postgresql", "mysql"],
                                "default": "sqlite"
                            }
                        },
                        "required": ["schema_content", "db_path"]
                    }
                ),
                types.Tool(
                    name="compare_with_models",
                    description="Compare database schema with backend models",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "db_path": {
                                "type": "string",
                                "description": "Path to SQLite database file"
                            },
                            "model_paths": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "List of Python model file paths"
                            }
                        },
                        "required": ["db_path", "model_paths"]
                    }
                ),
                types.Tool(
                    name="generate_schema_json_from_text",
                    description="Generate MCP DB schema JSON from business requirements text",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "business_requirements": {
                                "type": "string",
                                "description": "Business requirements and logic description in any format"
                            },
                            "output_file": {
                                "type": "string",
                                "description": "Optional output JSON file path"
                            },
                            "database_name": {
                                "type": "string",
                                "description": "Database name (defaults to 'generated_db')",
                                "default": "generated_db"
                            }
                        },
                        "required": ["business_requirements"]
                    }
                )
            ]

        @self.server.call_tool()
        async def handle_call_tool(
            name: str, arguments: Dict[str, Any]
        ) -> list[types.TextContent]:
            """Handle tool calls"""
            try:
                if name == "schema_json_to_sql":
                    return await self._handle_json_to_sql(arguments)
                elif name == "extract_schema_from_db":
                    return await self._handle_sql_to_json(arguments)
                elif name == "merge_schemas":
                    return await self._handle_merge_schemas(arguments)
                elif name == "validate_schema":
                    return await self._handle_validate_schema(arguments)
                elif name == "create_database_from_schema":
                    return await self._handle_create_database(arguments)
                elif name == "compare_with_models":
                    return await self._handle_compare_with_models(arguments)
                elif name == "generate_schema_json_from_text":
                    return await self._handle_generate_schema(arguments)
                else:
                    return [types.TextContent(type="text", text=f"Unknown tool: {name}")]
            except Exception as e:
                return [types.TextContent(type="text", text=f"Error: {str(e)}")]

    async def _handle_json_to_sql(self, arguments: Dict[str, Any]) -> list[types.TextContent]:
        """Convert JSON schema to SQL DDL"""
        schema_content = arguments["schema_content"]
        output_file = arguments.get("output_file")
        db_type = arguments.get("db_type", "sqlite")

        # Load schema (from file path or direct content)
        schema = self._load_schema(schema_content)
        
        # Convert to SQL
        sql_statements = self.converter.json_to_sql(schema, db_type)
        
        # Save to file if specified
        if output_file:
            Path(output_file).write_text(sql_statements, encoding="utf-8")
            result_text = f"✅ **{db_type.upper()} SQL DDL generated and saved to:** {output_file}\n\n"
        else:
            result_text = f"✅ **{db_type.upper()} SQL DDL generated:**\n\n"
            
        result_text += f"```sql\n{sql_statements}\n```"
        
        return [types.TextContent(type="text", text=result_text)]

    async def _handle_sql_to_json(self, arguments: Dict[str, Any]) -> list[types.TextContent]:
        """Extract JSON schema from database with multi-DB support"""
        db_path = arguments["db_path"]
        output_file = arguments.get("output_file")
        db_type = arguments.get("db_type", "sqlite")

        try:
            # Extract schema from database
            schema = self.converter.sql_to_json(db_path, db_type)
            
            # Convert to JSON string
            schema_json = json.dumps(schema, indent=2, ensure_ascii=False)
            
            # Save to file if specified
            if output_file:
                Path(output_file).write_text(schema_json, encoding="utf-8")
                result_text = f"✅ **{db_type.upper()} Schema extracted and saved to:** {output_file}\n\n"
            else:
                result_text = f"✅ **{db_type.upper()} Schema extracted:**\n\n"
            
            # Add summary
            table_count = len(schema.get("tables", {}))
            relationship_count = len(schema.get("relationships", []))
            
            result_text += f"📊 **Schema Summary:**\n"
            result_text += f"- Database: {schema.get('database', {}).get('name', 'unknown')}\n"
            result_text += f"- Tables: {table_count}\n"
            result_text += f"- Relationships: {relationship_count}\n\n"
            
            result_text += f"📋 **Tables Found:**\n"
            for table_name, table_info in schema.get("tables", {}).items():
                col_count = len(table_info.get("columns", {}))
                desc = table_info.get("description", "")
                result_text += f"- **{table_name}**: {col_count} columns - {desc}\n"
            
            result_text += f"\n```json\n{schema_json}\n```"
            
        except ImportError as e:
            result_text = f"❌ **Import Error:** {str(e)}\n"
            result_text += f"💡 **Tip:** Install required dependencies with: `pip install psycopg2-binary mysql-connector-python`"
        except Exception as e:
            result_text = f"❌ **Schema extraction failed:** {str(e)}"
        
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
        """Create database from JSON schema with multi-DB support"""
        schema_content = arguments["schema_content"]
        db_path = arguments["db_path"]
        include_seed_data = arguments.get("include_seed_data", False)
        db_type = arguments.get("db_type", "sqlite")

        # Load schema
        schema = self._load_schema(schema_content)
        
        try:
            # Create database using new multi-DB method
            result = self.converter.create_database_with_schema(schema, db_path, db_type, include_seed_data)
            
            result_text = f"✅ **{result['db_type'].upper()} Database created successfully!**\n\n"
            
            if db_type == "sqlite":
                result_text += f"📁 **Database:** {result['db_path']}\n"
            elif db_type == "postgresql":
                result_text += f"🐘 **PostgreSQL:** {result['connection_string']}\n"
            elif db_type == "mysql":
                result_text += f"🐬 **MySQL:** {result['connection_config']}\n"
            
            result_text += f"📊 **Tables created:** {result['tables_created']}\n"
            
            if result['seed_records'] > 0:
                result_text += f"🌱 **Seed records inserted:** {result['seed_records']}\n"
            
            result_text += f"\n📋 **Table List:**\n"
            for table in result['tables']:
                result_text += f"- **{table}**\n"
                
        except ImportError as e:
            result_text = f"❌ **Import Error:** {str(e)}\n"
            result_text += f"💡 **Tip:** Install required dependencies with: `pip install psycopg2-binary mysql-connector-python`"
        except Exception as e:
            result_text = f"❌ **Database creation failed:** {str(e)}"
        
        return [types.TextContent(type="text", text=result_text)]

    async def _handle_compare_with_models(self, arguments: Dict[str, Any]) -> list[types.TextContent]:
        """Compare database schema with backend models"""
        db_path = arguments["db_path"]
        model_paths = arguments["model_paths"]

        # Extract current database schema
        db_schema = self.converter.sql_to_json(db_path)
        
        # Compare with backend models
        comparison_result = self.converter.compare_with_backend_models(db_schema, model_paths)
        
        result_text = "🔍 **Database vs Models Comparison**\n\n"
        
        if comparison_result["missing_tables"]:
            result_text += f"❌ **Missing Tables in Database ({len(comparison_result['missing_tables'])}):**\n"
            for table in comparison_result["missing_tables"]:
                result_text += f"- {table}\n"
            result_text += "\n"
        
        if comparison_result.get("extra_tables"):
            result_text += f"⚠️ **Extra Tables in Database ({len(comparison_result['extra_tables'])}):**\n"
            for table in comparison_result["extra_tables"]:
                result_text += f"- {table}\n"
            result_text += "\n"
        
        if comparison_result.get("missing_columns"):
            result_text += f"📝 **Missing Columns:**\n"
            for table, columns in comparison_result["missing_columns"].items():
                result_text += f"- **{table}**: {', '.join(columns)}\n"
            result_text += "\n"
        
        if comparison_result["suggestions"]:
            result_text += f"💡 **Suggestions:**\n"
            for suggestion in comparison_result["suggestions"]:
                result_text += f"- {suggestion}\n"
        
        if not any([comparison_result["missing_tables"], 
                   comparison_result.get("extra_tables", []),
                   comparison_result.get("missing_columns", {})]):
            result_text += "✅ **No major discrepancies found!**\n"
            result_text += "Database schema appears to be in sync with models.\n"
        
        return [types.TextContent(type="text", text=result_text)]

    async def _handle_generate_schema(self, arguments: Dict[str, Any]) -> list[types.TextContent]:
        """Generate JSON schema from business requirements"""
        business_requirements = arguments["business_requirements"]
        output_file = arguments.get("output_file")
        database_name = arguments.get("database_name", "generated_db")

        # Generate schema from business text
        schema = self.converter.generate_from_text(business_requirements, database_name)
        
        # Convert to JSON string
        schema_json = json.dumps(schema, indent=2, ensure_ascii=False)
        
        # Save to file if specified
        if output_file:
            Path(output_file).write_text(schema_json, encoding="utf-8")
            result_text = f"✅ **Schema generated and saved to:** {output_file}\n\n"
        else:
            result_text = "✅ **Database schema generated from business requirements!**\n\n"
            
        # Add summary
        table_count = len(schema.get("tables", {}))
        relationship_count = len(schema.get("relationships", []))
        
        result_text += f"📊 **Generated Schema Summary:**\n"
        result_text += f"- Database: {schema.get('database', {}).get('name', database_name)}\n"
        result_text += f"- Tables: {table_count}\n"
        result_text += f"- Relationships: {relationship_count}\n\n"
        
        result_text += f"📋 **Tables Created:**\n"
        for table_name, table_info in schema.get("tables", {}).items():
            col_count = len(table_info.get("columns", {}))
            desc = table_info.get("description", "")
            result_text += f"- **{table_name}**: {col_count} columns - {desc}\n"
        
        result_text += f"\n```json\n{schema_json}\n```"
        
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