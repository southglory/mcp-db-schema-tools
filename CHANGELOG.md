# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2025-07-25

### Added
- **Automatic ID Generation**: Seed data now automatically generates missing primary key IDs for INTEGER primary key columns
- **New Tool**: `compare_with_models` - Compare database schema with backend SQLAlchemy models to detect synchronization issues
- **Enhanced Model Comparison**: Basic AST parsing to extract model information from Python files

### Fixed 
- **ENUM Type Processing**: Improved ENUM handling to properly generate `values` arrays in JSON schema output
- **Seed Data Validation**: Fixed missing ID field validation errors by automatically generating IDs where needed
- **Schema Consistency**: Better handling of ENUM types during SQL to JSON conversion

### Improved
- **Error Messages**: More detailed and actionable error messages during schema validation
- **Type Safety**: Enhanced type checking for ENUM values and default value validation
- **Documentation**: Updated README with new tool information

### Technical Details
- Added `_process_seed_data()` method to automatically handle missing primary key IDs
- Enhanced `compare_with_backend_models()` method for model synchronization checks
- Improved ENUM value extraction from CHECK constraints in SQL DDL
- Added new MCP tool endpoint for model comparison functionality

### Breaking Changes
- None - all changes are backward compatible

## [0.1.0] - 2025-07-24

### Added
- Initial release with core functionality
- JSON to SQL DDL conversion
- SQL to JSON schema extraction  
- Schema validation and integrity checking
- Multiple schema merging capabilities
- SQLite database creation from JSON schemas
- MCP server integration for Claude Code