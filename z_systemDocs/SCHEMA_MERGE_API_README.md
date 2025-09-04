# Schema Merge API Documentation

This API allows users to merge two PostgreSQL schemas that have the same table structure into a new or existing target schema. **The API now properly handles foreign key relationships and table dependencies.**

## API Endpoint

**URL:** `/inventory/merge-schemas/`  
**Method:** POST  
**Content-Type:** `application/x-www-form-urlencoded`

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `source_schema1` | string | Yes | First source schema name |
| `source_schema2` | string | Yes | Second source schema name |
| `target_schema` | string | Yes | Target schema name for the merged result |
| `create_new_schema` | boolean | No | Whether to create a new schema (default: true) |

## Request Examples

### Using cURL
```bash
curl -X POST http://localhost:8000/inventory/merge-schemas/ \
  -d "source_schema1=fra_high_mountain_high_himal" \
  -d "source_schema2=fra_high_mountain_high_himal" \
  -d "target_schema=merged_schema" \
  -d "create_new_schema=true"
```

### Using Python requests
```python
import requests

data = {
    'source_schema1': 'fra_high_mountain_high_himal',
    'source_schema2': 'fra_high_mountain_high_himal',
    'target_schema': 'merged_schema',
    'create_new_schema': 'true'
}

response = requests.post('http://localhost:8000/inventory/merge-schemas/', data=data)
result = response.json()
```

## Response Format

### Success Response (200 OK)
```json
{
    "success": true,
    "message": "Successfully merged schemas 'schema1' and 'schema2' into 'target_schema'",
    "details": {
        "source_schema1_tables": ["table1", "table2", "table3"],
        "source_schema2_tables": ["table1", "table2", "table3"],
        "target_schema_tables": ["table1", "table2", "table3"],
        "merged_tables": ["table1", "table2", "table3"],
        "creation_order": ["table1", "table2", "table3"]
    },
    "source_schema1": "schema1",
    "source_schema2": "schema2",
    "target_schema": "target_schema",
    "create_new_schema": true
}
```

### Error Responses

#### Missing Parameters (400 Bad Request)
```json
{
    "error": "Missing required parameters: source_schema1, source_schema2, target_schema"
}
```

#### Schema Not Found (404 Not Found)
```json
{
    "error": "Source schema 'non_existent_schema' does not exist"
}
```

#### Different Table Structures (400 Bad Request)
```json
{
    "error": "Schemas have different table structures and cannot be merged",
    "details": {
        "schema1_tables": ["table1", "table2"],
        "schema2_tables": ["table1", "table3"],
        "differences": {
            "only_in_schema1": ["table2"],
            "only_in_schema2": ["table3"],
            "common": ["table1"]
        }
    }
}
```

#### Target Schema Not Found (404 Not Found)
```json
{
    "error": "Target schema 'target_schema' does not exist and create_new_schema is False"
}
```

#### Server Error (500 Internal Server Error)
```json
{
    "error": "Failed to merge schemas: [specific error message]"
}
```

## Features

### 1. Schema Validation
- Checks if both source schemas exist
- Validates that schemas have identical table structures
- Ensures target schema exists when `create_new_schema=false`

### 2. Table Structure Comparison
- Compares table names between schemas
- Provides detailed differences if schemas don't match
- Prevents merging of incompatible schemas

### 3. **Foreign Key Relationship Handling** ⭐ **NEW**
- **Dependency Analysis**: Analyzes foreign key relationships between tables
- **Topological Sorting**: Creates tables in correct dependency order
- **Constraint Preservation**: Maintains all primary key and foreign key constraints
- **Circular Dependency Detection**: Prevents infinite loops in table creation

### 4. **Advanced Data Merging** ⭐ **IMPROVED**
- **Structure-First Approach**: Creates all table structures before copying data
- **Primary Key Conflict Resolution**: Uses UPSERT operations for duplicate handling
- **Data Integrity**: Preserves referential integrity during merge
- **Transaction Safety**: Uses database transactions for rollback capability

### 5. Flexible Target Schema
- Can create new target schema automatically
- Can merge into existing schema (with validation)
- Supports schema name with or without quotes

## **Foreign Key Handling Details** 🔗

### How It Works:
1. **Dependency Mapping**: Analyzes all foreign key relationships in source schemas
2. **Topological Sort**: Orders tables so referenced tables are created before dependent tables
3. **Structure Creation**: Creates all tables in dependency order with proper constraints
4. **Data Migration**: Copies data while maintaining referential integrity

### Example Dependency Order:
```
1. sub_plot_code (no dependencies)
2. positioning_method_code (no dependencies)
3. fao_landuse_class_code (no dependencies)
4. plot (depends on multiple code tables)
5. stand (depends on plot)
6. tree_and_climber (depends on plot and code tables)
```

## Error Handling

The API provides comprehensive error handling for:

1. **Missing Parameters**: Validates all required parameters are provided
2. **Schema Existence**: Checks if source and target schemas exist
3. **Table Structure Mismatch**: Prevents merging of incompatible schemas
4. **Foreign Key Conflicts**: Handles circular dependencies and constraint violations
5. **Database Errors**: Handles PostgreSQL-specific errors gracefully
6. **Transaction Rollback**: Ensures data consistency on errors

## Security Considerations

- Uses parameterized queries to prevent SQL injection
- Validates schema names before processing
- Implements proper error handling without exposing sensitive information
- Uses database transactions for data consistency
- **Foreign key constraint validation** prevents data corruption

## Usage Examples

### Example 1: Merge Schemas with Foreign Keys
```python
# Merge two schemas with complex foreign key relationships
data = {
    'source_schema1': 'production_data_2022',
    'source_schema2': 'production_data_2023',
    'target_schema': 'merged_production_data',
    'create_new_schema': 'true'
}
```

### Example 2: Merge into Existing Schema
```python
# Merge into an existing target schema
data = {
    'source_schema1': 'backup_schema1',
    'source_schema2': 'backup_schema2',
    'target_schema': 'main_schema',
    'create_new_schema': 'false'
}
```

### Example 3: Handle Different Table Structures
```python
# This will return an error with detailed differences
data = {
    'source_schema1': 'schema_with_table_a',
    'source_schema2': 'schema_with_table_b',
    'target_schema': 'merged_schema',
    'create_new_schema': 'true'
}
```

## Testing

Use the provided test scripts to verify the API functionality:

### Basic Testing:
```bash
python test_schema_merge.py
```

### Advanced Testing (with Foreign Keys):
```bash
python test_schema_merge_advanced.py
```

The test scripts demonstrate:
- ✅ Successful schema merging with foreign keys
- ✅ Table dependency resolution
- ✅ Data integrity preservation
- ✅ Error handling for non-existent schemas
- ✅ Response parsing and validation

## Database Requirements

- PostgreSQL database with NFI configuration
- Proper database permissions for schema creation and table operations
- Sufficient disk space for merged data
- Network connectivity to the database server
- **Foreign key constraint support** (standard in PostgreSQL)

## **Technical Implementation Details** 🔧

### New Utility Functions:
- `get_table_dependencies()`: Maps foreign key relationships
- `get_table_creation_order()`: Performs topological sorting
- `get_table_structure()`: Extracts CREATE TABLE statements

### Merge Process:
1. **Validation Phase**: Check schema existence and table compatibility
2. **Analysis Phase**: Map dependencies and determine creation order
3. **Structure Phase**: Create all tables in dependency order
4. **Data Phase**: Copy data with conflict resolution
5. **Verification Phase**: Confirm merge success and return details

### Performance Considerations:
- Uses efficient SQL queries for dependency analysis
- Implements batch operations where possible
- Maintains transaction integrity throughout the process
- Provides detailed progress reporting
