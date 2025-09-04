# Project Schema Management System

This document describes the automatic schema management system for MRV projects. Each project automatically gets its own database schema for data isolation and organization.

## Overview

When a user creates a new project, the system automatically:
1. Creates a new database schema named `project_{project_name}`
2. Stores all project-specific data in this schema
3. When the project is deleted, the schema and all its data are automatically removed

## How It Works

### Schema Naming Convention
- Project name: `MyProject` → Schema: `project_myproject`
- Project name: `test_project_123` → Schema: `project_test_project_123`
- Project name: `PROJECT_NAME` → Schema: `project_project_name`

### Automatic Schema Creation
When a new `Project` object is saved:
1. The `save()` method is overridden to detect new projects
2. For new projects, `create_project_schema()` is called automatically
3. A new schema is created in the NFI database using `CREATE SCHEMA IF NOT EXISTS`

### Automatic Schema Deletion
When a `Project` object is deleted:
1. The `delete()` method is overridden to handle cleanup
2. `delete_project_schema()` is called first to remove the schema and all its contents
3. The project record is then deleted from the database

## API Endpoints

### Project Management
- `GET /api/mrv/projects/` - List all projects
- `POST /api/mrv/projects/create/` - Create a new project (automatically creates schema)
- `GET /api/mrv/projects/{id}/` - Get project details
- `PUT /api/mrv/projects/{id}/update/` - Update project details
- `DELETE /api/mrv/projects/{id}/delete/` - Delete project (automatically deletes schema)

### Schema Information
- `GET /api/mrv/projects/{id}/schema/` - Get project schema information

## Project Model Methods

### Schema Management Methods
```python
project.get_schema_name()           # Returns schema name (e.g., "project_myproject")
project.create_project_schema()     # Creates the schema in database
project.delete_project_schema()     # Deletes the schema and all contents
project.schema_exists()             # Checks if schema exists
project.get_schema_tables()         # Returns list of tables in schema
```

### Example Usage
```python
# Create a new project (schema created automatically)
project = Project.objects.create(
    name="forest_analysis_2024",
    description="Forest analysis project for 2024"
)

# Get schema information
schema_name = project.get_schema_name()  # "project_forest_analysis_2024"
schema_exists = project.schema_exists()  # True
tables = project.get_schema_tables()     # [] (empty initially)

# Create tables in the project schema
with get_foris_connection() as conn:
    with conn.cursor() as cursor:
        cursor.execute(f"""
            CREATE TABLE {schema_name}.tree_data (
                id SERIAL PRIMARY KEY,
                tree_id VARCHAR(50),
                diameter NUMERIC,
                height NUMERIC
            )
        """)

# Delete project (schema deleted automatically)
project.delete()
```

## Database Connection

The system uses the NFI database connection (`get_foris_connection()`) for schema operations. This ensures that:
- All project schemas are created in the same database as the main application
- Schema operations are consistent with the existing database setup
- Foreign key relationships can be established between project data and reference data

## Error Handling

The system includes robust error handling:
- Schema creation/deletion errors are logged but don't prevent project operations
- If schema creation fails, the project is still created (schema can be created manually later)
- If schema deletion fails, the project is still deleted (schema cleanup can be done manually)

## Testing

Run the test script to verify functionality:
```bash
cd carbonapi
conda activate NFC
python test_project_schema.py
```

## Benefits

1. **Data Isolation**: Each project's data is completely separated
2. **Easy Cleanup**: Deleting a project removes all associated data
3. **Scalability**: No limit on number of projects or schemas
4. **Flexibility**: Projects can have different table structures
5. **Security**: Data from different projects cannot accidentally mix

## Best Practices

1. **Project Naming**: Use descriptive, unique names (only alphanumeric, underscore, hyphen allowed)
2. **Schema Usage**: Always use the project's schema name when creating tables
3. **Data Backup**: Consider backing up important project schemas before deletion
4. **Monitoring**: Monitor schema creation/deletion logs for any issues

## Troubleshooting

### Schema Not Created
- Check database connection settings
- Verify user has CREATE SCHEMA permissions
- Check Django logs for error messages

### Schema Not Deleted
- Check if schema has active connections
- Verify user has DROP SCHEMA permissions
- Manually drop schema if needed: `DROP SCHEMA project_name CASCADE`

### Permission Issues
- Ensure database user has appropriate permissions
- Check if schema name conflicts with existing schemas
- Verify project name follows naming conventions
