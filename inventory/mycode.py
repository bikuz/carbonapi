import os
import zipfile
import tempfile
import re
from django.conf import settings
from carbonapi.database.connection import get_foris_connection
import psycopg2
from psycopg2.sql import SQL, Identifier, Literal
from collections import defaultdict, deque

def extract_zip_file(zip_path, extract_to):
    """Extract zip file to temporary directory"""
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    return [f for f in os.listdir(extract_to) if f.endswith('.sql')]

def analyze_sql_file(sql_path):
    """Analyze SQL file to find schema creation statements"""
    with open(sql_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Check for CREATE SCHEMA statements
    schema_match = re.search(r'CREATE\s+SCHEMA\s+(IF\s+NOT\s+EXISTS\s+)?([^\s;]+)', content, re.IGNORECASE)
    schema_name = schema_match.group(2) if schema_match else None
    
    return {
        'has_schema_creation': bool(schema_match),
        'schema_name': schema_name,
        'sql_content': content
    }

def schema_exists(schema_name):
    """Check if schema exists in NFI database"""
    conn = None
    try:
        # Remove surrounding quotes if they exist
        clean_name = schema_name.strip('"')

        conn = get_foris_connection()
        with conn.cursor() as cursor:
            # Use parameterized query to avoid SQL injection
            cursor.execute(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s",
                (clean_name,)
            )
            result = cursor.fetchone()
            return bool(result)
    finally:
        if conn:
            conn.close()

def drop_schema_if_exists(schema_name):
    """
    Drop a schema if it exists in the NFI database
    Returns (success, message) tuple
    """
    clean_schema_name = schema_name.strip('"')
    try:
        with get_foris_connection() as conn:
            with conn.cursor() as cursor:
                # Use proper SQL composition for security
                cursor.execute(
                    SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(Identifier(clean_schema_name))
                )
            conn.commit()
        return True, f'Successfully dropped schema {clean_schema_name}'
    except Exception as e:
        return False, f'Failed to drop schema {clean_schema_name}: {str(e)}'
    
def execute_sql_script(sql_content, schema_name=None):
    """Execute SQL script in NFI database with optional schema context"""
    if not sql_content or not sql_content.strip():
        return False, "SQL content cannot be empty"
    
    conn = None
    try:
        conn = get_foris_connection()
        conn.autocommit = False
        with conn.cursor() as cursor:
            if schema_name:
                try:
                    cursor.execute(
                        SQL("SET search_path TO {}").format(Identifier(schema_name))
                    )
                except Exception as e:
                    return False, f"Failed to set search path to schema '{schema_name}': {str(e)}"

            # Execute SQL content
            try:
                cursor.execute(sql_content)
            except psycopg2.Error as e:
                return False, f"SQL execution error: {str(e)}"
            except Exception as e:
                return False, f"Unexpected error during SQL execution: {str(e)}"

        try:
            conn.commit()
        except Exception as e:
            conn.rollback()
            return False, f"Failed to commit transaction: {str(e)}"
        
        return True, None
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        return False, f"Database connection error: {str(e)}"
    except Exception as e:
        if conn:
            conn.rollback()
        return False, f"Unexpected error: {str(e)}"
    finally:
        if conn:
            conn.close()

def ensure_schema_import_table_exists():
    """Ensure the schema_imports table exists in NFI_tables"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS schema_imports (
        id SERIAL PRIMARY KEY,
        uploaded_file VARCHAR(500) NOT NULL,
        schema_name VARCHAR(100),
        status VARCHAR(20) NOT NULL,
        created_at TIMESTAMP NOT NULL,
        completed_at TIMESTAMP,
        message TEXT
    )
    """
    with get_foris_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_table_sql)
        conn.commit()

def create_schema_import_record(uploaded_file, schema_name, status='pending'):
    """Create a new import record directly in NFI_tables"""
    with get_foris_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO schema_imports 
                (uploaded_file, schema_name, status, created_at)
                VALUES (%s, %s, %s, NOW())
                RETURNING id
                """,
                (uploaded_file, schema_name, status)
            )
            import_id = cursor.fetchone()[0]
        conn.commit()
    return import_id

def get_schema_import_record(import_id):
    """Retrieve an import record from NFI_tables"""
    with get_foris_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, uploaded_file, schema_name, status, 
                       created_at, completed_at, message
                FROM schema_imports
                WHERE id = %s
                """,
                (import_id,)
            )
            row = cursor.fetchone()
    
    if not row:
        return None
    
    return {
        'id': row[0],
        'uploaded_file': row[1],
        'schema_name': row[2],
        'status': row[3],
        'created_at': row[4],
        'completed_at': row[5],
        'message': row[6]
    }

def update_schema_import_record(import_id, **kwargs):
    """Update an import record in NFI_tables"""
    set_clauses = []
    params = []
    
    for field, value in kwargs.items():
        set_clauses.append(f"{field} = %s")
        params.append(value)
    
    params.append(import_id)
    
    with get_foris_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                UPDATE schema_imports
                SET {', '.join(set_clauses)}
                WHERE id = %s
                """,
                params
            )
        conn.commit()

def get_schema_tables(schema_name):
    """
    Get list of tables in a schema
    Returns list of table names
    """
    conn = None
    try:
        clean_name = schema_name.strip('"')
        conn = get_foris_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """, (clean_name,))
            return [row[0] for row in cursor.fetchall()]
    finally:
        if conn:
            conn.close()

def compare_schema_tables(schema1, schema2):
    """
    Compare tables between two schemas
    Returns (are_equal, schema1_tables, schema2_tables, differences)
    """
    schema1_tables = set(get_schema_tables(schema1))
    schema2_tables = set(get_schema_tables(schema2))
    
    are_equal = schema1_tables == schema2_tables
    differences = {
        'only_in_schema1': list(schema1_tables - schema2_tables),
        'only_in_schema2': list(schema2_tables - schema1_tables),
        'common': list(schema1_tables & schema2_tables)
    }
    
    return are_equal, list(schema1_tables), list(schema2_tables), differences

def get_table_dependencies(schema_name):
    """
    Get table dependencies (foreign key relationships) for a schema
    Returns dict with table as key and list of tables it depends on as value
    """
    conn = None
    try:
        clean_name = schema_name.strip('"')
        conn = get_foris_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    tc.table_name,
                    ccu.table_name AS referenced_table
                FROM information_schema.table_constraints tc
                JOIN information_schema.constraint_column_usage ccu 
                    ON tc.constraint_name = ccu.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY' 
                    AND tc.table_schema = %s
                    AND ccu.table_schema = %s
                ORDER BY tc.table_name, ccu.table_name
            """, (clean_name, clean_name))
            
            dependencies = defaultdict(set)
            for row in cursor.fetchall():
                table, referenced_table = row
                # A table depends on its referenced tables
                dependencies[table].add(referenced_table)
            
            # Convert sets to lists for consistency
            return {table: list(deps) for table, deps in dependencies.items()}
    finally:
        if conn:
            conn.close()

def get_table_creation_order(schema_name):
    """
    Get tables in order of creation (dependencies first) using topological sort
    Returns list of tables in correct creation order
    """
    if not schema_name:
        raise ValueError("Schema name must be provided")
    
    try:
        tables = get_schema_tables(schema_name)
        dependencies = get_table_dependencies(schema_name)
    except Exception as e:
        raise Exception(f"Failed to get schema information for '{schema_name}': {str(e)}")
    
    if not tables:
        return []
    
    # Initialize in-degree count for each table
    in_degree = {table: 0 for table in tables}
    
    # Count incoming edges (dependencies)
    for table, deps in dependencies.items():
        for dep in deps:
            if dep in in_degree:  # Only consider dependencies within the same schema
                in_degree[table] += 1
    
    # Use Kahn's algorithm for topological sorting
    queue = deque([table for table in tables if in_degree[table] == 0])
    result = []
    
    while queue:
        current_table = queue.popleft()
        result.append(current_table)
        
        # For each table that depends on current_table, reduce its in-degree
        for table, deps in dependencies.items():
            if current_table in deps:
                in_degree[table] -= 1
                if in_degree[table] == 0:
                    queue.append(table)
    
    # Check for circular dependencies
    if len(result) != len(tables):
        remaining_tables = set(tables) - set(result)
        raise Exception(f"Circular dependency detected among tables: {remaining_tables}")
    
    return result

def get_table_structure(schema_name, table_name):
    """
    Get CREATE TABLE statement for a table
    Returns the CREATE TABLE SQL statement
    """
    if not schema_name or not table_name:
        raise ValueError("Both schema_name and table_name must be provided")
    
    conn = None
    try:
        clean_schema = schema_name.strip('"')
        conn = get_foris_connection()
        with conn.cursor() as cursor:
            # Get table structure using information_schema
            cursor.execute("""
                SELECT 
                    column_name,
                    data_type,
                    character_maximum_length,
                    is_nullable,
                    column_default,
                    ordinal_position
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (clean_schema, table_name))
            
            columns = cursor.fetchall()
            
            if not columns:
                raise Exception(f"Table '{table_name}' not found in schema '{clean_schema}'")
            
            # Get primary key
            cursor.execute("""
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY' 
                    AND tc.table_schema = %s 
                    AND tc.table_name = %s
                ORDER BY kcu.ordinal_position
            """, (clean_schema, table_name))
            
            primary_keys = [row[0] for row in cursor.fetchall()]
            
            # Get foreign key constraints
            cursor.execute("""
                SELECT 
                    tc.constraint_name,
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name,
                    rc.update_rule,
                    rc.delete_rule
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage ccu 
                    ON ccu.constraint_name = tc.constraint_name
                JOIN information_schema.referential_constraints rc 
                    ON tc.constraint_name = rc.constraint_name
                    AND tc.constraint_schema = rc.constraint_schema
                WHERE tc.constraint_type = 'FOREIGN KEY' 
                    AND tc.table_schema = %s 
                    AND tc.table_name = %s
                ORDER BY tc.constraint_name, kcu.ordinal_position
            """, (clean_schema, table_name))
            
            foreign_keys = cursor.fetchall()
            
            # Get unique constraints
            cursor.execute("""
                SELECT 
                    tc.constraint_name,
                    kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'UNIQUE' 
                    AND tc.table_schema = %s 
                    AND tc.table_name = %s
                ORDER BY tc.constraint_name, kcu.ordinal_position
            """, (clean_schema, table_name))
            
            unique_constraints = cursor.fetchall()
            
            # Get check constraints
            cursor.execute("""
                SELECT 
                    tc.constraint_name,
                    cc.check_clause
                FROM information_schema.table_constraints tc
                JOIN information_schema.check_constraints cc 
                    ON tc.constraint_name = cc.constraint_name
                WHERE tc.constraint_type = 'CHECK' 
                    AND tc.table_schema = %s 
                    AND tc.table_name = %s
                ORDER BY tc.constraint_name
            """, (clean_schema, table_name))
            
            check_constraints = cursor.fetchall()
            
            # Build CREATE TABLE statement
            create_sql = f'CREATE TABLE "{table_name}" (\n'
            column_definitions = []
            
            for col in columns:
                col_name, data_type, max_length, is_nullable, default_val, _ = col
                
                # Build column definition
                col_def = f'    "{col_name}" {data_type.upper()}'
                
                if max_length and data_type in ['character varying', 'varchar', 'character', 'char']:
                    col_def += f'({max_length})'
                
                if is_nullable == 'NO':
                    col_def += ' NOT NULL'
                
                if default_val:
                    col_def += f' DEFAULT {default_val}'
                
                column_definitions.append(col_def)
            
            # Add primary key
            if primary_keys:
                pk_cols = ', '.join([f'"{pk}"' for pk in primary_keys])
                column_definitions.append(f'    PRIMARY KEY ({pk_cols})')
            
            # Add foreign key constraints
            fk_groups = defaultdict(lambda: {
                'columns': [],
                'foreign_columns': [],
                'foreign_table': None,
                'update_rule': None,
                'delete_rule': None
            })
            
            for fk in foreign_keys:
                constraint_name, column_name, foreign_table, foreign_column, update_rule, delete_rule = fk
                fk_groups[constraint_name]['columns'].append(column_name)
                fk_groups[constraint_name]['foreign_columns'].append(foreign_column)
                fk_groups[constraint_name]['foreign_table'] = foreign_table
                fk_groups[constraint_name]['update_rule'] = update_rule
                fk_groups[constraint_name]['delete_rule'] = delete_rule
            
            for constraint_name, fk_info in fk_groups.items():
                columns_str = ', '.join([f'"{col}"' for col in fk_info['columns']])
                foreign_columns_str = ', '.join([f'"{col}"' for col in fk_info['foreign_columns']])
                fk_def = f'    CONSTRAINT "{constraint_name}" FOREIGN KEY ({columns_str}) REFERENCES "{fk_info["foreign_table"]}" ({foreign_columns_str})'
                
                # Add ON UPDATE and ON DELETE rules if they're not the default
                if fk_info['update_rule'] and fk_info['update_rule'] != 'NO ACTION':
                    fk_def += f' ON UPDATE {fk_info["update_rule"]}'
                if fk_info['delete_rule'] and fk_info['delete_rule'] != 'NO ACTION':
                    fk_def += f' ON DELETE {fk_info["delete_rule"]}'
                
                column_definitions.append(fk_def)
            
            # Add unique constraints
            uc_groups = defaultdict(list)
            for uc in unique_constraints:
                constraint_name, column_name = uc
                uc_groups[constraint_name].append(column_name)
            
            for constraint_name, columns in uc_groups.items():
                columns_str = ', '.join([f'"{col}"' for col in columns])
                column_definitions.append(f'    CONSTRAINT "{constraint_name}" UNIQUE ({columns_str})')
            
            # Add check constraints
            for cc in check_constraints:
                constraint_name, check_clause = cc
                column_definitions.append(f'    CONSTRAINT "{constraint_name}" CHECK ({check_clause})')
            
            create_sql += ',\n'.join(column_definitions)
            create_sql += '\n)'
            
            return create_sql
            
    except psycopg2.Error as e:
        raise Exception(f"Database error while getting table structure for '{table_name}': {str(e)}")
    except Exception as e:
        raise Exception(f"Error while getting table structure for '{table_name}': {str(e)}")
    finally:
        if conn:
            conn.close()

def get_table_indexes(schema_name, table_name):
    """
    Get CREATE INDEX statements for a table
    Returns list of CREATE INDEX SQL statements
    """
    conn = None
    try:
        clean_schema = schema_name.strip('"')
        conn = get_foris_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    i.indexname,
                    i.indexdef
                FROM pg_indexes i
                WHERE i.schemaname = %s 
                    AND i.tablename = %s
                    AND i.indexname NOT LIKE '%%_pkey'  -- Exclude primary key indexes
                ORDER BY i.indexname
            """, (clean_schema, table_name))
            
            indexes = []
            for row in cursor.fetchall():
                index_name, index_def = row
                # Modify the index definition to use the correct schema
                if f'ON "{table_name}"' in index_def:
                    modified_def = index_def.replace(
                        f'ON "{table_name}"',
                        f'ON "{clean_schema}"."{table_name}"'
                    )
                else:
                    modified_def = index_def.replace(
                        f'ON {table_name}',
                        f'ON "{clean_schema}"."{table_name}"'
                    )
                indexes.append(modified_def)
            
            return indexes
            
    finally:
        if conn:
            conn.close()

def get_complete_table_definition(schema_name, table_name):
    """
    Get complete table definition including structure and indexes
    Returns dict with 'create_table' and 'indexes' keys
    """
    create_table_sql = get_table_structure(schema_name, table_name)
    indexes = get_table_indexes(schema_name, table_name)
    
    return {
        'create_table': create_table_sql,
        'indexes': indexes,
        'complete_sql': create_table_sql + '\n\n' + '\n'.join(indexes) if indexes else create_table_sql
    }

def get_table_columns(schema_name, table_name):
    """Get column information for a table"""
    conn = None
    try:
        clean_schema = schema_name.strip('"')
        conn = get_foris_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (clean_schema, table_name))
            return cursor.fetchall()
    finally:
        if conn:
            conn.close()

def get_primary_key_columns(schema_name, table_name):
    """Get primary key columns for a table"""
    conn = None
    try:
        clean_schema = schema_name.strip('"')
        conn = get_foris_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY' 
                    AND tc.table_schema = %s 
                    AND tc.table_name = %s
                ORDER BY kcu.ordinal_position
            """, (clean_schema, table_name))
            return [row[0] for row in cursor.fetchall()]
    finally:
        if conn:
            conn.close()

def merge_schemas(source_schema1, source_schema2, target_schema, create_new_schema=True, merge_strategy='union'):
    """
    Merge two schemas into a target schema with improved error handling and performance
    
    Args:
        source_schema1, source_schema2: Source schema names
        target_schema: Target schema name
        create_new_schema: Whether to create new target schema
        merge_strategy: 'union' (merge all data), 'priority' (first schema wins on conflicts)
    
    Returns:
        tuple: (success, message, details)
    """
    conn = None
    try:
        # Validate input parameters
        if not source_schema1 or not source_schema2 or not target_schema:
            return False, "All schema names must be provided", None
        
        if source_schema1 == source_schema2:
            return False, "Source schemas must be different", None
        
        # Check if source schemas exist
        if not schema_exists(source_schema1):
            return False, f"Source schema '{source_schema1}' does not exist", None
        
        if not schema_exists(source_schema2):
            return False, f"Source schema '{source_schema2}' does not exist", None
        
        # Check if target schema already exists (if not creating new)
        if not create_new_schema and not schema_exists(target_schema):
            return False, f"Target schema '{target_schema}' does not exist and create_new_schema=False", None
        
        conn = get_foris_connection()
        conn.autocommit = False
        
        with conn.cursor() as cursor:
            # Create target schema if requested
            if create_new_schema:
                cursor.execute(
                    SQL("CREATE SCHEMA IF NOT EXISTS {}").format(Identifier(target_schema))
                )
            
            # Get tables from both source schemas
            try:
                schema1_tables = get_schema_tables(source_schema1)
                schema2_tables = get_schema_tables(source_schema2)
            except Exception as e:
                return False, f"Failed to retrieve tables from source schemas: {str(e)}", None
            
            if not schema1_tables and not schema2_tables:
                return False, "Both source schemas are empty - nothing to merge", None
            
            # Get creation order for both schemas
            try:
                schema1_order = get_table_creation_order(source_schema1) if schema1_tables else []
                schema2_order = get_table_creation_order(source_schema2) if schema2_tables else []
            except Exception as e:
                return False, f"Failed to determine table creation order: {str(e)}", None
            
            # Combine tables while maintaining dependency order
            all_tables = []
            seen_tables = set()
            
            # Add tables from schema1 first (for priority)
            for table in schema1_order:
                if table not in seen_tables:
                    all_tables.append(table)
                    seen_tables.add(table)
            
            # Add remaining tables from schema2
            for table in schema2_order:
                if table not in seen_tables:
                    all_tables.append(table)
                    seen_tables.add(table)
            
            # Step 1: Create all table structures in target schema
            created_tables = []
            for table in all_tables:
                try:
                    # Determine which schema to get structure from
                    source_schema = source_schema1 if table in schema1_tables else source_schema2
                    
                    # Get table structure
                    create_sql = get_table_structure(source_schema, table)
                    if not create_sql:
                        return False, f"Failed to get table structure for '{table}' from schema '{source_schema}'", None
                    
                    # Drop existing table in target schema if it exists
                    cursor.execute(
                        SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                            Identifier(target_schema), 
                            Identifier(table)
                        )
                    )
                    
                    # Create table in target schema
                    target_create_sql = create_sql.replace(
                        f'CREATE TABLE "{table}"', 
                        f'CREATE TABLE "{target_schema}"."{table}"'
                    )
                    cursor.execute(target_create_sql)
                    created_tables.append(table)
                    
                except Exception as e:
                    return False, f"Failed to create table '{table}' in target schema: {str(e)}", None
            
            # Step 2: Copy data based on merge strategy
            copied_tables = []
            for table in all_tables:
                try:
                    table_in_schema1 = table in schema1_tables
                    table_in_schema2 = table in schema2_tables
                    
                    if merge_strategy == 'union':
                        # Copy from schema1 first
                        if table_in_schema1:
                            cursor.execute(
                                SQL("INSERT INTO {}.{} SELECT * FROM {}.{}").format(
                                    Identifier(target_schema),
                                    Identifier(table),
                                    Identifier(source_schema1),
                                    Identifier(table)
                                )
                            )
                        
                        # Then copy from schema2, handling conflicts
                        if table_in_schema2:
                            if table_in_schema1:
                                # Both schemas have this table - use UPSERT
                                pk_columns = get_primary_key_columns(target_schema, table)
                                
                                if pk_columns:
                                    # Use ON CONFLICT with primary key
                                    pk_list = ', '.join([f'"{pk}"' for pk in pk_columns])
                                    
                                    # Get all columns for update
                                    all_columns = get_table_columns(target_schema, table)
                                    update_columns = [col[0] for col in all_columns if col[0] not in pk_columns]
                                    
                                    if update_columns:
                                        update_set = ', '.join([
                                            f'"{col}" = EXCLUDED."{col}"' 
                                            for col in update_columns
                                        ])
                                        
                                        cursor.execute(f'''
                                            INSERT INTO "{target_schema}"."{table}" 
                                            SELECT * FROM "{source_schema2}"."{table}"
                                            ON CONFLICT ({pk_list}) 
                                            DO UPDATE SET {update_set}
                                        ''')
                                    else:
                                        # Only primary key columns, use DO NOTHING
                                        cursor.execute(f'''
                                            INSERT INTO "{target_schema}"."{table}" 
                                            SELECT * FROM "{source_schema2}"."{table}"
                                            ON CONFLICT ({pk_list}) DO NOTHING
                                        ''')
                                else:
                                    # No primary key, use DO NOTHING to avoid duplicates
                                    cursor.execute(f'''
                                        INSERT INTO "{target_schema}"."{table}" 
                                        SELECT * FROM "{source_schema2}"."{table}"
                                        ON CONFLICT DO NOTHING
                                    ''')
                            else:
                                # Only in schema2, direct insert
                                cursor.execute(
                                    SQL("INSERT INTO {}.{} SELECT * FROM {}.{}").format(
                                        Identifier(target_schema),
                                        Identifier(table),
                                        Identifier(source_schema2),
                                        Identifier(table)
                                    )
                                )
                    
                    elif merge_strategy == 'priority':
                        # Schema1 has priority, only copy from schema2 if not in schema1
                        if table_in_schema1:
                            cursor.execute(
                                SQL("INSERT INTO {}.{} SELECT * FROM {}.{}").format(
                                    Identifier(target_schema),
                                    Identifier(table),
                                    Identifier(source_schema1),
                                    Identifier(table)
                                )
                            )
                        elif table_in_schema2:
                            cursor.execute(
                                SQL("INSERT INTO {}.{} SELECT * FROM {}.{}").format(
                                    Identifier(target_schema),
                                    Identifier(table),
                                    Identifier(source_schema2),
                                    Identifier(table)
                                )
                            )
                    
                    copied_tables.append(table)
                    
                except Exception as e:
                    return False, f"Failed to copy data for table '{table}': {str(e)}", None
            
            # Step 3: Create indexes after data insertion
            created_indexes = []
            for table in all_tables:
                try:
                    # Get indexes from the source schema that has this table
                    source_schema = source_schema1 if table in schema1_tables else source_schema2
                    indexes = get_table_indexes(source_schema, table)
                    
                    for index_sql in indexes:
                        try:
                            # Modify index to target schema
                            target_index_sql = index_sql.replace(
                                f'ON "{source_schema}"."{table}"',
                                f'ON "{target_schema}"."{table}"'
                            )
                            cursor.execute(target_index_sql)
                            created_indexes.append(f"{table}: {index_sql.split()[2]}")  # Extract index name
                        except Exception as e:
                            # Log index creation failure but don't fail the entire operation
                            print(f"Warning: Failed to create index for table '{table}': {str(e)}")
                            
                except Exception as e:
                    print(f"Warning: Failed to retrieve indexes for table '{table}': {str(e)}")
            
            try:
                conn.commit()
            except Exception as e:
                conn.rollback()
                return False, f"Failed to commit transaction: {str(e)}", None
            
            # Generate detailed results
            try:
                target_tables = get_schema_tables(target_schema)
                details = {
                    'source_schema1': source_schema1,
                    'source_schema2': source_schema2,
                    'target_schema': target_schema,
                    'merge_strategy': merge_strategy,
                    'source_schema1_tables': schema1_tables,
                    'source_schema2_tables': schema2_tables,
                    'target_schema_tables': target_tables,
                    'created_tables': created_tables,
                    'copied_tables': copied_tables,
                    'created_indexes': created_indexes,
                    'tables_only_in_schema1': [t for t in schema1_tables if t not in schema2_tables],
                    'tables_only_in_schema2': [t for t in schema2_tables if t not in schema1_tables],
                    'tables_in_both_schemas': [t for t in schema1_tables if t in schema2_tables],
                    'creation_order': all_tables
                }
            except Exception as e:
                return False, f"Failed to generate merge details: {str(e)}", None
            
            success_msg = (f"Successfully merged schemas '{source_schema1}' and '{source_schema2}' "
                          f"into '{target_schema}' using '{merge_strategy}' strategy. "
                          f"Created {len(created_tables)} tables, copied data to {len(copied_tables)} tables, "
                          f"and created {len(created_indexes)} indexes.")
            
            return True, success_msg, details
            
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        return False, f"Database error during schema merge: {str(e)}", None
    except Exception as e:
        if conn:
            conn.rollback()
        return False, f"Unexpected error during schema merge: {str(e)}", None
    finally:
        if conn:
            conn.close()

def merge_multiple_schemas(source_schemas, target_schema, create_new_schema=True, merge_strategy='union'):
    """
    Merge multiple schemas into a target schema with improved error handling
    
    Args:
        source_schemas (list): List of source schema names to merge
        target_schema (str): Target schema name
        create_new_schema (bool): Whether to create new target schema
        merge_strategy (str): 'union' (all data), 'priority' (first schema wins)
    
    Returns:
        tuple: (success, message, details)
    """
    if not source_schemas or len(source_schemas) < 2:
        return False, "At least 2 source schemas must be provided", None
    
    if not target_schema:
        return False, "Target schema name must be provided", None
    
    # Remove duplicates while preserving order
    unique_schemas = []
    for schema in source_schemas:
        if schema not in unique_schemas:
            unique_schemas.append(schema)
    
    if len(unique_schemas) < 2:
        return False, "At least 2 unique source schemas must be provided", None
    
    conn = None
    try:
        # Validate all source schemas exist
        for schema in unique_schemas:
            if not schema_exists(schema):
                return False, f"Source schema '{schema}' does not exist", None
        
        # Check target schema if not creating new
        if not create_new_schema and not schema_exists(target_schema):
            return False, f"Target schema '{target_schema}' does not exist and create_new_schema=False", None
        
        conn = get_foris_connection()
        conn.autocommit = False
        
        with conn.cursor() as cursor:
            # Create target schema if requested
            if create_new_schema:
                cursor.execute(
                    SQL("CREATE SCHEMA IF NOT EXISTS {}").format(Identifier(target_schema))
                )
            
            # Get all unique tables across all schemas
            all_tables = []
            schema_tables = {}
            
            for schema in unique_schemas:
                try:
                    tables = get_schema_tables(schema)
                    schema_tables[schema] = tables
                    
                    # Add tables while maintaining order from first schema that has them
                    for table in get_table_creation_order(schema) if tables else []:
                        if table not in all_tables:
                            all_tables.append(table)
                except Exception as e:
                    return False, f"Failed to get tables from schema '{schema}': {str(e)}", None
            
            if not all_tables:
                return False, "All source schemas are empty - nothing to merge", None
            
            # Step 1: Create tables
            created_tables = []
            for table in all_tables:
                try:
                    # Find first schema that has this table
                    source_schema = None
                    for schema in unique_schemas:
                        if table in schema_tables[schema]:
                            source_schema = schema
                            break
                    
                    if source_schema:
                        # Drop existing table in target schema
                        cursor.execute(
                            SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                                Identifier(target_schema), 
                                Identifier(table)
                            )
                        )
                        
                        # Get and execute create statement
                        create_sql = get_table_structure(source_schema, table)
                        target_create_sql = create_sql.replace(
                            f'CREATE TABLE "{table}"', 
                            f'CREATE TABLE "{target_schema}"."{table}"'
                        )
                        cursor.execute(target_create_sql)
                        created_tables.append(table)
                        
                except Exception as e:
                    return False, f"Failed to create table '{table}' in target schema: {str(e)}", None
            
            # Step 2: Copy data based on strategy
            copied_data = {}
            for table in all_tables:
                try:
                    schemas_with_table = [s for s in unique_schemas if table in schema_tables[s]]
                    copied_data[table] = []
                    
                    if merge_strategy == 'priority':
                        # Only use data from first schema that has the table
                        if schemas_with_table:
                            source_schema = schemas_with_table[0]
                            cursor.execute(
                                SQL("INSERT INTO {}.{} SELECT * FROM {}.{}").format(
                                    Identifier(target_schema),
                                    Identifier(table),
                                    Identifier(source_schema),
                                    Identifier(table)
                                )
                            )
                            copied_data[table].append(source_schema)
                    
                    elif merge_strategy == 'union':
                        # Merge data from all schemas that have this table
                        for i, source_schema in enumerate(schemas_with_table):
                            if i == 0:
                                # First schema: direct insert
                                cursor.execute(
                                    SQL("INSERT INTO {}.{} SELECT * FROM {}.{}").format(
                                        Identifier(target_schema),
                                        Identifier(table),
                                        Identifier(source_schema),
                                        Identifier(table)
                                    )
                                )
                                copied_data[table].append(source_schema)
                            else:
                                # Subsequent schemas: handle conflicts
                                pk_columns = get_primary_key_columns(target_schema, table)
                                
                                if pk_columns:
                                    # Use UPSERT
                                    pk_list = ', '.join([f'"{pk}"' for pk in pk_columns])
                                    all_columns = get_table_columns(target_schema, table)
                                    update_columns = [col[0] for col in all_columns if col[0] not in pk_columns]
                                    
                                    if update_columns:
                                        update_set = ', '.join([
                                            f'"{col}" = EXCLUDED."{col}"' 
                                            for col in update_columns
                                        ])
                                        
                                        cursor.execute(f'''
                                            INSERT INTO "{target_schema}"."{table}" 
                                            SELECT * FROM "{source_schema}"."{table}"
                                            ON CONFLICT ({pk_list}) 
                                            DO UPDATE SET {update_set}
                                        ''')
                                    else:
                                        cursor.execute(f'''
                                            INSERT INTO "{target_schema}"."{table}" 
                                            SELECT * FROM "{source_schema}"."{table}"
                                            ON CONFLICT ({pk_list}) DO NOTHING
                                        ''')
                                else:
                                    # No primary key
                                    cursor.execute(f'''
                                        INSERT INTO "{target_schema}"."{table}" 
                                        SELECT * FROM "{source_schema}"."{table}"
                                        ON CONFLICT DO NOTHING
                                    ''')
                                
                                copied_data[table].append(source_schema)
                    
                except Exception as e:
                    return False, f"Failed to merge data for table '{table}': {str(e)}", None
            
            # Step 3: Create indexes
            created_indexes = []
            for table in all_tables:
                try:
                    # Get indexes from first schema that has this table
                    source_schema = None
                    for schema in unique_schemas:
                        if table in schema_tables[schema]:
                            source_schema = schema
                            break
                    
                    if source_schema:
                        indexes = get_table_indexes(source_schema, table)
                        for index_sql in indexes:
                            try:
                                target_index_sql = index_sql.replace(
                                    f'ON "{source_schema}"."{table}"',
                                    f'ON "{target_schema}"."{table}"'
                                )
                                cursor.execute(target_index_sql)
                                created_indexes.append(f"{table}: {index_sql.split()[2] if len(index_sql.split()) > 2 else 'unknown'}")
                            except Exception as e:
                                print(f"Warning: Failed to create index for table '{table}': {str(e)}")
                                
                except Exception as e:
                    print(f"Warning: Failed to process indexes for table '{table}': {str(e)}")
            
            try:
                conn.commit()
            except Exception as e:
                conn.rollback()
                return False, f"Failed to commit transaction: {str(e)}", None
            
            # Generate results
            try:
                target_tables = get_schema_tables(target_schema)
                details = {
                    'source_schemas': unique_schemas,
                    'target_schema': target_schema,
                    'merge_strategy': merge_strategy,
                    'schema_tables': schema_tables,
                    'target_schema_tables': target_tables,
                    'created_tables': created_tables,
                    'copied_data': copied_data,
                    'created_indexes': created_indexes,
                    'merged_tables': all_tables,
                    'creation_order': all_tables
                }
            except Exception as e:
                return False, f"Failed to generate merge details: {str(e)}", None
            
            success_msg = (f"Successfully merged {len(unique_schemas)} schemas into '{target_schema}' "
                          f"using '{merge_strategy}' strategy. Created {len(created_tables)} tables "
                          f"and {len(created_indexes)} indexes.")
            
            return True, success_msg, details
            
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        return False, f"Database error during schema merge: {str(e)}", None
    except Exception as e:
        if conn:
            conn.rollback()
        return False, f"Unexpected error during schema merge: {str(e)}", None
    finally:
        if conn:
            conn.close()

def schema_exists_and_has_tables(schema_name):
    """
    Check if schema exists and has tables
    Returns (exists, table_count, tables)
    """
    if not schema_exists(schema_name):
        return False, 0, []
    
    tables = get_schema_tables(schema_name)
    return True, len(tables), tables

def categorize_error(error):
    """
    Categorize different types of errors for better error handling
    Returns (error_type, user_friendly_message, technical_details)
    """
    error_str = str(error).lower()
    
    # Database connection errors
    if any(keyword in error_str for keyword in ['connection', 'connect', 'timeout', 'refused']):
        return 'CONNECTION_ERROR', 'Database connection failed. Please check your network connection and database settings.', str(error)
    
    # Authentication errors
    if any(keyword in error_str for keyword in ['authentication', 'password', 'permission', 'access denied']):
        return 'AUTHENTICATION_ERROR', 'Database authentication failed. Please check your credentials.', str(error)
    
    # Schema/table not found errors
    if any(keyword in error_str for keyword in ['does not exist', 'not found', 'schema', 'table']):
        return 'NOT_FOUND_ERROR', 'The requested schema or table does not exist.', str(error)
    
    # Foreign key constraint violations
    if any(keyword in error_str for keyword in ['foreign key', 'constraint', 'violation']):
        return 'CONSTRAINT_ERROR', 'Foreign key constraint violation. Please check data integrity.', str(error)
    
    # Unique constraint violations
    if any(keyword in error_str for keyword in ['unique', 'duplicate', 'already exists']):
        return 'UNIQUE_CONSTRAINT_ERROR', 'Unique constraint violation. Duplicate data detected.', str(error)
    
    # Syntax errors
    if any(keyword in error_str for keyword in ['syntax', 'invalid', 'malformed']):
        return 'SYNTAX_ERROR', 'SQL syntax error. Please check your SQL statements.', str(error)
    
    # Permission errors
    if any(keyword in error_str for keyword in ['permission', 'privilege', 'denied']):
        return 'PERMISSION_ERROR', 'Insufficient permissions to perform this operation.', str(error)
    
    # Lock/timeout errors
    if any(keyword in error_str for keyword in ['lock', 'timeout', 'deadlock']):
        return 'LOCK_ERROR', 'Database lock or timeout error. Please try again.', str(error)
    
    # Data type errors
    if any(keyword in error_str for keyword in ['type', 'cast', 'conversion']):
        return 'DATA_TYPE_ERROR', 'Data type mismatch or conversion error.', str(error)
    
    # Default case
    return 'UNKNOWN_ERROR', 'An unexpected error occurred.', str(error)

def get_all_schemas():
    """
    Get list of all schemas in the database
    Returns list of schema names
    """
    conn = None
    try:
        conn = get_foris_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                AND schema_name NOT LIKE 'pg_%'
                ORDER BY schema_name
            """)
            return [row[0] for row in cursor.fetchall()]
    finally:
        if conn:
            conn.close()

def get_schemas_with_info():
    """
    Get list of all schemas with additional information
    Returns list of dictionaries with schema info
    """
    conn = None
    try:
        conn = get_foris_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    s.schema_name,
                    COUNT(t.table_name) as table_count
                FROM information_schema.schemata s
                LEFT JOIN information_schema.tables t 
                    ON s.schema_name = t.table_schema 
                    AND t.table_type = 'BASE TABLE'
                WHERE s.schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                AND s.schema_name NOT LIKE 'pg_%'
                GROUP BY s.schema_name
                ORDER BY s.schema_name
            """)
            
            schemas = []
            for row in cursor.fetchall():
                schema_name, table_count = row
                
                # Get size information separately (more reliable)
                try:
                    cursor.execute("""
                        SELECT COALESCE(SUM(pg_total_relation_size(quote_ident(%s)||'.'||quote_ident(table_name))), 0)
                        FROM information_schema.tables
                        WHERE table_schema = %s AND table_type = 'BASE TABLE'
                    """, (schema_name, schema_name))
                    
                    total_size_bytes = cursor.fetchone()[0] or 0
                except:
                    total_size_bytes = 0
                
                schemas.append({
                    'schema_name': schema_name,
                    'table_count': table_count,
                    'total_size_bytes': total_size_bytes,
                    'total_size_mb': round(total_size_bytes / (1024 * 1024), 2) if total_size_bytes else 0,
                    'exists': True
                })
            
            return schemas
    finally:
        if conn:
            conn.close()

def merge_schemas_incremental(source_schemas, target_schema, create_new_schema=True, batch_size=1000):
    """
    Merge multiple schemas incrementally with batching for large tables
    
    Args:
        source_schemas (list): List of source schema names to merge
        target_schema (str): Target schema name
        create_new_schema (bool): Whether to create new target schema
        batch_size (int): Number of rows to process in each batch
    
    Returns:
        tuple: (success, message, details)
    """
    if not source_schemas or len(source_schemas) < 2:
        return False, "At least 2 source schemas must be provided", None
    
    conn = None
    try:
        # Validate schemas
        for schema in source_schemas:
            if not schema_exists(schema):
                return False, f"Source schema '{schema}' does not exist", None
        
        conn = get_foris_connection()
        conn.autocommit = False
        
        with conn.cursor() as cursor:
            # Create target schema if requested
            if create_new_schema:
                cursor.execute(
                    SQL("CREATE SCHEMA IF NOT EXISTS {}").format(Identifier(target_schema))
                )
            
            # Get all tables and their row counts
            table_info = {}
            for schema in source_schemas:
                tables = get_schema_tables(schema)
                for table in tables:
                    if table not in table_info:
                        table_info[table] = {}
                    
                    # Get row count for performance estimation
                    cursor.execute(
                        SQL("SELECT COUNT(*) FROM {}.{}").format(
                            Identifier(schema), 
                            Identifier(table)
                        )
                    )
                    row_count = cursor.fetchone()[0]
                    table_info[table][schema] = row_count
            
            # Process tables in order of size (smallest first for better memory usage)
            sorted_tables = sorted(table_info.keys(), key=lambda t: sum(table_info[t].values()))
            
            for table in sorted_tables:
                try:
                    # Create table if it doesn't exist
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables 
                            WHERE table_schema = %s AND table_name = %s
                        )
                    """, (target_schema, table))
                    
                    if not cursor.fetchone()[0]:
                        # Get structure from first schema that has this table
                        source_schema = next(schema for schema in source_schemas if schema in table_info[table])
                        create_sql = get_table_structure(source_schema, table)
                        if create_sql:
                            target_create_sql = create_sql.replace(
                                f'CREATE TABLE "{table}"', 
                                f'CREATE TABLE "{target_schema}"."{table}"'
                            )
                            cursor.execute(target_create_sql)
                    
                    # Merge data in batches
                    for schema in source_schemas:
                        if schema in table_info[table]:
                            total_rows = table_info[table][schema]
                            
                            if total_rows > batch_size:
                                # Process in batches
                                offset = 0
                                while offset < total_rows:
                                    cursor.execute(
                                        SQL("INSERT INTO {}.{} SELECT * FROM {}.{} LIMIT %s OFFSET %s ON CONFLICT DO NOTHING").format(
                                            Identifier(target_schema),
                                            Identifier(table),
                                            Identifier(schema),
                                            Identifier(table)
                                        ),
                                        (batch_size, offset)
                                    )
                                    offset += batch_size
                            else:
                                # Small table, process all at once
                                cursor.execute(
                                    SQL("INSERT INTO {}.{} SELECT * FROM {}.{} ON CONFLICT DO NOTHING").format(
                                        Identifier(target_schema),
                                        Identifier(table),
                                        Identifier(schema),
                                        Identifier(table)
                                    )
                                )
                    
                except Exception as e:
                    return False, f"Failed to merge table '{table}': {str(e)}", None
            
            try:
                conn.commit()
            except Exception as e:
                conn.rollback()
                return False, f"Failed to commit transaction: {str(e)}", None
            
            details = {
                'source_schemas': source_schemas,
                'target_schema': target_schema,
                'table_info': table_info,
                'batch_size': batch_size,
                'processed_tables': len(sorted_tables)
            }
            
            return True, f"Successfully merged {len(source_schemas)} schemas into '{target_schema}' incrementally", details
            
    except Exception as e:
        if conn:
            conn.rollback()
        return False, f"Error during incremental merge: {str(e)}", None
    finally:
        if conn:
            conn.close()