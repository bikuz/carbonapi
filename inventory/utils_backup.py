import os
import zipfile
import tempfile
import re
from django.conf import settings
from carbonapi.database.connection import get_foris_connection
import psycopg2
from psycopg2.sql import SQL

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

# def schema_exists(schema_name):
#     """Check if schema exists in the database"""
#     conn = None
#     try:
#         conn = psycopg2.connect(**settings.DATABASES['default'])
#         with conn.cursor() as cursor:
#             cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s", (schema_name,))
#             return bool(cursor.fetchone())
#     finally:
#         if conn:
#             conn.close()

# def execute_sql_script(sql_content, schema_name=None):
#     """Execute SQL script with optional schema context"""
#     conn = None
#     try:
#         conn = psycopg2.connect(**settings.DATABASES['default'])
#         conn.autocommit = False
#         with conn.cursor() as cursor:
#             if schema_name:
#                 cursor.execute(f"SET search_path TO {schema_name}")
#             cursor.execute(sql_content)
#         conn.commit()
#         return True, None
#     except Exception as e:
#         if conn:
#             conn.rollback()
#         return False, str(e)
#     finally:
#         if conn:
#             conn.close()


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
            # print(f'NFI schema:{clean_name}', result)  
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
                # Drop schema cascade to remove all contained objects
                cursor.execute(f'DROP SCHEMA IF EXISTS "{clean_schema_name}" CASCADE')
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
                    cursor.execute(f"SET search_path TO {schema_name}")
                except Exception as e:
                    return False, f"Failed to set search path to schema '{schema_name}': {str(e)}"

            # # Split SQL by semicolons to execute commands one by one
            # for command in sql_content.split(';'):
            #     if command.strip():
            #         cursor.execute(command)

            # Use psycopg2's execute_batch which handles semicolons properly
            try:
                cursor.execute(SQL(sql_content))
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
    Returns dict with table as key and list of dependent tables as value
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
            
            dependencies = {}
            for row in cursor.fetchall():
                table, referenced_table = row
                if table not in dependencies:
                    dependencies[table] = []
                dependencies[table].append(referenced_table)
            
            return dependencies
    finally:
        if conn:
            conn.close()

def get_table_creation_order(schema_name):
    """
    Get tables in order of creation (dependencies first)
    Returns list of tables in correct creation order
    """
    if not schema_name:
        raise ValueError("Schema name must be provided")
    
    try:
        tables = get_schema_tables(schema_name)
        dependencies = get_table_dependencies(schema_name)
    except Exception as e:
        raise Exception(f"Failed to get schema information for '{schema_name}': {str(e)}")
    
    # Create dependency graph
    graph = {table: set(dependencies.get(table, [])) for table in tables}
    
    # Topological sort to get creation order
    result = []
    visited = set()
    temp_visited = set()
    
    def visit(table):
        if table in temp_visited:
            raise Exception(f"Circular dependency detected involving table: {table}")
        if table in visited:
            return
        
        temp_visited.add(table)
        
        try:
            for dependency in graph.get(table, []):
                if dependency in tables:  # Only consider dependencies within the same schema
                    visit(dependency)
        except Exception as e:
            temp_visited.remove(table)
            raise Exception(f"Error processing dependencies for table '{table}': {str(e)}")
        
        temp_visited.remove(table)
        visited.add(table)
        result.append(table)
    
    try:
        for table in tables:
            if table not in visited:
                visit(table)
    except Exception as e:
        raise Exception(f"Failed to determine table creation order: {str(e)}")
    
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
            # Get table structure using pg_dump approach
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
            
            # Get primary key
            cursor.execute("""
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
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
                JOIN information_schema.constraint_column_usage ccu 
                    ON ccu.constraint_name = tc.constraint_name
                    AND ccu.ordinal_position = kcu.ordinal_position
                JOIN information_schema.referential_constraints rc 
                    ON tc.constraint_name = rc.constraint_name
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
                col_def = f'    "{col_name}" {data_type}'
                
                if max_length:
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
            fk_groups = {}
            for fk in foreign_keys:
                constraint_name, column_name, foreign_table, foreign_column, update_rule, delete_rule = fk
                if constraint_name not in fk_groups:
                    fk_groups[constraint_name] = {
                        'columns': [],
                        'foreign_columns': [],
                        'foreign_table': foreign_table,
                        'update_rule': update_rule,
                        'delete_rule': delete_rule
                    }
                fk_groups[constraint_name]['columns'].append(column_name)
                fk_groups[constraint_name]['foreign_columns'].append(foreign_column)
            
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
            uc_groups = {}
            for uc in unique_constraints:
                constraint_name, column_name = uc
                if constraint_name not in uc_groups:
                    uc_groups[constraint_name] = []
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
                modified_def = index_def.replace(
                    f'CREATE INDEX "{index_name}" ON "{table_name}"',
                    f'CREATE INDEX "{index_name}" ON "{clean_schema}"."{table_name}"'
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

def merge_schemas(source_schema1, source_schema2, target_schema, create_new_schema=True):
    """
    Merge two schemas into a target schema
    Returns (success, message, details)
    
    Process:
    1. Create tables in dependency order (parent tables first)
    2. Insert data from schema1 in dependency order (parent tables first)
    3. Insert data from schema2 in dependency order (parent tables first)
    4. Create indexes after all data is inserted
    
    This ensures foreign key constraints are respected throughout the process.
    """
    conn = None
    try:
        # Validate input parameters
        if not source_schema1 or not source_schema2 or not target_schema:
            return False, "All schema names must be provided", None
        
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
                cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{target_schema}"')
            
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
                schema1_order = get_table_creation_order(source_schema1)
                schema2_order = get_table_creation_order(source_schema2)
            except Exception as e:
                return False, f"Failed to determine table creation order: {str(e)}", None
            
            # Combine and deduplicate tables, maintaining order
            all_tables = []
            for table in schema1_order + schema2_order:
                if table not in all_tables:
                    all_tables.append(table)
            
            # Step 1: Create all table structures in target schema
            for table in all_tables:
                try:
                    # Get table structure from schema1 (assuming both schemas have same structure)
                    create_sql = get_table_structure(source_schema1, table)
                    if not create_sql:
                        return False, f"Failed to get table structure for '{table}' from schema '{source_schema1}'", None
                    
                    # Create table in target schema
                    cursor.execute(f'DROP TABLE IF EXISTS "{target_schema}"."{table}" CASCADE')
                    cursor.execute(create_sql.replace(f'CREATE TABLE "{table}"', f'CREATE TABLE "{target_schema}"."{table}"'))
                except Exception as e:
                    return False, f"Failed to create table '{table}' in target schema: {str(e)}", None
            
            # Step 2: Copy data from schema1 first (in dependency order)
            for table in schema1_order:
                if table in schema1_tables:
                    try:
                        cursor.execute(f'''
                            INSERT INTO "{target_schema}"."{table}" 
                            SELECT * FROM "{source_schema1}"."{table}"
                        ''')
                    except Exception as e:
                        return False, f"Failed to copy data from schema1 table '{table}': {str(e)}", None
            
            # Step 3: Copy data from schema2 (this will overwrite/merge with schema1 data)
            # Use dependency order to avoid FK violations
            for table in schema2_order:
                if table in schema2_tables:
                    # For tables that exist in both schemas, we need to handle conflicts
                    if table in schema1_tables:
                        try:
                            # Get primary key columns for conflict resolution
                            cursor.execute(f"""
                                SELECT c.column_name
                                FROM information_schema.table_constraints tc
                                JOIN information_schema.constraint_column_usage ccu 
                                    ON tc.constraint_name = ccu.constraint_name
                                JOIN information_schema.columns c 
                                    ON ccu.table_name = c.table_name 
                                    AND ccu.column_name = c.column_name
                                WHERE tc.constraint_type = 'PRIMARY KEY' 
                                    AND tc.table_schema = '{target_schema}' 
                                    AND tc.table_name = '{table}'
                                ORDER BY c.ordinal_position
                            """)
                            
                            pk_columns = [row[0] for row in cursor.fetchall()]
                            
                            if pk_columns:
                                # Use UPSERT with primary key conflict resolution
                                pk_list = ', '.join([f'"{pk}"' for pk in pk_columns])
                                update_set = ', '.join([
                                    f'"{col}" = EXCLUDED."{col}"' 
                                    for col in get_schema_tables(target_schema) if col in pk_columns
                                ])
                                
                                cursor.execute(f'''
                                    INSERT INTO "{target_schema}"."{table}" 
                                    SELECT * FROM "{source_schema2}"."{table}"
                                    ON CONFLICT ({pk_list}) 
                                    DO UPDATE SET {update_set}
                                ''')
                            else:
                                # No primary key, just insert (may create duplicates)
                                cursor.execute(f'''
                                    INSERT INTO "{target_schema}"."{table}" 
                                    SELECT * FROM "{source_schema2}"."{table}"
                                ''')
                        except Exception as e:
                            return False, f"Failed to merge data for table '{table}' (conflict resolution failed): {str(e)}", None
                    else:
                        # Table only exists in schema2, just copy
                        try:
                            cursor.execute(f'''
                                INSERT INTO "{target_schema}"."{table}" 
                                SELECT * FROM "{source_schema2}"."{table}"
                            ''')
                        except Exception as e:
                            return False, f"Failed to copy data from schema2 table '{table}': {str(e)}", None
            
            # Step 4: Create indexes after data insertion to avoid FK violations during index creation
            for table in all_tables:
                try:
                    indexes = get_table_indexes(source_schema1, table)
                    for index_sql in indexes:
                        try:
                            # Modify index to target schema
                            target_index_sql = index_sql.replace(
                                f'ON "{source_schema1}"."{table}"',
                                f'ON "{target_schema}"."{table}"'
                            )
                            cursor.execute(target_index_sql)
                        except Exception as e:
                            # Log index creation failure but don't fail the entire operation
                            print(f"Warning: Failed to create index for table '{table}': {str(e)}")
                except Exception as e:
                    return False, f"Failed to retrieve indexes for table '{table}': {str(e)}", None
            
            try:
                conn.commit()
            except Exception as e:
                conn.rollback()
                return False, f"Failed to commit transaction: {str(e)}", None
            
            try:
                details = {
                    'source_schema1_tables': schema1_tables,
                    'source_schema2_tables': schema2_tables,
                    'target_schema_tables': get_schema_tables(target_schema),
                    'merged_tables': all_tables,
                    'creation_order': all_tables
                }
            except Exception as e:
                return False, f"Failed to generate merge details: {str(e)}", None
            
            return True, f"Successfully merged schemas '{source_schema1}' and '{source_schema2}' into '{target_schema}'", details
            
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

def merge_multiple_schemas(source_schemas, target_schema, create_new_schema=True, merge_strategy='union'):
    """
    Merge multiple schemas into a target schema with better performance
    
    Args:
        source_schemas (list): List of source schema names to merge
        target_schema (str): Target schema name
        create_new_schema (bool): Whether to create new target schema
        merge_strategy (str): 'union' (all data), 'intersection' (common data), 'priority' (first schema wins)
    
    Returns:
        tuple: (success, message, details)
    """
    if not source_schemas or len(source_schemas) < 2:
        return False, "At least 2 source schemas must be provided", None
    
    if not target_schema:
        return False, "Target schema name must be provided", None
    
    conn = None
    try:
        # Validate all source schemas exist
        for schema in source_schemas:
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
                cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{target_schema}"')
            
            # Get all unique tables across all schemas
            all_tables = set()
            schema_tables = {}
            
            for schema in source_schemas:
                try:
                    tables = get_schema_tables(schema)
                    schema_tables[schema] = tables
                    all_tables.update(tables)
                except Exception as e:
                    return False, f"Failed to get tables from schema '{schema}': {str(e)}", None
            
            if not all_tables:
                return False, "All source schemas are empty - nothing to merge", None
            
            # Get creation order for the first schema (assuming similar structure)
            try:
                creation_order = get_table_creation_order(source_schemas[0])
                # Filter to only include tables that exist in at least one schema
                creation_order = [table for table in creation_order if table in all_tables]
            except Exception as e:
                return False, f"Failed to determine table creation order: {str(e)}", None
            
            # Step 1: Create tables only if they don't exist (more efficient)
            for table in creation_order:
                try:
                    # Check if table already exists in target schema
                    cursor.execute(f"""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables 
                            WHERE table_schema = %s AND table_name = %s
                        )
                    """, (target_schema, table))
                    
                    table_exists = cursor.fetchone()[0]
                    
                    if not table_exists:
                        # Get table structure from first schema that has this table
                        source_schema = None
                        for schema in source_schemas:
                            if table in schema_tables[schema]:
                                source_schema = schema
                                break
                        
                        if source_schema:
                            create_sql = get_table_structure(source_schema, table)
                            if create_sql:
                                cursor.execute(create_sql.replace(
                                    f'CREATE TABLE "{table}"', 
                                    f'CREATE TABLE "{target_schema}"."{table}"'
                                ))
                except Exception as e:
                    return False, f"Failed to create table '{table}' in target schema: {str(e)}", None
            
            # Step 2: Merge data efficiently based on strategy
            for table in creation_order:
                try:
                    # Get schemas that have this table
                    schemas_with_table = [schema for schema in source_schemas if table in schema_tables[schema]]
                    
                    if not schemas_with_table:
                        continue
                    
                    if merge_strategy == 'priority':
                        # Only use data from first schema that has the table
                        source_schema = schemas_with_table[0]
                        cursor.execute(f'''
                            INSERT INTO "{target_schema}"."{table}" 
                            SELECT * FROM "{source_schema}"."{table}"
                            ON CONFLICT DO NOTHING
                        ''')
                    
                    elif merge_strategy == 'union':
                        # Merge data from all schemas, handling conflicts
                        for i, source_schema in enumerate(schemas_with_table):
                            if i == 0:
                                # First schema: direct insert
                                cursor.execute(f'''
                                    INSERT INTO "{target_schema}"."{table}" 
                                    SELECT * FROM "{source_schema}"."{table}"
                                ''')
                            else:
                                # Subsequent schemas: use UPSERT
                                try:
                                    # Get primary key for conflict resolution
                                    cursor.execute(f"""
                                        SELECT c.column_name
                                        FROM information_schema.table_constraints tc
                                        JOIN information_schema.constraint_column_usage ccu 
                                            ON tc.constraint_name = ccu.constraint_name
                                        JOIN information_schema.columns c 
                                            ON ccu.table_name = c.table_name 
                                            AND ccu.column_name = c.column_name
                                        WHERE tc.constraint_type = 'PRIMARY KEY' 
                                            AND tc.table_schema = %s 
                                            AND tc.table_name = %s
                                        ORDER BY c.ordinal_position
                                    """, (target_schema, table))
                                    
                                    pk_columns = [row[0] for row in cursor.fetchall()]
                                    
                                    if pk_columns:
                                        # Use UPSERT with primary key conflict resolution
                                        pk_list = ', '.join([f'"{pk}"' for pk in pk_columns])
                                        cursor.execute(f'''
                                            INSERT INTO "{target_schema}"."{table}" 
                                            SELECT * FROM "{source_schema}"."{table}"
                                            ON CONFLICT ({pk_list}) 
                                            DO UPDATE SET 
                                                {', '.join([f'"{col}" = EXCLUDED."{col}"' for col in pk_columns])}
                                        ''')
                                    else:
                                        # No primary key, use DO NOTHING to avoid duplicates
                                        cursor.execute(f'''
                                            INSERT INTO "{target_schema}"."{table}" 
                                            SELECT * FROM "{source_schema}"."{table}"
                                            ON CONFLICT DO NOTHING
                                        ''')
                                except Exception as e:
                                    # Log conflict resolution failure but continue
                                    print(f"Warning: Conflict resolution failed for table '{table}' from schema '{source_schema}': {str(e)}")
                    
                    elif merge_strategy == 'intersection':
                        # Only insert data that exists in ALL schemas (complex, requires temporary tables)
                        # This is a simplified version - for complex intersection logic, 
                        # we'd need to create temporary tables and use INTERSECT
                        if len(schemas_with_table) == len(source_schemas):
                            # All schemas have this table, use first schema's data
                            source_schema = schemas_with_table[0]
                            cursor.execute(f'''
                                INSERT INTO "{target_schema}"."{table}" 
                                SELECT * FROM "{source_schema}"."{table}"
                                ON CONFLICT DO NOTHING
                            ''')
                    
                except Exception as e:
                    return False, f"Failed to merge data for table '{table}': {str(e)}", None
            
            # Step 3: Create indexes efficiently (only if they don't exist)
            for table in creation_order:
                try:
                    # Check if table has indexes
                    cursor.execute(f"""
                        SELECT COUNT(*) FROM pg_indexes 
                        WHERE schemaname = %s AND tablename = %s
                    """, (target_schema, table))
                    
                    index_count = cursor.fetchone()[0]
                    
                    if index_count == 0:
                        # Only create indexes if none exist
                        source_schema = None
                        for schema in source_schemas:
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
                                except Exception as e:
                                    print(f"Warning: Failed to create index for table '{table}': {str(e)}")
                except Exception as e:
                    print(f"Warning: Failed to process indexes for table '{table}': {str(e)}")
            
            try:
                conn.commit()
            except Exception as e:
                conn.rollback()
                return False, f"Failed to commit transaction: {str(e)}", None
            
            try:
                details = {
                    'source_schemas': source_schemas,
                    'target_schema': target_schema,
                    'merge_strategy': merge_strategy,
                    'schema_tables': schema_tables,
                    'target_schema_tables': get_schema_tables(target_schema),
                    'merged_tables': list(all_tables),
                    'creation_order': creation_order
                }
            except Exception as e:
                return False, f"Failed to generate merge details: {str(e)}", None
            
            return True, f"Successfully merged {len(source_schemas)} schemas into '{target_schema}' using {merge_strategy} strategy", details
            
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
                    COUNT(t.table_name) as table_count,
                    COALESCE(SUM(pg_total_relation_size(s.schema_name||'.'||t.table_name)), 0) as total_size_bytes
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
                schema_name, table_count, total_size_bytes = row
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
                cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{target_schema}"')
            
            # Get all tables and their row counts
            table_info = {}
            for schema in source_schemas:
                tables = get_schema_tables(schema)
                for table in tables:
                    if table not in table_info:
                        table_info[table] = {}
                    
                    # Get row count for performance estimation
                    cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
                    row_count = cursor.fetchone()[0]
                    table_info[table][schema] = row_count
            
            # Process tables in order of size (smallest first for better memory usage)
            sorted_tables = sorted(table_info.keys(), key=lambda t: sum(table_info[t].values()))
            
            for table in sorted_tables:
                try:
                    # Create table if it doesn't exist
                    cursor.execute(f"""
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
                            cursor.execute(create_sql.replace(
                                f'CREATE TABLE "{table}"', 
                                f'CREATE TABLE "{target_schema}"."{table}"'
                            ))
                    
                    # Merge data in batches
                    for schema in source_schemas:
                        if schema in table_info[table]:
                            total_rows = table_info[table][schema]
                            
                            if total_rows > batch_size:
                                # Process in batches
                                offset = 0
                                while offset < total_rows:
                                    cursor.execute(f'''
                                        INSERT INTO "{target_schema}"."{table}" 
                                        SELECT * FROM "{schema}"."{table}"
                                        LIMIT %s OFFSET %s
                                        ON CONFLICT DO NOTHING
                                    ''', (batch_size, offset))
                                    offset += batch_size
                            else:
                                # Small table, process all at once
                                cursor.execute(f'''
                                    INSERT INTO "{target_schema}"."{table}" 
                                    SELECT * FROM "{schema}"."{table}"
                                    ON CONFLICT DO NOTHING
                                ''')
                    
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