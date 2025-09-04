"""
Data import utilities for MRV projects.
Handles importing data from foris_connection to project schemas in default connection.
"""

import logging
from typing import Dict, List, Tuple, Optional, Any
from django.db import connections, transaction
from django.conf import settings
from psycopg2.sql import SQL, Identifier, Literal
from carbonapi.database.connection import get_foris_connection

try:
    import pandas as pd
except ImportError:
    pd = None
from datetime import datetime

from .models import Project, ProjectDataImportManager

logger = logging.getLogger(__name__)

class DataImportError(Exception):
    """Custom exception for data import errors"""
    pass

class DataImportService:
    """Service class for handling data imports between databases"""
    
    def __init__(self):
        self.default_connection = connections['default']
        self.foris_connection = None
        try:
            self.foris_connection = get_foris_connection()
        except Exception as e:
            raise DataImportError(f"Failed to connect to foris database: {str(e)}")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def close(self):
        """Close the foris database connection"""
        if self.foris_connection:
            try:
                self.foris_connection.close()
            except:
                pass  # Ignore errors when closing
    
    def get_foris_table_preview(self, schema_name: str, table_name: str, limit: int = 5) -> Dict[str, Any]:
        """
        Get preview data from a table in foris_connection
        For tree_and_climber table, uses join with plot table
        
        Args:
            schema_name: Source schema name
            table_name: Source table name  
            limit: Number of sample rows to return
            
        Returns:
            Dictionary containing preview data
        """
        try:
            with self.foris_connection.cursor() as cursor:
                # Check if this is tree_and_climber table - use join query
                if table_name == 'tree_and_climber':
                    # Use the join query for preview with optimized safe casting
                    join_query = f"""
                    SELECT 
                        -- Plot table fields with safe casting
                        p.plot_id_::BIGINT as plot_id,
                        CASE 
                            WHEN p.col ~ '^-?\\d+$' THEN p.col::BIGINT 
                            ELSE NULL::BIGINT 
                        END as plot_col,
                        CASE 
                            WHEN p."row" ~ '^-?\\d+$' THEN p."row"::BIGINT 
                            ELSE NULL::BIGINT 
                        END as plot_row,
                        CASE 
                            WHEN p."number" ~ '^-?\\d+$' THEN p."number"::BIGINT 
                            ELSE NULL::BIGINT 
                        END as plot_number,
                        p.utm_coordinate_x::REAL as plot_x,
                        p.utm_coordinate_y::REAL as plot_y,
                        CASE 
                            WHEN p.phy_zone ~ '^-?\\d+$' THEN p.phy_zone::INTEGER 
                            ELSE NULL::INTEGER 
                        END as phy_zone,
                        CASE 
                            WHEN p.district_code ~ '^-?\\d+$' THEN p.district_code::INTEGER 
                            ELSE NULL::INTEGER 
                        END as district_code,
                        
                        -- Tree_and_climber table fields - already numeric types, direct casting
                        t.tree_no::INTEGER as tree_no,
                        t.tree_and_climber_forest_stand::INTEGER as forest_stand,
                        t.tree_and_climber_bearing::REAL as bearing,
                        t.tree_and_climber_distance::REAL as distance,
                        CASE 
                            WHEN t.tree_and_climber_species_code ~ '^-?\\d+$' THEN t.tree_and_climber_species_code::INTEGER 
                            ELSE NULL::INTEGER 
                        END as species_code,
                        t.tree_and_climber_dbh::REAL as dbh,
                        CASE 
                            WHEN t.quality_class ~ '^-?\\d+$' THEN t.quality_class::INTEGER 
                            ELSE NULL::INTEGER 
                        END as quality_class,
                        t.quality_class_code_id_::BIGINT as quality_class_code,
                        CASE 
                            WHEN t.crown_class ~ '^-?\\d+$' THEN t.crown_class::INTEGER 
                            ELSE NULL::INTEGER 
                        END as crown_class,
                        t.crown_class_code_id_::BIGINT as crown_class_code,
                        CASE 
                            WHEN t.sample_tree_type ~ '^-?\\d+$' THEN t.sample_tree_type::INTEGER 
                            ELSE NULL::INTEGER 
                        END as sample_tree_type,
                        t.sample_tree_type_code_id_::BIGINT as sample_tree_type_code,
                        t.height::REAL as height,
                        t.crown_height::REAL as crown_height,
                        t.base_tree_height::REAL as base_tree_height,
                        t.base_crown_height::REAL as base_crown_height,
                        t.base_slope::REAL as base_slope,
                        t.age::INTEGER as age,
                        t.radial_growth::INTEGER as radial_growth
                        
                    FROM "{schema_name}".plot p
                    INNER JOIN "{schema_name}"."{table_name}" t 
                        ON p.plot_id_ = t.plot_id_
                    WHERE p.plot_id_ IS NOT NULL
                    ORDER BY p.plot_id_::BIGINT, t.tree_no::INTEGER
                    """
                    
                    try:
                        # Get total rows from join
                        count_query = f"""
                        SELECT COUNT(*)
                        FROM "{schema_name}".plot p
                        INNER JOIN "{schema_name}"."{table_name}" t 
                            ON p.plot_id_ = t.plot_id_
                        WHERE p.plot_id_ IS NOT NULL
                        """
                        cursor.execute(count_query)
                        total_rows = cursor.fetchone()[0]
                        
                        # Get sample data
                        cursor.execute(join_query + f" LIMIT {limit}")
                        sample_rows = cursor.fetchall()
                        column_names = [desc[0] for desc in cursor.description]
                        
                        # Create columns info from the join result with proper data types
                        data_type_mapping = {
                            'plot_id': 'bigint',
                            'plot_col': 'bigint', 
                            'plot_row': 'bigint',
                            'plot_number': 'bigint',
                            'plot_x': 'real',
                            'plot_y': 'real',
                            'phy_zone': 'integer',
                            'district_code': 'integer',
                            'tree_no': 'integer',
                            'forest_stand': 'integer',
                            'bearing': 'real',
                            'distance': 'real',
                            'species_code': 'integer',
                            'dbh': 'real',
                            'quality_class': 'integer',
                            'quality_class_code': 'bigint',
                            'crown_class': 'integer',
                            'crown_class_code': 'bigint',
                            'sample_tree_type': 'integer',
                            'sample_tree_type_code': 'bigint',
                            'height': 'real',
                            'crown_height': 'real',
                            'base_tree_height': 'real',
                            'base_crown_height': 'real',
                            'base_slope': 'real',
                            'age': 'integer',
                            'radial_growth': 'integer'
                        }
                        
                        columns_info = []
                        for col_name in column_names:
                            columns_info.append({
                                'name': col_name,
                                'data_type': data_type_mapping.get(col_name, 'text'),
                                'nullable': True,
                                'null_count': 0,  # We'll calculate this if needed
                                'sample_values': []
                            })
                        
                        sample_data = []
                        for row in sample_rows:
                            sample_data.append(dict(zip(column_names, row)))
                        
                        return {
                            'total_rows': total_rows,
                            'columns': columns_info,
                            'sample_data': sample_data,
                            'quality_score': self._calculate_quality_score(total_rows, columns_info),
                            'is_join_query': True,
                            'join_info': f"Joined {schema_name}.plot with {schema_name}.{table_name}"
                        }
                        
                    except Exception as e:
                        error_msg = f"Error executing join query for preview: {str(e)}"
                        logger.error(error_msg)
                        
                        # Check if it's a missing field error
                        if "column" in str(e).lower() and ("does not exist" in str(e).lower() or "not found" in str(e).lower()):
                            missing_field_msg = f"Missing field in {schema_name}.{table_name} or {schema_name}.plot tables. Error: {str(e)}"
                            raise DataImportError(missing_field_msg)
                        else:
                            raise DataImportError(error_msg)
                
                else:
                    # Standard table preview (non-join)
                    # Get table structure
                    cursor.execute("""
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns 
                        WHERE table_schema = %s AND table_name = %s
                        ORDER BY ordinal_position
                    """, [schema_name, table_name])
                    
                    columns_info = []
                    for row in cursor.fetchall():
                        columns_info.append({
                            'name': row[0],
                            'data_type': row[1],
                            'nullable': row[2] == 'YES'
                        })
                    
                    if not columns_info:
                        raise DataImportError(f"Table {schema_name}.{table_name} not found or has no columns")
                    
                    # Get total row count
                    cursor.execute(
                        SQL("SELECT COUNT(*) FROM {}.{}").format(
                            Identifier(schema_name), 
                            Identifier(table_name)
                        )
                    )
                    total_rows = cursor.fetchone()[0]
                    
                    # Get sample data with null counts
                    sample_data = []
                    
                    if total_rows > 0:
                        # Get sample rows
                        cursor.execute(
                            SQL("SELECT * FROM {}.{} LIMIT %s").format(
                                Identifier(schema_name), 
                                Identifier(table_name)
                            ),
                            [limit]
                        )
                        
                        column_names = [desc[0] for desc in cursor.description]
                        sample_rows = cursor.fetchall()
                        
                        for row in sample_rows:
                            sample_data.append(dict(zip(column_names, row)))
                        
                        # Get null counts and sample values for each column
                        for col_info in columns_info:
                            col_name = col_info['name']
                            
                            # Get null count
                            cursor.execute(
                                SQL("SELECT COUNT(*) FROM {}.{} WHERE {} IS NULL").format(
                                    Identifier(schema_name),
                                    Identifier(table_name),
                                    Identifier(col_name)
                                )
                            )
                            null_count = cursor.fetchone()[0]
                            
                            # Get sample non-null values
                            cursor.execute(
                                SQL("SELECT DISTINCT {} FROM {}.{} WHERE {} IS NOT NULL LIMIT 5").format(
                                    Identifier(col_name),
                                    Identifier(schema_name),
                                    Identifier(table_name),
                                    Identifier(col_name)
                                )
                            )
                            sample_values = [str(row[0]) for row in cursor.fetchall()]
                            
                            col_info['null_count'] = null_count
                            col_info['sample_values'] = sample_values
                    
                    return {
                        'total_rows': total_rows,
                        'columns': columns_info,
                        'sample_data': sample_data,
                        'quality_score': self._calculate_quality_score(total_rows, columns_info),
                        'is_join_query': False
                    }
                
        except DataImportError:
            # Re-raise DataImportError as-is
            raise
        except Exception as e:
            logger.error(f"Error getting table preview for {schema_name}.{table_name}: {str(e)}")
            raise DataImportError(f"Failed to get table preview: {str(e)}")
    
    def _calculate_quality_score(self, total_rows: int, columns_info: List[Dict]) -> int:
        """Calculate a simple data quality score (0-100)"""
        if total_rows == 0:
            return 0
        
        total_null_percentage = 0
        for col in columns_info:
            null_percentage = (col.get('null_count', 0) / total_rows) * 100
            total_null_percentage += null_percentage
        
        avg_null_percentage = total_null_percentage / len(columns_info) if columns_info else 100
        quality_score = max(0, min(100, 100 - avg_null_percentage))
        
        return int(quality_score)
    
    def get_project_table_structure(self, project: Project, table_name: str = 'tree_biometric_calc') -> List[Dict]:
        """Get the structure of a table in the project schema"""
        try:
            schema_name = project.get_schema_name()
            with self.default_connection.cursor() as cursor:
                cursor.execute("""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns 
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, [schema_name, table_name])
                
                return [
                    {
                        'column_name': row[0],
                        'data_type': row[1],
                        'is_nullable': row[2],
                        'column_default': row[3]
                    }
                    for row in cursor.fetchall()
                ]
        except Exception as e:
            logger.error(f"Error getting project table structure: {str(e)}")
            return []
    
    def import_data_to_project(self, project: Project, import_id: int, schema_name: str, table_name: str, action: str = 'append') -> Tuple[bool, str, int]:
        """
        Import data from foris_connection to project schema
        
        Args:
            project: Project instance
            import_id: ID of the import record in project schema
            schema_name: Source schema name
            table_name: Source table name
            action: 'append' or 'replace'
            
        Returns:
            Tuple of (success, message, imported_rows)
        """
        try:
            # Initialize project data import manager
            import_manager = ProjectDataImportManager(project)
            
            # Mark import as processing
            import_manager.update_import_status(import_id, 'processing')
            
            source_schema = schema_name
            source_table = table_name
            target_schema = project.get_schema_name()
            target_table = 'tree_biometric_calc'  # Default target table
            
            # Ensure project schema and table exist
            if not project.schema_exists():
                success, message = project.create_project_schema()
                if not success:
                    import_manager.update_import_status(import_id, 'failed', error_message=f"Failed to create project schema: {message}")
                    return False, message, 0
            
            if not project.table_exists(target_table):
                error_msg = f"Target table {target_table} does not exist in project schema"
                import_manager.update_import_status(import_id, 'failed', error_message=error_msg)
                return False, error_msg, 0
            
            # Get source table structure and data using the specialized join query
            with self.foris_connection.cursor() as foris_cursor:
                # Build the join query for plot and tree_and_climber tables with optimized safe casting
                join_query = f"""
                SELECT 
                    -- Plot table fields with safe casting
                    p.plot_id_::BIGINT as plot_id,
                    CASE 
                        WHEN p.col ~ '^-?\\d+$' THEN p.col::BIGINT 
                        ELSE NULL::BIGINT 
                    END as plot_col,
                    CASE 
                        WHEN p."row" ~ '^-?\\d+$' THEN p."row"::BIGINT 
                        ELSE NULL::BIGINT 
                    END as plot_row,
                    CASE 
                        WHEN p."number" ~ '^-?\\d+$' THEN p."number"::BIGINT 
                        ELSE NULL::BIGINT 
                    END as plot_number,
                    p.utm_coordinate_x::REAL as plot_x,
                    p.utm_coordinate_y::REAL as plot_y,
                    CASE 
                        WHEN p.phy_zone ~ '^-?\\d+$' THEN p.phy_zone::INTEGER 
                        ELSE NULL::INTEGER 
                    END as phy_zone,
                    CASE 
                        WHEN p.district_code ~ '^-?\\d+$' THEN p.district_code::INTEGER 
                        ELSE NULL::INTEGER 
                    END as district_code,
                    
                    -- Tree_and_climber table fields - already numeric types, direct casting
                    t.tree_no::INTEGER as tree_no,
                    t.tree_and_climber_forest_stand::INTEGER as forest_stand,
                    t.tree_and_climber_bearing::REAL as bearing,
                    t.tree_and_climber_distance::REAL as distance,
                    CASE 
                        WHEN t.tree_and_climber_species_code ~ '^-?\\d+$' THEN t.tree_and_climber_species_code::INTEGER 
                        ELSE NULL::INTEGER 
                    END as species_code,
                    t.tree_and_climber_dbh::REAL as dbh,
                    CASE 
                        WHEN t.quality_class ~ '^-?\\d+$' THEN t.quality_class::INTEGER 
                        ELSE NULL::INTEGER 
                    END as quality_class,
                    t.quality_class_code_id_::BIGINT as quality_class_code,
                    CASE 
                        WHEN t.crown_class ~ '^-?\\d+$' THEN t.crown_class::INTEGER 
                        ELSE NULL::INTEGER 
                    END as crown_class,
                    t.crown_class_code_id_::BIGINT as crown_class_code,
                    CASE 
                        WHEN t.sample_tree_type ~ '^-?\\d+$' THEN t.sample_tree_type::INTEGER 
                        ELSE NULL::INTEGER 
                    END as sample_tree_type,
                    t.sample_tree_type_code_id_::BIGINT as sample_tree_type_code,
                    t.height::REAL as height,
                    t.crown_height::REAL as crown_height,
                    t.base_tree_height::REAL as base_tree_height,
                    t.base_crown_height::REAL as base_crown_height,
                    t.base_slope::REAL as base_slope,
                    t.age::INTEGER as age,
                    t.radial_growth::INTEGER as radial_growth
                    
                FROM "{source_schema}".plot p
                INNER JOIN "{source_schema}"."{source_table}" t 
                    ON p.plot_id_ = t.plot_id_
                WHERE p.plot_id_ IS NOT NULL
                ORDER BY p.plot_id_::BIGINT, t.tree_no::INTEGER
                """
                
                # Get total rows from the join query
                count_query = f"""
                SELECT COUNT(*)
                FROM "{source_schema}".plot p
                INNER JOIN "{source_schema}"."{source_table}" t 
                    ON p.plot_id_ = t.plot_id_
                WHERE p.plot_id_ IS NOT NULL
                """
                
                try:
                    foris_cursor.execute(count_query)
                    total_source_rows = foris_cursor.fetchone()[0]
                    import_manager.update_import_status(import_id, 'processing', total_rows=total_source_rows)
                    
                    if total_source_rows == 0:
                        import_manager.update_import_status(import_id, 'completed', imported_rows=0)
                        return True, "No data to import (no matching records found in join)", 0
                    
                    # Get the column structure from the join query by executing it with LIMIT 0
                    foris_cursor.execute(join_query + " LIMIT 0")
                    
                    # Map proper data types for the joined columns
                    data_type_mapping = {
                        'plot_id': 'bigint',
                        'plot_col': 'bigint', 
                        'plot_row': 'bigint',
                        'plot_number': 'bigint',
                        'plot_x': 'real',
                        'plot_y': 'real',
                        'phy_zone': 'integer',
                        'district_code': 'integer',
                        'tree_no': 'integer',
                        'forest_stand': 'integer',
                        'bearing': 'real',
                        'distance': 'real',
                        'species_code': 'integer',
                        'dbh': 'real',
                        'quality_class': 'integer',
                        'quality_class_code': 'bigint',
                        'crown_class': 'integer',
                        'crown_class_code': 'bigint',
                        'sample_tree_type': 'integer',
                        'sample_tree_type_code': 'bigint',
                        'height': 'real',
                        'crown_height': 'real',
                        'base_tree_height': 'real',
                        'base_crown_height': 'real',
                        'base_slope': 'real',
                        'age': 'integer',
                        'radial_growth': 'integer'
                    }
                    
                    source_columns = {}
                    for desc in foris_cursor.description:
                        col_name = desc[0]
                        source_columns[col_name] = data_type_mapping.get(col_name, 'text')
                    
                except Exception as e:
                    error_msg = f"Error executing join query: {str(e)}"
                    logger.error(error_msg)
                    import_manager.update_import_status(import_id, 'failed', error_message=error_msg)
                    return False, error_msg, 0
            
            # Get target table structure
            target_columns = self.get_project_table_structure(project, target_table)
            target_column_names = {col['column_name']: col['data_type'] for col in target_columns}
            
            # Find matching columns (case-insensitive)
            column_mapping = self._create_column_mapping(source_columns, target_column_names)
            
            if not column_mapping:
                error_msg = "No matching columns found between source and target tables"
                import_manager.update_import_status(import_id, 'failed', error_message=error_msg)
                return False, error_msg, 0
            
            # If action is 'replace' or 'replace_selected', delete existing data from the same schema/table combination
            if action in ['replace', 'replace_selected']:
                if action == 'replace':
                    logger.info(f"Replace all action detected - deleting existing data from {source_schema}.{source_table}")
                    # For replace all, also clear all import records and create a single new one
                    self._clear_all_import_records(project)
                    # Create a new import record for the replace all operation
                    new_import_id = import_manager.create_import_record(
                        schema_name, table_name, action, 
                        f"Replace all data - imported from {source_schema}.{source_table}"
                    )
                    # Update the import_id to use the new record
                    import_id = new_import_id
                    logger.info(f"Created new import record {import_id} for replace all operation")
                else:
                    logger.info(f"Replace selected action detected - deleting existing data from {source_schema}.{source_table}")
                deleted_rows = self._delete_existing_import_data(project, source_schema, source_table)
                logger.info(f"Deleted {deleted_rows} existing rows from {source_schema}.{source_table}")
            
            # Perform the import using the join query
            logger.info(f"Starting data import with action: {action}, from {source_schema}.{source_table} to {target_schema}.{target_table}")
            imported_rows = self._execute_data_import_with_join(
                project,
                action, 
                source_schema, 
                source_table, 
                target_schema, 
                target_table, 
                column_mapping,
                join_query,
                import_id
            )
            
            import_manager.update_import_status(import_id, 'completed', imported_rows=imported_rows)
            
            return True, f"Successfully imported {imported_rows} rows", imported_rows
            
        except Exception as e:
            error_msg = f"Import failed: {str(e)}"
            logger.error(f"Data import error: {error_msg}")
            try:
                import_manager = ProjectDataImportManager(project)
                import_manager.update_import_status(import_id, 'failed', error_message=error_msg)
            except:
                pass  # If we can't update status, at least return the error
            return False, error_msg, 0
    
    def _create_column_mapping(self, source_columns: Dict[str, str], target_columns: Dict[str, str]) -> Dict[str, str]:
        """Create mapping between source and target columns"""
        mapping = {}
        
        # Direct name matches (case-insensitive)
        for source_col, source_type in source_columns.items():
            for target_col, target_type in target_columns.items():
                if source_col.lower() == target_col.lower():
                    mapping[source_col] = target_col
                    break
        
        # Add common aliases and mappings for forest biometric data
        aliases = {
            'dbh': ['diameter', 'dbh_cm', 'diameter_cm', 'tree_and_climber_dbh'],
            'height': ['ht', 'height_m', 'tree_height'],
            'species_code': ['species', 'sp_code', 'species_id', 'tree_and_climber_species_code'],
            'plot_id': ['plot', 'plot_number', 'plot_no', 'plot_id_'],
            'tree_no': ['tree_id', 'tree_number', 'tree'],
            'plot_col': ['col', 'column'],
            'plot_row': ['row'],
            'plot_number': ['number', 'plot_num'],
            'plot_x': ['utm_coordinate_x', 'x_coordinate', 'x'],
            'plot_y': ['utm_coordinate_y', 'y_coordinate', 'y'],
            'forest_stand': ['tree_and_climber_forest_stand', 'stand'],
            'bearing': ['tree_and_climber_bearing'],
            'distance': ['tree_and_climber_distance'],
            'quality_class': ['quality_class'],
            'quality_class_code': ['quality_class_code_id_'],
            'crown_class': ['crown_class'],
            'crown_class_code': ['crown_class_code_id_'],
            'sample_tree_type': ['sample_tree_type'],
            'sample_tree_type_code': ['sample_tree_type_code_id_'],
            'crown_height': ['crown_height'],
            'base_tree_height': ['base_tree_height'],
            'base_crown_height': ['base_crown_height'],
            'base_slope': ['base_slope'],
            'age': ['age'],
            'radial_growth': ['radial_growth'],
            'phy_zone': ['physiography_zone', 'physiographic_zone'],
            'district_code': ['district']
        }
        
        for target_col in target_columns.keys():
            if target_col not in mapping.values():  # Not already mapped
                target_lower = target_col.lower()
                
                # Check aliases
                for alias_group in aliases.values():
                    if target_lower in [a.lower() for a in alias_group]:
                        for source_col in source_columns.keys():
                            source_lower = source_col.lower()
                            if source_lower in [a.lower() for a in alias_group] and source_col not in mapping:
                                mapping[source_col] = target_col
                                break
        
        return mapping
    
    def _execute_data_import_with_join(self, project: Project, action: str, source_schema: str, 
                           source_table: str, target_schema: str, target_table: str, 
                           column_mapping: Dict[str, str], join_query: str, import_id: int = None) -> int:
        """Execute the actual data import"""
        
        # Handle replace actions
        if action == 'replace':
            with self.default_connection.cursor() as cursor:
                # First, count existing rows
                cursor.execute(
                    SQL("SELECT COUNT(*) FROM {}.{}").format(
                        Identifier(target_schema),
                        Identifier(target_table)
                    )
                )
                existing_rows = cursor.fetchone()[0]
                logger.info(f"Replace all action: Deleting {existing_rows} existing rows from {target_schema}.{target_table}")
                
                # Delete all existing data
                cursor.execute(
                    SQL("DELETE FROM {}.{}").format(
                        Identifier(target_schema),
                        Identifier(target_table)
                    )
                )
                logger.info(f"Replace all action: Successfully deleted all rows from {target_schema}.{target_table}")
        elif action == 'replace_selected':
            logger.info(f"Replace selected action: Data from same schema/table already deleted, adding new data to {target_schema}.{target_table}")
        else:
            logger.info(f"Append action: Adding new data to {target_schema}.{target_table}")
        
        # Build the INSERT query
        source_cols = list(column_mapping.keys())
        target_cols = list(column_mapping.values())
        
        # Check if pandas is available
        if pd is None:
            raise DataImportError("pandas is required for data import operations")
        
        # Use pandas for efficient data transfer
        with self.foris_connection.cursor() as foris_cursor:
            # Read data in chunks using the join query
            chunk_size = 1000
            total_imported = 0
            
            # Execute the join query to get all data
            foris_cursor.execute(join_query)
            
            # Process in chunks
            while True:
                rows = foris_cursor.fetchmany(chunk_size)
                if not rows:
                    break
                
                # Get column names from cursor description (from join query)
                column_names = [desc[0] for desc in foris_cursor.description]
                
                # Convert to DataFrame for easier manipulation
                df = pd.DataFrame(rows, columns=column_names)
                
                # Clean and prepare data
                df = self._clean_dataframe(df, column_mapping)
                
                if not df.empty:
                    # Insert into target table
                    rows_inserted = self._insert_dataframe_to_table(
                        df, target_schema, target_table, target_cols, import_id
                    )
                    total_imported += rows_inserted
        
        return total_imported
    
    def _clean_dataframe(self, df: pd.DataFrame, column_mapping: Dict[str, str]) -> pd.DataFrame:
        """Clean and prepare DataFrame for import"""
        # Rename columns to target names
        df = df.rename(columns=column_mapping)
        
        # Handle common data cleaning and ensure proper data types
        for col in df.columns:
            if df[col].dtype == 'object':
                # Clean string columns
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace('', None)
                df[col] = df[col].replace('nan', None)
                df[col] = df[col].replace('NaN', None)
                df[col] = df[col].replace('None', None)
            elif pd.api.types.is_numeric_dtype(df[col]):
                # Handle numeric columns - ensure they're native Python types
                df[col] = pd.to_numeric(df[col], errors='coerce')
                # Convert to nullable types to handle NaN properly
                if pd.api.types.is_integer_dtype(df[col]):
                    df[col] = df[col].astype('Int64')  # Nullable integer
                elif pd.api.types.is_float_dtype(df[col]):
                    df[col] = df[col].astype('Float64')  # Nullable float
        
        # Replace any remaining NaN values with None for proper SQL NULL handling
        df = df.where(pd.notnull(df), None)
        
        return df
    
    def _insert_dataframe_to_table(self, df: pd.DataFrame, schema: str, table: str, columns: List[str], import_id: int = None) -> int:
        """Insert DataFrame into target table with optional import_id tracking"""
        try:
            with transaction.atomic():
                with self.default_connection.cursor() as cursor:
                    # Add import_id to columns if provided and for tree_biometric_calc table
                    actual_columns = columns.copy()
                    if import_id is not None and table == 'tree_biometric_calc':
                        actual_columns = ['import_id'] + actual_columns
                    
                    # Prepare the INSERT statement
                    placeholders = ', '.join(['%s'] * len(actual_columns))
                    insert_sql = SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
                        Identifier(schema),
                        Identifier(table),
                        SQL(', ').join(map(Identifier, actual_columns)),
                        SQL(placeholders)
                    )
                    
                    # Convert DataFrame to list of tuples with proper type conversion
                    values = []
                    for _, row in df.iterrows():
                        row_values = []
                        
                        # Add import_id as first value if provided
                        if import_id is not None and table == 'tree_biometric_calc':
                            row_values.append(import_id)
                        
                        for col in columns:
                            value = row.get(col)
                            # Handle NaN/None values
                            if pd.isna(value) or value is None:
                                row_values.append(None)
                            else:
                                # Convert NumPy types to native Python types
                                if hasattr(value, 'item'):  # NumPy scalar types have .item() method
                                    value = value.item()
                                elif pd.api.types.is_integer_dtype(type(value)):
                                    value = int(value) if not pd.isna(value) else None
                                elif pd.api.types.is_float_dtype(type(value)):
                                    value = float(value) if not pd.isna(value) else None
                                elif isinstance(value, (pd.Timestamp, pd.Timedelta)):
                                    value = value.to_pydatetime() if pd.notna(value) else None
                                
                                row_values.append(value)
                        values.append(tuple(row_values))
                    
                    # Execute batch insert
                    cursor.executemany(insert_sql, values)
                    return len(values)
                    
        except Exception as e:
            logger.error(f"Error inserting data: {str(e)}")
            raise DataImportError(f"Failed to insert data: {str(e)}")
    
    def _delete_existing_import_data(self, project: Project, source_schema: str, source_table: str) -> int:
        """Delete existing data from tree_biometric_calc that was imported from the same schema/table combination"""
        try:
            import_manager = ProjectDataImportManager(project)
            schema_name = project.get_schema_name()
            
            with self.default_connection.cursor() as cursor:
                cursor.execute(SQL("SET search_path TO {}").format(Identifier(schema_name)))
                
                # Find import records with the same schema and table
                cursor.execute("""
                    SELECT id FROM project_data_imports 
                    WHERE schema_name = %s AND table_name = %s
                """, [source_schema, source_table])
                
                import_ids = [row[0] for row in cursor.fetchall()]
                
                if not import_ids:
                    return 0
                
                # Delete data from tree_biometric_calc for these import IDs
                cursor.execute("""
                    DELETE FROM tree_biometric_calc 
                    WHERE import_id = ANY(%s)
                """, [import_ids])
                
                deleted_rows = cursor.rowcount
                logger.info(f"Deleted {deleted_rows} rows from tree_biometric_calc for schema {source_schema}.{source_table}")
                
                return deleted_rows
                
        except Exception as e:
            logger.error(f"Error deleting existing import data: {str(e)}")
            return 0
    
    def _clear_all_import_records(self, project: Project) -> None:
        """Clear all import records from project_data_imports table (used for replace all action)"""
        try:
            schema_name = project.get_schema_name()
            
            with self.default_connection.cursor() as cursor:
                cursor.execute(SQL("SET search_path TO {}").format(Identifier(schema_name)))
                
                # Delete all records from project_data_imports
                cursor.execute("DELETE FROM project_data_imports")
                deleted_count = cursor.rowcount
                logger.info(f"Cleared {deleted_count} import records for replace all operation")
                
        except Exception as e:
            logger.error(f"Error clearing import records: {str(e)}")
            # Don't raise exception as this is not critical for the main operation
    
    def delete_project_import(self, project: Project, import_id: int) -> Tuple[bool, str]:
        """Delete a specific import and its associated data from tree_biometric_calc"""
        try:
            import_manager = ProjectDataImportManager(project)
            
            # Check if import exists
            import_record = import_manager.get_import_by_id(import_id)
            if not import_record:
                return False, "Import not found"
            
            schema_name = project.get_schema_name()
            
            with transaction.atomic():
                with self.default_connection.cursor() as cursor:
                    # Set search path to project schema
                    cursor.execute(SQL("SET search_path TO {}").format(Identifier(schema_name)))
                    
                    # First, count how many rows will be deleted
                    cursor.execute(
                        "SELECT COUNT(*) FROM tree_biometric_calc WHERE import_id = %s",
                        [import_id]
                    )
                    rows_to_delete = cursor.fetchone()[0]
                    
                    # Delete associated data from tree_biometric_calc
                    if rows_to_delete > 0:
                        cursor.execute(
                            "DELETE FROM tree_biometric_calc WHERE import_id = %s",
                            [import_id]
                        )
                        logger.info(f"Deleted {rows_to_delete} rows from tree_biometric_calc for import {import_id}")
                    
                    # Delete the import record
                    success = import_manager.delete_import(import_id)
                    
                    if success:
                        if rows_to_delete > 0:
                            return True, f"Import record and {rows_to_delete} associated tree records deleted successfully"
                        else:
                            return True, "Import record deleted successfully (no associated tree data found)"
                    else:
                        return False, "Failed to delete import record"
            
        except Exception as e:
            logger.error(f"Error deleting import: {str(e)}")
            return False, f"Failed to delete import: {str(e)}"
