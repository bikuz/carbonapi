"""
Data quality check utilities for MRV projects.
Handles validation and correction of forest biometric data.
"""

import logging
from typing import Dict, List, Tuple, Optional, Any
from django.db import connections, transaction
from django.conf import settings
from psycopg2.sql import SQL, Identifier, Literal
import re

from .models import Project, ProjectDataImportManager

logger = logging.getLogger(__name__)

class DataQualityError(Exception):
    """Custom exception for data quality errors"""
    pass

class DataQualityService:
    """Service class for handling data quality checks and corrections"""
    
    def __init__(self, project: Project):
        self.project = project
        self.schema_name = project.get_schema_name()
        self.default_connection = connections['default']
    
    def perform_quality_check(self, check_type: str, schema_data: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Perform comprehensive data quality check
        
        Args:
            check_type: 'selected' or 'all'
            schema_data: Optional schema data for selected check
            
        Returns:
            Dictionary containing quality check results
        """
        try:
            logger.info(f"Starting data quality check for project {self.project.name}, type: {check_type}")
            
            # Step 1: Generate plot codes
            logger.info("Step 1: Generating plot codes...")
            plot_code_results = self._generate_plot_codes(check_type, schema_data)
            
            # Step 2: Validate physiography zones
            logger.info("Step 2: Validating physiography zones...")
            phy_zone_results = self._validate_phy_zones(check_type, schema_data)
            
            # Step 3: Validate tree numbers
            logger.info("Step 3: Validating tree numbers...")
            tree_no_results = self._validate_tree_numbers(check_type, schema_data)
            
            # Step 4: Validate species codes
            logger.info("Step 4: Validating species codes...")
            species_code_results = self._validate_species_codes(check_type, schema_data)
            
            # Step 5: Validate DBH values
            logger.info("Step 5: Validating DBH values...")
            dbh_results = self._validate_dbh_values(check_type, schema_data)
            
            # Get ignored records counts for each issue type
            logger.info("Getting ignored records counts...")
            plot_code_ignored = self.get_ignored_records_count('plot_code', schema_data)
            phy_zone_ignored = self.get_ignored_records_count('phy_zone', schema_data)
            tree_no_ignored = self.get_ignored_records_count('tree_no', schema_data)
            species_code_ignored = self.get_ignored_records_count('species_code', schema_data)
            dbh_ignored = self.get_ignored_records_count('dbh', schema_data)
            
            # Add ignored counts to results
            plot_code_results['ignored_count'] = plot_code_ignored
            phy_zone_results['ignored_count'] = phy_zone_ignored
            tree_no_results['ignored_count'] = tree_no_ignored
            species_code_results['ignored_count'] = species_code_ignored
            dbh_results['ignored_count'] = dbh_ignored
            
            # Compile results
            total_records = self._get_total_records(check_type, schema_data)
            total_issues = (
                plot_code_results['count'] + 
                phy_zone_results['count'] + 
                tree_no_results['count'] + 
                species_code_results['count'] + 
                dbh_results['count']
            )
            
            quality_score = max(0, min(100, 100 - (total_issues / total_records * 100))) if total_records > 0 else 100
            
            logger.info(f"Compiled results - Total records: {total_records}, Total issues: {total_issues}, Quality score: {quality_score}")
            
            results = {
                'totalRecords': total_records,
                'totalIssues': total_issues,
                'qualityScore': round(quality_score, 1),
                'issues': [
                    plot_code_results,
                    phy_zone_results,
                    tree_no_results,
                    species_code_results,
                    dbh_results
                ]
            }
            
            logger.info(f"Quality check completed. Total issues: {total_issues}, Quality score: {quality_score}%")
            return results
            
        except Exception as e:
            logger.error(f"Error during quality check: {str(e)}")
            raise DataQualityError(f"Failed to perform quality check: {str(e)}")
    
    def _get_total_records(self, check_type: str, schema_data: Optional[Dict] = None) -> int:
        """Get total number of records to check"""
        try:
            logger.info(f"Getting total records for check_type: {check_type}, schema_data: {schema_data}")
            logger.info(f"Using schema: {self.schema_name}")
            
            with self.default_connection.cursor() as cursor:
                cursor.execute(SQL("SET search_path TO {}").format(Identifier(self.schema_name)))
                
                if check_type == 'selected' and schema_data:
                    # Count records from specific import
                    import_id = schema_data.get('import_id')
                    logger.info(f"Counting records for import_id: {import_id}")
                    cursor.execute("""
                        SELECT COUNT(*) FROM tree_biometric_calc 
                        WHERE import_id = %s
                    """, [import_id])
                else:
                    # Count all records
                    logger.info("Counting all records in tree_biometric_calc")
                    cursor.execute("SELECT COUNT(*) FROM tree_biometric_calc")
                
                count = cursor.fetchone()[0]
                logger.info(f"Total records found: {count}")
                return count
        except Exception as e:
            logger.error(f"Error getting total records: {str(e)}")
            return 0
    
    def _generate_plot_codes(self, check_type: str, schema_data: Optional[Dict] = None) -> Dict[str, Any]:
        """Generate plot codes and identify missing/invalid ones"""
        try:
            with self.default_connection.cursor() as cursor:
                cursor.execute(SQL("SET search_path TO {}").format(Identifier(self.schema_name)))
                
                # Update plot codes for records that don't have them
                if check_type == 'selected' and schema_data:
                    cursor.execute("""
                        UPDATE tree_biometric_calc 
                        SET plot_code = CONCAT(
                            LPAD(COALESCE(plot_col, 0)::TEXT, 4, '0'), '-',
                            LPAD(COALESCE(plot_row, 0)::TEXT, 4, '0'), '-',
                            LPAD(COALESCE(plot_number, 0)::TEXT, 3, '0')
                        )
                        WHERE import_id = %s AND (plot_code IS NULL OR plot_code = '')
                    """, [schema_data.get('import_id')])
                else:
                    cursor.execute("""
                        UPDATE tree_biometric_calc 
                        SET plot_code = CONCAT(
                            LPAD(COALESCE(plot_col, 0)::TEXT, 4, '0'), '-',
                            LPAD(COALESCE(plot_row, 0)::TEXT, 4, '0'), '-',
                            LPAD(COALESCE(plot_number, 0)::TEXT, 3, '0')
                        )
                        WHERE plot_code IS NULL OR plot_code = ''
                    """)
                
                # Count records with missing plot_code components (excluding ignored records)
                if check_type == 'selected' and schema_data:
                    cursor.execute("""
                        SELECT COUNT(*) FROM tree_biometric_calc 
                        WHERE import_id = %s AND ignore = FALSE AND (
                            plot_col IS NULL OR plot_col <= 0 OR
                            plot_row IS NULL OR plot_row <= 0 OR
                            plot_number IS NULL OR plot_number <= 0
                        )
                    """, [schema_data.get('import_id')])
                else:
                    cursor.execute("""
                        SELECT COUNT(*) FROM tree_biometric_calc 
                        WHERE ignore = FALSE AND (
                            plot_col IS NULL OR plot_col <= 0 OR
                            plot_row IS NULL OR plot_row <= 0 OR
                            plot_number IS NULL OR plot_number <= 0
                        )
                    """)
                
                count = cursor.fetchone()[0]
                
                return {
                    'type': 'plot_code',
                    'title': 'Plot Code Generation',
                    'description': 'Generate plot codes in format "0000-0000-000"',
                    'count': count,
                    'status': 'pending' if count > 0 else 'completed',
                    'validation_rules': 'plot_col, plot_row, plot_number must be > 0'
                }
                
        except Exception as e:
            logger.error(f"Error generating plot codes: {str(e)}")
            return {
                'type': 'plot_code',
                'title': 'Plot Code Generation',
                'description': 'Generate plot codes in format "0000-0000-000"',
                'count': 0,
                'status': 'error',
                'error': str(e)
            }
    
    def _validate_phy_zones(self, check_type: str, schema_data: Optional[Dict] = None) -> Dict[str, Any]:
        """Validate physiography zones (must be 1-5)"""
        try:
            with self.default_connection.cursor() as cursor:
                cursor.execute(SQL("SET search_path TO {}").format(Identifier(self.schema_name)))
                
                # Count invalid phy_zone values (excluding ignored records)
                if check_type == 'selected' and schema_data:
                    cursor.execute("""
                        SELECT COUNT(*) FROM tree_biometric_calc 
                        WHERE import_id = %s AND ignore = FALSE AND (
                            phy_zone IS NULL OR phy_zone < 1 OR phy_zone > 5
                        )
                    """, [schema_data.get('import_id')])
                else:
                    cursor.execute("""
                        SELECT COUNT(*) FROM tree_biometric_calc 
                        WHERE ignore = FALSE AND (
                            phy_zone IS NULL OR phy_zone < 1 OR phy_zone > 5
                        )
                    """)
                
                count = cursor.fetchone()[0]
                
                return {
                    'type': 'phy_zone',
                    'title': 'Physiography Zone Validation',
                    'description': 'Validate physiography zones (must be 1-5)',
                    'count': count,
                    'status': 'pending' if count > 0 else 'completed',
                    'validation_rules': 'Must be between 1-5'
                }
                
        except Exception as e:
            logger.error(f"Error validating phy zones: {str(e)}")
            return {
                'type': 'phy_zone',
                'title': 'Physiography Zone Validation',
                'description': 'Validate physiography zones (must be 1-5)',
                'count': 0,
                'status': 'error',
                'error': str(e)
            }
    
    def _validate_tree_numbers(self, check_type: str, schema_data: Optional[Dict] = None) -> Dict[str, Any]:
        """Validate tree numbers (must be > 0)"""
        try:
            with self.default_connection.cursor() as cursor:
                cursor.execute(SQL("SET search_path TO {}").format(Identifier(self.schema_name)))
                
                # Count invalid tree_no values (excluding ignored records)
                if check_type == 'selected' and schema_data:
                    cursor.execute("""
                        SELECT COUNT(*) FROM tree_biometric_calc 
                        WHERE import_id = %s AND ignore = FALSE AND (
                            tree_no IS NULL OR tree_no <= 0
                        )
                    """, [schema_data.get('import_id')])
                else:
                    cursor.execute("""
                        SELECT COUNT(*) FROM tree_biometric_calc 
                        WHERE ignore = FALSE AND (
                            tree_no IS NULL OR tree_no <= 0
                        )
                    """)
                
                count = cursor.fetchone()[0]
                
                return {
                    'type': 'tree_no',
                    'title': 'Tree Number Validation',
                    'description': 'Validate tree numbers (must be > 0)',
                    'count': count,
                    'status': 'pending' if count > 0 else 'completed',
                    'validation_rules': 'Must be greater than 0'
                }
                
        except Exception as e:
            logger.error(f"Error validating tree numbers: {str(e)}")
            return {
                'type': 'tree_no',
                'title': 'Tree Number Validation',
                'description': 'Validate tree numbers (must be > 0)',
                'count': 0,
                'status': 'error',
                'error': str(e)
            }
    
    def _validate_species_codes(self, check_type: str, schema_data: Optional[Dict] = None) -> Dict[str, Any]:
        """Validate species codes against forest_species table"""
        try:
            with self.default_connection.cursor() as cursor:
                cursor.execute(SQL("SET search_path TO {}").format(Identifier(self.schema_name)))
                
                # Count invalid species_code values (excluding ignored records)
                if check_type == 'selected' and schema_data:
                    cursor.execute("""
                        SELECT COUNT(*) FROM tree_biometric_calc t
                        WHERE t.import_id = %s AND t.ignore = FALSE AND (
                            t.species_code IS NULL OR 
                            t.species_code NOT IN (
                                SELECT code FROM public.forest_species WHERE code IS NOT NULL
                            )
                        )
                    """, [schema_data.get('import_id')])
                else:
                    cursor.execute("""
                        SELECT COUNT(*) FROM tree_biometric_calc t
                        WHERE t.ignore = FALSE AND (
                            t.species_code IS NULL OR 
                            t.species_code NOT IN (
                                SELECT code FROM public.forest_species WHERE code IS NOT NULL
                            )
                        )
                    """)
                
                count = cursor.fetchone()[0]
                
                return {
                    'type': 'species_code',
                    'title': 'Species Code Validation',
                    'description': 'Validate species codes against forest_species table',
                    'count': count,
                    'status': 'pending' if count > 0 else 'completed',
                    'validation_rules': 'Must exist in forest_species table'
                }
                
        except Exception as e:
            logger.error(f"Error validating species codes: {str(e)}")
            return {
                'type': 'species_code',
                'title': 'Species Code Validation',
                'description': 'Validate species codes against forest_species table',
                'count': 0,
                'status': 'error',
                'error': str(e)
            }
    
    def _validate_dbh_values(self, check_type: str, schema_data: Optional[Dict] = None) -> Dict[str, Any]:
        """Validate DBH values (must be > 0)"""
        try:
            with self.default_connection.cursor() as cursor:
                cursor.execute(SQL("SET search_path TO {}").format(Identifier(self.schema_name)))
                
                # Count invalid dbh values (excluding ignored records)
                if check_type == 'selected' and schema_data:
                    cursor.execute("""
                        SELECT COUNT(*) FROM tree_biometric_calc 
                        WHERE import_id = %s AND ignore = FALSE AND (
                            dbh IS NULL OR dbh <= 0
                        )
                    """, [schema_data.get('import_id')])
                else:
                    cursor.execute("""
                        SELECT COUNT(*) FROM tree_biometric_calc 
                        WHERE ignore = FALSE AND (
                            dbh IS NULL OR dbh <= 0
                        )
                    """)
                
                count = cursor.fetchone()[0]
                
                return {
                    'type': 'dbh',
                    'title': 'DBH Validation',
                    'description': 'Validate DBH values (must be > 0)',
                    'count': count,
                    'status': 'pending' if count > 0 else 'completed',
                    'validation_rules': 'Must be greater than 0'
                }
                
        except Exception as e:
            logger.error(f"Error validating DBH values: {str(e)}")
            return {
                'type': 'dbh',
                'title': 'DBH Validation',
                'description': 'Validate DBH values (must be > 0)',
                'count': 0,
                'status': 'error',
                'error': str(e)
            }
    
    def get_issue_details(self, issue_type: str, filters: Dict = None, page: int = 1, page_size: int = 50, exclude_ignored: bool = False) -> Dict[str, Any]:
        """Get detailed records for a specific issue type with pagination"""
        try:
            with self.default_connection.cursor() as cursor:
                cursor.execute(SQL("SET search_path TO {}").format(Identifier(self.schema_name)))
                
                # Build WHERE clause based on filters
                where_conditions = []
                params = []
                
                if filters:
                    if filters.get('plotCode'):
                        where_conditions.append("plot_code ILIKE %s")
                        params.append(f"%{filters['plotCode']}%")
                    
                    if filters.get('phyZone'):
                        where_conditions.append("phy_zone = %s")
                        params.append(filters['phyZone'])
                    
                    if filters.get('treeNo'):
                        where_conditions.append("tree_no = %s")
                        params.append(filters['treeNo'])
                    
                    # Handle issue-specific filters
                    if filters.get('issueFilter'):
                        issue_filter = filters['issueFilter']
                        if issue_type == 'plot_code':
                            if issue_filter == 'missing_col':
                                where_conditions.append("plot_col IS NULL")
                            elif issue_filter == 'missing_row':
                                where_conditions.append("plot_row IS NULL")
                            elif issue_filter == 'missing_number':
                                where_conditions.append("plot_number IS NULL")
                            elif issue_filter == 'invalid_col':
                                where_conditions.append("plot_col <= 0")
                            elif issue_filter == 'invalid_row':
                                where_conditions.append("plot_row <= 0")
                            elif issue_filter == 'invalid_number':
                                where_conditions.append("plot_number <= 0")
                            else:
                                # Filter by specific plot_code value
                                where_conditions.append("plot_code = %s")
                                params.append(issue_filter)
                        elif issue_type == 'phy_zone':
                            if issue_filter == 'null_zone':
                                where_conditions.append("phy_zone IS NULL")
                            elif issue_filter == 'invalid_zone':
                                where_conditions.append("(phy_zone < 1 OR phy_zone > 5)")
                            else:
                                # Filter by specific phy_zone value
                                where_conditions.append("phy_zone = %s")
                                params.append(issue_filter)
                        elif issue_type == 'tree_no':
                            if issue_filter == 'null_tree':
                                where_conditions.append("tree_no IS NULL")
                            elif issue_filter == 'invalid_tree':
                                where_conditions.append("tree_no <= 0")
                            else:
                                # Filter by specific tree_no value
                                where_conditions.append("tree_no = %s")
                                params.append(issue_filter)
                        elif issue_type == 'species_code':
                            if issue_filter == 'null_species':
                                where_conditions.append("species_code IS NULL")
                            elif issue_filter == 'invalid_species':
                                where_conditions.append("species_code NOT IN (SELECT code FROM public.forest_species WHERE code IS NOT NULL)")
                            else:
                                # Filter by specific species_code value
                                where_conditions.append("species_code = %s")
                                params.append(issue_filter)
                        elif issue_type == 'dbh':
                            if issue_filter == 'null_dbh':
                                where_conditions.append("dbh IS NULL")
                            elif issue_filter == 'invalid_dbh':
                                where_conditions.append("dbh <= 0")
                            else:
                                # Filter by specific dbh value
                                where_conditions.append("dbh = %s")
                                params.append(issue_filter)
                
                # Add issue-specific conditions
                if issue_type == 'plot_code':
                    where_conditions.append("(plot_col IS NULL OR plot_col <= 0 OR plot_row IS NULL OR plot_row <= 0 OR plot_number IS NULL OR plot_number <= 0)")
                elif issue_type == 'phy_zone':
                    where_conditions.append("(phy_zone IS NULL OR phy_zone < 1 OR phy_zone > 5)")
                    logger.info(f"Added phy_zone condition: (phy_zone IS NULL OR phy_zone < 1 OR phy_zone > 5)")
                elif issue_type == 'tree_no':
                    where_conditions.append("(tree_no IS NULL OR tree_no <= 0)")
                elif issue_type == 'species_code':
                    where_conditions.append("(species_code IS NULL OR species_code NOT IN (SELECT code FROM public.forest_species WHERE code IS NOT NULL))")
                elif issue_type == 'dbh':
                    where_conditions.append("(dbh IS NULL OR dbh <= 0)")
                
                # Add ignored records filter if requested
                if exclude_ignored:
                    where_conditions.append("ignore = FALSE")
                    logger.info("Added ignore filter: ignore = FALSE")
                
                where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
                
                logger.info(f"Final WHERE clause: {where_clause}")
                logger.info(f"Query parameters: {params}")
                
                # Calculate pagination
                offset = (page - 1) * page_size
                
                # Get total count first
                count_query = f"""
                    SELECT COUNT(*) FROM tree_biometric_calc 
                    WHERE {where_clause}
                """
                cursor.execute(count_query, params)
                total_count = cursor.fetchone()[0]
                
                # Get records with issues (paginated)
                if issue_type == 'phy_zone':
                    # For phy_zone issues, include physiography name from public.physiography table
                    query = f"""
                        SELECT 
                            t.calc_id, 
                            t.plot_code, 
                            t.phy_zone, 
                            t.tree_no, 
                            t.phy_zone as phy_zone_value,
                            p.name as physiography_name
                        FROM tree_biometric_calc t
                        LEFT JOIN public.physiography p ON t.phy_zone = p.code
                        WHERE {where_clause}
                        ORDER BY t.plot_code, t.phy_zone, t.tree_no
                        LIMIT %s OFFSET %s
                    """
                elif issue_type == 'species_code':
                    # For species_code issues, return the actual species code
                    query = f"""
                        SELECT 
                            t.calc_id, 
                            t.plot_code, 
                            t.phy_zone, 
                            t.tree_no, 
                            t.species_code,
                            t.dbh,
                            fs.species_name,
                            t.species_code as species_code_value
                        FROM tree_biometric_calc t
                        LEFT JOIN public.forest_species fs ON t.species_code = fs.code
                        WHERE {where_clause}
                        ORDER BY t.plot_code, t.tree_no
                        LIMIT %s OFFSET %s
                    """
                elif issue_type == 'dbh':
                    # For dbh issues, include species name from public.forest_species table
                    query = f"""
                        SELECT 
                            t.calc_id, 
                            t.plot_code, 
                            t.phy_zone, 
                            t.tree_no, 
                            t.species_code,
                            t.dbh,
                            fs.species_name
                        FROM tree_biometric_calc t
                        LEFT JOIN public.forest_species fs ON t.species_code = fs.code
                        WHERE {where_clause}
                        ORDER BY t.plot_code, t.tree_no
                        LIMIT %s OFFSET %s
                    """
                else:
                    query = f"""
                        SELECT calc_id, plot_code, phy_zone, tree_no, species_code, dbh, 
                               plot_col, plot_row, plot_number
                        FROM tree_biometric_calc 
                        WHERE {where_clause}
                        ORDER BY plot_code, tree_no
                        LIMIT %s OFFSET %s
                    """
                
                # Add pagination parameters
                query_params = params + [page_size, offset]
                cursor.execute(query, query_params)
                records = []
                
                logger.info(f"Query executed successfully. Fetching results for page {page}...")
                
                for row in cursor.fetchall():
                    if issue_type == 'phy_zone':
                        records.append({
                            'calc_id': row[0],
                            'plot_code': row[1],
                            'phy_zone': row[2],
                            'tree_no': row[3],
                            'phy_zone_value': row[4],
                            'physiography_name': row[5]
                        })
                    elif issue_type == 'species_code':
                        records.append({
                            'calc_id': row[0],
                            'plot_code': row[1],
                            'phy_zone': row[2],
                            'tree_no': row[3],
                            'species_code': row[4],
                            'dbh': row[5],
                            'species_code_value': row[6]
                        })
                    elif issue_type == 'dbh':
                        records.append({
                            'calc_id': row[0],
                            'plot_code': row[1],
                            'phy_zone': row[2],
                            'tree_no': row[3],
                            'species_code': row[4],
                            'dbh': row[5],
                            'species_name': row[6]
                        })
                    else:
                        records.append({
                            'calc_id': row[0],
                            'plot_code': row[1],
                            'phy_zone': row[2],
                            'tree_no': row[3],
                            'species_code': row[4],
                            'dbh': row[5],
                            'plot_col': row[6],
                            'plot_row': row[7],
                            'plot_number': row[8]
                        })
                
                logger.info(f"Processed {len(records)} records for {issue_type} issues (page {page})")
                
                # Calculate pagination info
                total_pages = (total_count + page_size - 1) // page_size
                has_next = page < total_pages
                has_previous = page > 1
                
                return {
                    'records': records,
                    'total_count': total_count,
                    'page': page,
                    'page_size': page_size,
                    'total_pages': total_pages,
                    'has_next': has_next,
                    'has_previous': has_previous,
                    'start_record': offset + 1,
                    'end_record': min(offset + page_size, total_count)
                }
                
        except Exception as e:
            logger.error(f"Error getting issue details: {str(e)}")
            raise DataQualityError(f"Failed to get issue details: {str(e)}")
    
    def update_record(self, record_id: int, field: str, value: Any) -> bool:
        """Update a single record field"""
        try:
            with transaction.atomic():
                with self.default_connection.cursor() as cursor:
                    cursor.execute(SQL("SET search_path TO {}").format(Identifier(self.schema_name)))
                    
                    # Validate value based on field
                    if field == 'phy_zone':
                        if not (1 <= value <= 5):
                            raise DataQualityError("Physiography zone must be between 1-5")
                    elif field in ['tree_no', 'dbh']:
                        if value <= 0:
                            raise DataQualityError(f"{field} must be greater than 0")
                    elif field == 'species_code':
                        # Check if species exists in public schema
                        cursor.execute("SELECT 1 FROM public.forest_species WHERE code = %s", [value])
                        if not cursor.fetchone():
                            raise DataQualityError("Species code must exist in forest_species table")
                    
                    # Update the record
                    cursor.execute(
                        SQL("UPDATE tree_biometric_calc SET {} = %s WHERE calc_id = %s").format(
                            Identifier(field)
                        ),
                        [value, record_id]
                    )
                    
                    if cursor.rowcount == 0:
                        raise DataQualityError("Record not found")
                    
                    return True
                    
        except Exception as e:
            logger.error(f"Error updating record: {str(e)}")
            raise DataQualityError(f"Failed to update record: {str(e)}")
    
    def bulk_update_records(self, record_ids: List[int], field: str, value: Any) -> int:
        """Bulk update multiple records"""
        try:
            with transaction.atomic():
                with self.default_connection.cursor() as cursor:
                    cursor.execute(SQL("SET search_path TO {}").format(Identifier(self.schema_name)))
                    
                    # Validate value based on field
                    if field == 'phy_zone':
                        if not (1 <= value <= 5):
                            raise DataQualityError("Physiography zone must be between 1-5")
                    elif field in ['tree_no', 'dbh']:
                        if value <= 0:
                            raise DataQualityError(f"{field} must be greater than 0")
                    elif field == 'species_code':
                        # Check if species exists in public schema
                        cursor.execute("SELECT 1 FROM public.forest_species WHERE code = %s", [value])
                        if not cursor.fetchone():
                            raise DataQualityError("Species code must exist in forest_species table")
                    
                    # Bulk update records
                    cursor.execute(
                        SQL("UPDATE tree_biometric_calc SET {} = %s WHERE calc_id = ANY(%s)").format(
                            Identifier(field)
                        ),
                        [value, record_ids]
                    )
                    
                    return cursor.rowcount
                    
        except Exception as e:
            logger.error(f"Error bulk updating records: {str(e)}")
            raise DataQualityError(f"Failed to bulk update records: {str(e)}")

    def get_ignored_records_count(self, issue_type: str, schema_data: Optional[Dict] = None) -> int:
        """Get count of ignored records for a specific issue type"""
        try:
            with self.default_connection.cursor() as cursor:
                cursor.execute(SQL("SET search_path TO {}").format(Identifier(self.schema_name)))
                
                # Build WHERE clause for ignored records with the specific issue
                where_conditions = ["ignore = TRUE"]
                
                if schema_data and schema_data.get('import_id'):
                    where_conditions.append("import_id = %s")
                    params = [schema_data.get('import_id')]
                else:
                    params = []
                
                # Add issue-specific conditions
                if issue_type == 'plot_code':
                    where_conditions.append("(plot_col IS NULL OR plot_col <= 0 OR plot_row IS NULL OR plot_row <= 0 OR plot_number IS NULL OR plot_number <= 0)")
                elif issue_type == 'phy_zone':
                    where_conditions.append("(phy_zone IS NULL OR phy_zone < 1 OR phy_zone > 5)")
                elif issue_type == 'tree_no':
                    where_conditions.append("(tree_no IS NULL OR tree_no <= 0)")
                elif issue_type == 'species_code':
                    where_conditions.append("(species_code IS NULL OR species_code NOT IN (SELECT code FROM public.forest_species WHERE code IS NOT NULL))")
                elif issue_type == 'dbh':
                    where_conditions.append("(dbh IS NULL OR dbh <= 0)")
                
                where_clause = " AND ".join(where_conditions)
                
                cursor.execute(f"""
                    SELECT COUNT(*) FROM tree_biometric_calc 
                    WHERE {where_clause}
                """, params)
                
                count = cursor.fetchone()[0]
                return count
                
        except Exception as e:
            logger.error(f"Error getting ignored records count: {str(e)}")
            return 0

    def ignore_records(self, record_ids: List[int]) -> int:
        """Mark records as ignored"""
        try:
            with transaction.atomic():
                with self.default_connection.cursor() as cursor:
                    cursor.execute(SQL("SET search_path TO {}").format(Identifier(self.schema_name)))
                    
                    # Mark records as ignored
                    cursor.execute(
                        "UPDATE tree_biometric_calc SET ignore = TRUE WHERE calc_id = ANY(%s)",
                        [record_ids]
                    )
                    
                    return cursor.rowcount
                    
        except Exception as e:
            logger.error(f"Error ignoring records: {str(e)}")
            raise DataQualityError(f"Failed to ignore records: {str(e)}")

    def unignore_records(self, record_ids: List[int]) -> int:
        """Unmark records as ignored"""
        try:
            with transaction.atomic():
                with self.default_connection.cursor() as cursor:
                    cursor.execute(SQL("SET search_path TO {}").format(Identifier(self.schema_name)))
                    
                    # Unmark records as ignored
                    cursor.execute(
                        "UPDATE tree_biometric_calc SET ignore = FALSE WHERE calc_id = ANY(%s)",
                        [record_ids]
                    )
                    
                    return cursor.rowcount
                    
        except Exception as e:
            logger.error(f"Error unignoring records: {str(e)}")
            raise DataQualityError(f"Failed to unignore records: {str(e)}")

    def get_ignored_records(self, issue_type: str, filters: Dict = None, page: int = 1, page_size: int = 50) -> Dict[str, Any]:
        """Get ignored records for a specific issue type with pagination"""
        try:
            with self.default_connection.cursor() as cursor:
                cursor.execute(SQL("SET search_path TO {}").format(Identifier(self.schema_name)))
                
                # Build WHERE clause for ignored records
                where_conditions = ["ignore = TRUE"]
                params = []
                
                if filters:
                    if filters.get('plotCode'):
                        where_conditions.append("plot_code ILIKE %s")
                        params.append(f"%{filters['plotCode']}%")
                    
                    if filters.get('phyZone'):
                        where_conditions.append("phy_zone = %s")
                        params.append(filters['phyZone'])
                    
                    if filters.get('treeNo'):
                        where_conditions.append("tree_no = %s")
                        params.append(filters['treeNo'])
                
                # Add issue-specific conditions
                if issue_type == 'plot_code':
                    where_conditions.append("(plot_col IS NULL OR plot_col <= 0 OR plot_row IS NULL OR plot_row <= 0 OR plot_number IS NULL OR plot_number <= 0)")
                elif issue_type == 'phy_zone':
                    where_conditions.append("(phy_zone IS NULL OR phy_zone < 1 OR phy_zone > 5)")
                elif issue_type == 'tree_no':
                    where_conditions.append("(tree_no IS NULL OR tree_no <= 0)")
                elif issue_type == 'species_code':
                    where_conditions.append("(species_code IS NULL OR species_code NOT IN (SELECT code FROM public.forest_species WHERE code IS NOT NULL))")
                elif issue_type == 'dbh':
                    where_conditions.append("(dbh IS NULL OR dbh <= 0)")
                
                where_clause = " AND ".join(where_conditions)
                
                # Calculate pagination
                offset = (page - 1) * page_size
                
                # Get total count first
                count_query = f"""
                    SELECT COUNT(*) FROM tree_biometric_calc 
                    WHERE {where_clause}
                """
                cursor.execute(count_query, params)
                total_count = cursor.fetchone()[0]
                
                # Get ignored records with pagination
                if issue_type == 'phy_zone':
                    query = f"""
                        SELECT 
                            t.calc_id, 
                            t.plot_code, 
                            t.phy_zone, 
                            t.tree_no, 
                            t.phy_zone as phy_zone_value,
                            p.name as physiography_name
                        FROM tree_biometric_calc t
                        LEFT JOIN public.physiography p ON t.phy_zone = p.code
                        WHERE {where_clause}
                        ORDER BY t.plot_code, t.phy_zone, t.tree_no
                        LIMIT %s OFFSET %s
                    """
                elif issue_type == 'species_code':
                    query = f"""
                        SELECT 
                            t.calc_id, 
                            t.plot_code, 
                            t.phy_zone, 
                            t.tree_no, 
                            t.species_code,
                            t.dbh,
                            fs.species_name,
                            t.species_code as species_code_value
                        FROM tree_biometric_calc t
                        LEFT JOIN public.forest_species fs ON t.species_code = fs.code
                        WHERE {where_clause}
                        ORDER BY t.plot_code, t.tree_no
                        LIMIT %s OFFSET %s
                    """
                elif issue_type == 'dbh':
                    query = f"""
                        SELECT 
                            t.calc_id, 
                            t.plot_code, 
                            t.phy_zone, 
                            t.tree_no, 
                            t.species_code,
                            t.dbh,
                            fs.species_name
                        FROM tree_biometric_calc t
                        LEFT JOIN public.forest_species fs ON t.species_code = fs.code
                        WHERE {where_clause}
                        ORDER BY t.plot_code, t.tree_no
                        LIMIT %s OFFSET %s
                    """
                else:
                    query = f"""
                        SELECT calc_id, plot_code, phy_zone, tree_no, species_code, dbh, 
                               plot_col, plot_row, plot_number
                        FROM tree_biometric_calc 
                        WHERE {where_clause}
                        ORDER BY plot_code, tree_no
                        LIMIT %s OFFSET %s
                    """
                
                # Add pagination parameters
                query_params = params + [page_size, offset]
                cursor.execute(query, query_params)
                records = []
                
                for row in cursor.fetchall():
                    if issue_type == 'phy_zone':
                        records.append({
                            'calc_id': row[0],
                            'plot_code': row[1],
                            'phy_zone': row[2],
                            'tree_no': row[3],
                            'phy_zone_value': row[4],
                            'physiography_name': row[5]
                        })
                    elif issue_type == 'species_code':
                        records.append({
                            'calc_id': row[0],
                            'plot_code': row[1],
                            'phy_zone': row[2],
                            'tree_no': row[3],
                            'species_code': row[4],
                            'dbh': row[5],
                            'species_name': row[6],
                            'species_code_value': row[7]
                        })
                    elif issue_type == 'dbh':
                        records.append({
                            'calc_id': row[0],
                            'plot_code': row[1],
                            'phy_zone': row[2],
                            'tree_no': row[3],
                            'species_code': row[4],
                            'dbh': row[5],
                            'species_name': row[6]
                        })
                    else:
                        records.append({
                            'calc_id': row[0],
                            'plot_code': row[1],
                            'phy_zone': row[2],
                            'tree_no': row[3],
                            'species_code': row[4],
                            'dbh': row[5],
                            'plot_col': row[6],
                            'plot_row': row[7],
                            'plot_number': row[8]
                        })
                
                # Calculate pagination info
                total_pages = (total_count + page_size - 1) // page_size
                has_next = page < total_pages
                has_previous = page > 1
                
                return {
                    'records': records,
                    'total_count': total_count,
                    'page': page,
                    'page_size': page_size,
                    'total_pages': total_pages,
                    'has_next': has_next,
                    'has_previous': has_previous,
                    'start_record': offset + 1,
                    'end_record': min(offset + page_size, total_count)
                }
                
        except Exception as e:
            logger.error(f"Error getting ignored records: {str(e)}")
            raise DataQualityError(f"Failed to get ignored records: {str(e)}")
