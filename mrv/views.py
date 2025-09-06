from django.shortcuts import render, redirect
from django.contrib import messages
from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.contrib.auth.decorators import login_required
from django.core.exceptions import ValidationError
from django.db import transaction
import json
import io
import matplotlib
import numpy as np
import pandas as pd
from datetime import datetime
import re

from mrv.models import Project, Physiography, ProjectDataImportManager
from mrv.serializers import ProjectSerializer
from mrv.data_import_utils import DataImportService, DataImportError
from mrv.data_quality_utils import DataQualityService, DataQualityError
from psycopg2.sql import SQL, Identifier
from django.db import connections
from sympy import symbols, exp, log, sqrt, parse_expr
from sympy.core.sympify import SympifyError
from django.conf import settings
import math

matplotlib.use('Agg')
import matplotlib.pyplot as plt
import base64

# API Views for Project Management
@csrf_exempt
@require_http_methods(["GET"])
def api_projects_list(request):
    """API endpoint to list all projects"""
    try:
        projects = Project.objects.all()
        serializer = ProjectSerializer(projects, many=True)
        return JsonResponse({
            'success': True,
            'projects': serializer.data
        })
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@csrf_exempt
@require_http_methods(["GET"])
def api_project_detail(request, project_id):
    """API endpoint to get project details"""
    try:
        project = Project.objects.get(id=project_id)
        serializer = ProjectSerializer(project)
        return JsonResponse({
            'success': True,
            'project': serializer.data
        })
    except Project.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Project not found'
        }, status=404)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@csrf_exempt
@require_http_methods(["POST"])
def api_project_create(request):
    """API endpoint to create a new project"""
    try:
        data = json.loads(request.body)
        
        # Validate required fields
        if not data.get('name'):
            return JsonResponse({
                'success': False,
                'error': 'Project name is required'
            }, status=400)
         
        # Validate project name format
        if not re.match(r'^[a-zA-Z0-9_-]+$', data['name']):
            return JsonResponse({
                'success': False,
                'error': 'Project name can only contain letters, numbers, underscores (_), and hyphens (-).'
            }, status=400)
        
        # Check if project name already exists
        if Project.objects.filter(name=data['name']).exists():
            return JsonResponse({
                'success': False,
                'error': 'Project with this name already exists'
            }, status=400)
        
        # Create project
        project_data = {
            'name': data['name'],
            'description': data.get('description', ''),
            'status': 'draft',
            'current_phase': 1,
            'current_step': data.get('current_step', 1),
            'created_by': request.user if request.user.is_authenticated else None
        }
        
        project = Project.objects.create(**project_data)
        serializer = ProjectSerializer(project)
        
        # Check if schema was created successfully
        schema_created = project.schema_exists()
        
        # Get information about created tables
        tables_info = {}
        if schema_created:
            tables = project.get_schema_tables()
            tables_info = {
                'tables': tables,
                'tree_biometric_calc_exists': project.table_exists('tree_biometric_calc')
            }
        
        return JsonResponse({
            'success': True,
            'project': serializer.data,
            'message': 'Project created successfully',
            'schema_created': schema_created,
            'schema_name': project.get_schema_name() if schema_created else None,
            'tables_info': tables_info
        }, status=201)
        
    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON data'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@csrf_exempt
@require_http_methods(["PUT", "PATCH"])
def api_project_update(request, project_id):
    """API endpoint to update project details"""
    try:
        project = Project.objects.get(id=project_id)
        data = json.loads(request.body)
        
        # Update allowed fields
        allowed_fields = ['description', 'status', 'current_phase', 'current_step']
        for field in allowed_fields:
            if field in data:
                setattr(project, field, data[field])
        
        project.save()
        serializer = ProjectSerializer(project)
        
        return JsonResponse({
            'success': True,
            'project': serializer.data,
            'message': 'Project updated successfully'
        })
        
    except Project.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Project not found'
        }, status=404)
    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON data'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@csrf_exempt
@require_http_methods(["DELETE"])
def api_project_delete(request, project_id):
    """API endpoint to delete a project"""
    try:
        project = Project.objects.get(id=project_id)
        
        # Check if schema exists before deletion
        schema_existed = project.schema_exists()
        schema_name = project.get_schema_name() if schema_existed else None
        
        # Delete project (this will also delete the schema)
        project.delete()
        
        return JsonResponse({
            'success': True,
            'message': 'Project deleted successfully',
            'schema_deleted': schema_existed,
            'schema_name': schema_name
        })
        
    except Project.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Project not found'
        }, status=404)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@csrf_exempt
@require_http_methods(["GET"])
def api_physiography_list(request):
    """API endpoint to list all physiography options"""
    try:
        physiography_list = Physiography.objects.all().values('code', 'name', 'ecological')
        return JsonResponse({
            'success': True,
            'physiography': list(physiography_list)
        })
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@csrf_exempt
@require_http_methods(["GET"])
def api_forest_species_list(request):
    """API endpoint to list all forest species options"""
    try:
        from .models import ForestSpecies
        species_list = ForestSpecies.objects.all().values('code', 'species_name', 'species', 'family', 'scientific_name', 'name')
        return JsonResponse({
            'success': True,
            'species': list(species_list)
        })
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@csrf_exempt
@require_http_methods(["POST"])
def api_project_physiography_options(request, project_id):
    """API endpoint to get physiography options for a specific project's data"""
    try:
        project = Project.objects.get(id=project_id)
        data = json.loads(request.body)
        
        issue_type = data.get('issue_type')
        filters = data.get('filters', {})
        
        if not issue_type:
            return JsonResponse({
                'success': False,
                'error': 'issue_type is required'
            }, status=400)
        
        # Get unique phy_zone values from the current issue data
        schema_name = project.get_schema_name()
        with connections['default'].cursor() as cursor:
            cursor.execute(SQL("SET search_path TO {}").format(Identifier(schema_name)))
            
            # Build WHERE clause based on filters
            where_conditions = []
            params = []
            
            if filters.get('plotCode'):
                where_conditions.append("plot_code ILIKE %s")
                params.append(f"%{filters['plotCode']}%")
                
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
            
            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
            
            # Get unique phy_zone values
            query = f"""
                SELECT DISTINCT phy_zone 
                FROM tree_biometric_calc 
                WHERE {where_clause} AND phy_zone IS NOT NULL
                ORDER BY phy_zone
            """
            
            cursor.execute(query, params)
            phy_zone_values = [row[0] for row in cursor.fetchall()]
        
        # Get physiography data for these codes from public schema
        physiography_options = []
        
        # Use raw SQL to access public.physiography table
        with connections['default'].cursor() as cursor:
            # Reset search path to public schema
            cursor.execute("SET search_path TO public")
            
            if phy_zone_values:
                # Get physiography data for the found phy_zone values
                placeholders = ','.join(['%s'] * len(phy_zone_values))
                physiography_query = f"""
                    SELECT code, name, ecological 
                    FROM physiography 
                    WHERE code IN ({placeholders})
                    ORDER BY code
                """
                
                cursor.execute(physiography_query, phy_zone_values)
                physiography_options = [
                    {
                        'code': row[0],
                        'name': row[1],
                        'ecological': row[2]
                    }
                    for row in cursor.fetchall()
                ]
            else:
                # If no specific phy_zone values found, get all physiography options
                cursor.execute("""
                    SELECT code, name, ecological 
                    FROM physiography 
                    ORDER BY code
                """)
                physiography_options = [
                    {
                        'code': row[0],
                        'name': row[1],
                        'ecological': row[2]
                    }
                    for row in cursor.fetchall()
                ]
        
        return JsonResponse({
            'success': True,
            'physiography_options': physiography_options,
            'phy_zone_values': phy_zone_values,
            'debug_info': {
                'issue_type': issue_type,
                'filters': filters,
                'total_physiography_options': len(physiography_options),
                'total_phy_zone_values': len(phy_zone_values)
            }
        })
        
    except Project.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Project not found'
        }, status=404)
    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON data'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@csrf_exempt
@require_http_methods(["GET"])
def api_project_schema_info(request, project_id):
    """API endpoint to get project schema and table information"""
    try:
        project = Project.objects.get(id=project_id)
        
        schema_exists = project.schema_exists()
        schema_info = {
            'schema_name': project.get_schema_name(),
            'schema_exists': schema_exists,
            'tables': []
        }
        
        if schema_exists:
            tables = project.get_schema_tables()
            schema_info['tables'] = tables
            
            # Get detailed info for tree_biometric_calc table if it exists
            if project.table_exists('tree_biometric_calc'):
                table_structure = project.get_table_structure('tree_biometric_calc')
                schema_info['tree_biometric_calc_structure'] = [
                    {
                        'column_name': row[0],
                        'data_type': row[1],
                        'is_nullable': row[2],
                        'column_default': row[3]
                    }
                    for row in table_structure
                ]
        
        return JsonResponse({
            'success': True,
            'project_id': project_id,
            'project_name': project.name,
            'schema_info': schema_info
        })
        
    except Project.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Project not found'
        }, status=404)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

# Data Import API Views
@csrf_exempt
@require_http_methods(["GET"])
def api_project_data_imports_list(request, project_id):
    """API endpoint to list all data imports for a project"""
    try:
        project = Project.objects.get(id=project_id)
        import_manager = ProjectDataImportManager(project)
        
        imports_data = import_manager.list_imports()
        
        return JsonResponse({
            'success': True,
            'results': imports_data,
            'total_imports': len(imports_data),
            'project_id': project_id,
            'project_name': project.name
        })
        
    except Project.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Project not found'
        }, status=404)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@csrf_exempt
@require_http_methods(["POST"])
def api_project_data_import_preview(request, project_id):
    """API endpoint to preview data from foris_connection before import"""
    try:
        project = Project.objects.get(id=project_id)
        data = json.loads(request.body)
        
        schema_name = data.get('schema_name')
        table_name = data.get('table_name')
        
        if not schema_name or not table_name:
            return JsonResponse({
                'success': False,
                'error': 'schema_name and table_name are required'
            }, status=400)
        
        # Initialize data import service and get preview data
        with DataImportService() as import_service:
            preview_data = import_service.get_foris_table_preview(schema_name, table_name)
        
        return JsonResponse({
            'success': True,
            'preview_data': preview_data,
            'project_id': project_id,
            'source': {
                'schema_name': schema_name,
                'table_name': table_name
            }
        })
        
    except Project.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Project not found'
        }, status=404)
    except DataImportError as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=400)
    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON data'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@csrf_exempt
@require_http_methods(["POST"])
def api_project_data_import_create(request, project_id):
    """API endpoint to create and execute a data import"""
    try:
        project = Project.objects.get(id=project_id)
        data = json.loads(request.body)
        
        # Validate required fields
        required_fields = ['schema_name', 'table_name']
        for field in required_fields:
            if not data.get(field):
                return JsonResponse({
                    'success': False,
                    'error': f'{field} is required'
                }, status=400)
        
        schema_name = data['schema_name']
        table_name = data['table_name']
        action = data.get('action', 'append')
        description = data.get('description', f'Import from {schema_name}.{table_name}')
        
        # Validate action
        if action not in ['append', 'replace', 'replace_selected']:
            return JsonResponse({
                'success': False,
                'error': 'action must be either "append", "replace", or "replace_selected"'
            }, status=400)
        
        # Create data import record in project schema
        import_manager = ProjectDataImportManager(project)
        import_id = import_manager.create_import_record(
            schema_name=schema_name,
            table_name=table_name,
            action=action,
            description=description
        )
        
        # Initialize import service and execute import
        with DataImportService() as import_service:
            success, message, imported_rows = import_service.import_data_to_project(
                project, import_id, schema_name, table_name, action
            )
        
        # Get the updated import record
        data_import = import_manager.get_import_by_id(import_id)
        
        if success:
            return JsonResponse({
                'success': True,
                'message': message,
                'data_import': data_import,
                'imported_rows': imported_rows
            }, status=201)
        else:
            return JsonResponse({
                'success': False,
                'error': message,
                'data_import': data_import
            }, status=400)
        
    except Project.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Project not found'
        }, status=404)
    except DataImportError as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=400)
    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON data'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@csrf_exempt
@require_http_methods(["GET"])
def api_project_data_import_detail(request, project_id, import_id):
    """API endpoint to get details of a specific data import"""
    try:
        project = Project.objects.get(id=project_id)
        import_manager = ProjectDataImportManager(project)
        
        data_import = import_manager.get_import_by_id(import_id)
        
        if not data_import:
            return JsonResponse({
                'success': False,
                'error': 'Data import not found'
            }, status=404)
        
        return JsonResponse({
            'success': True,
            'data_import': data_import,
            'project_id': project_id,
            'project_name': project.name
        })
        
    except Project.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Project not found'
        }, status=404)
    
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@csrf_exempt
@require_http_methods(["DELETE"])
def api_project_data_import_delete(request, project_id, import_id):
    """API endpoint to delete a data import"""
    try:
        project = Project.objects.get(id=project_id)
        
        # Initialize import service and delete the import
        with DataImportService() as import_service:
            success, message = import_service.delete_project_import(project, import_id)
        
        if success:
            return JsonResponse({
                'success': True,
                'message': message,
                'project_id': project_id,
                'import_id': import_id
            })
        else:
            return JsonResponse({
                'success': False,
                'error': message
            }, status=400)
        
    except Project.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Project not found'
        }, status=404)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

# Data Quality Check API Views
@csrf_exempt
@require_http_methods(["POST"])
def api_project_data_quality_check(request, project_id):
    """API endpoint to perform data quality check"""
    try:
        project = Project.objects.get(id=project_id)
        data = json.loads(request.body)
        
        check_type = data.get('check_type', 'all')
        schema_data = data.get('schema_data')
        
        if check_type not in ['selected', 'all']:
            return JsonResponse({
                'success': False,
                'error': 'check_type must be either "selected" or "all"'
            }, status=400)
        
        if check_type == 'selected' and not schema_data:
            return JsonResponse({
                'success': False,
                'error': 'schema_data is required when check_type is "selected"'
            }, status=400)
        
        # Initialize data quality service and perform check
        quality_service = DataQualityService(project)
        results = quality_service.perform_quality_check(check_type, schema_data)
        
        return JsonResponse({
            'success': True,
            'results': results
        })
        
    except Project.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Project not found'
        }, status=404)
    except DataQualityError as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=400)
    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON data'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@csrf_exempt
@require_http_methods(["POST"])
def api_project_data_quality_issue_details(request, project_id, issue_type):
    """API endpoint to get detailed records for a specific issue type"""
    try:
        project = Project.objects.get(id=project_id)
        data = json.loads(request.body)
        
        filters = data.get('filters', {})
        page = data.get('page', 1)
        page_size = data.get('page_size', 50)
        
        # Validate issue type
        valid_issue_types = ['plot_code', 'phy_zone', 'tree_no', 'species_code', 'dbh']
        if issue_type not in valid_issue_types:
            return JsonResponse({
                'success': False,
                'error': f'Invalid issue_type. Must be one of: {", ".join(valid_issue_types)}'
            }, status=400)
        
        # Validate pagination parameters
        try:
            page = int(page)
            page_size = int(page_size)
        except (ValueError, TypeError):
            return JsonResponse({
                'success': False,
                'error': 'page and page_size must be valid integers'
            }, status=400)
        
        if page < 1:
            return JsonResponse({
                'success': False,
                'error': 'page must be a positive integer'
            }, status=400)
        
        if page_size < 1 or page_size > 1000:
            return JsonResponse({
                'success': False,
                'error': 'page_size must be a positive integer between 1 and 1000'
            }, status=400)
        
        # Check if we should exclude ignored records
        exclude_ignored = filters.get('exclude_ignored', False)
        
        # Initialize data quality service and get issue details
        quality_service = DataQualityService(project)
        details = quality_service.get_issue_details(issue_type, filters, page, page_size, exclude_ignored)
        
        return JsonResponse({
            'success': True,
            'details': details
        })
        
    except Project.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Project not found'
        }, status=404)
    except DataQualityError as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=400)
    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON data'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@csrf_exempt
@require_http_methods(["PUT"])
def api_project_data_quality_update_record(request, project_id):
    """API endpoint to update a single record"""
    try:
        project = Project.objects.get(id=project_id)
        data = json.loads(request.body)
        
        # Validate required fields
        required_fields = ['record_id', 'issue_type', 'field', 'value']
        for field in required_fields:
            if field not in data:
                return JsonResponse({
                    'success': False,
                    'error': f'{field} is required'
                }, status=400)
        
        record_id = data['record_id']
        issue_type = data['issue_type']
        field = data['field']
        value = data['value']
        
        # Validate issue type
        valid_issue_types = ['plot_code', 'phy_zone', 'tree_no', 'species_code', 'dbh']
        if issue_type not in valid_issue_types:
            return JsonResponse({
                'success': False,
                'error': f'Invalid issue_type. Must be one of: {", ".join(valid_issue_types)}'
            }, status=400)
        
        # Initialize data quality service and update record
        quality_service = DataQualityService(project)
        success = quality_service.update_record(record_id, field, value)
        
        if success:
            return JsonResponse({
                'success': True,
                'message': 'Record updated successfully'
            })
        else:
            return JsonResponse({
                'success': False,
                'error': 'Failed to update record'
            }, status=400)
        
    except Project.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Project not found'
        }, status=404)
    except DataQualityError as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=400)
    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON data'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@csrf_exempt
@require_http_methods(["PUT"])
def api_project_data_quality_bulk_update(request, project_id):
    """API endpoint to bulk update multiple records"""
    try:
        project = Project.objects.get(id=project_id)
        data = json.loads(request.body)
        
        # Validate required fields
        required_fields = ['issue_type', 'record_ids', 'value']
        for field in required_fields:
            if field not in data:
                return JsonResponse({
                    'success': False,
                    'error': f'{field} is required'
                }, status=400)
        
        issue_type = data['issue_type']
        record_ids = data['record_ids']
        value = data['value']
        
        # Validate issue type
        valid_issue_types = ['plot_code', 'phy_zone', 'tree_no', 'species_code', 'dbh']
        if issue_type not in valid_issue_types:
            return JsonResponse({
                'success': False,
                'error': f'Invalid issue_type. Must be one of: {", ".join(valid_issue_types)}'
            }, status=400)
        
        # Validate record_ids is a list
        if not isinstance(record_ids, list):
            return JsonResponse({
                'success': False,
                'error': 'record_ids must be a list'
            }, status=400)
        
        # Initialize data quality service and bulk update records
        quality_service = DataQualityService(project)
        updated_count = quality_service.bulk_update_records(record_ids, issue_type, value)
        
        return JsonResponse({
            'success': True,
            'message': f'{updated_count} records updated successfully',
            'updated_count': updated_count
        })
        
    except Project.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Project not found'
        }, status=404)
    except DataQualityError as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=400)
    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON data'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@csrf_exempt
@require_http_methods(["PUT"])
def api_project_data_quality_ignore_records(request, project_id):
    """API endpoint to ignore multiple records"""
    try:
        project = Project.objects.get(id=project_id)
        data = json.loads(request.body)
        
        # Validate required fields
        required_fields = ['record_ids']
        for field in required_fields:
            if field not in data:
                return JsonResponse({
                    'success': False,
                    'error': f'{field} is required'
                }, status=400)
        
        record_ids = data['record_ids']
        
        # Validate record_ids is a list
        if not isinstance(record_ids, list):
            return JsonResponse({
                'success': False,
                'error': 'record_ids must be a list'
            }, status=400)
        
        # Initialize data quality service and ignore records
        quality_service = DataQualityService(project)
        ignored_count = quality_service.ignore_records(record_ids)
        
        return JsonResponse({
            'success': True,
            'message': f'{ignored_count} records ignored successfully',
            'ignored_count': ignored_count
        })
        
    except Project.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Project not found'
        }, status=404)
    except DataQualityError as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=400)
    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON data'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@csrf_exempt
@require_http_methods(["PUT"])
def api_project_data_quality_unignore_records(request, project_id):
    """API endpoint to unignore multiple records"""
    try:
        project = Project.objects.get(id=project_id)
        data = json.loads(request.body)
        
        # Validate required fields
        required_fields = ['record_ids']
        for field in required_fields:
            if field not in data:
                return JsonResponse({
                    'success': False,
                    'error': f'{field} is required'
                }, status=400)
        
        record_ids = data['record_ids']
        
        # Validate record_ids is a list
        if not isinstance(record_ids, list):
            return JsonResponse({
                'success': False,
                'error': 'record_ids must be a list'
            }, status=400)
        
        # Initialize data quality service and unignore records
        quality_service = DataQualityService(project)
        unignored_count = quality_service.unignore_records(record_ids)
        
        return JsonResponse({
            'success': True,
            'message': f'{unignored_count} records unignored successfully',
            'unignored_count': unignored_count
        })
        
    except Project.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Project not found'
        }, status=404)
    except DataQualityError as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=400)
    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON data'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@csrf_exempt
@require_http_methods(["POST"])
def api_project_data_quality_ignored_records(request, project_id, issue_type):
    """API endpoint to get ignored records for a specific issue type"""
    try:
        project = Project.objects.get(id=project_id)
        data = json.loads(request.body)
        
        filters = data.get('filters', {})
        page = data.get('page', 1)
        page_size = data.get('page_size', 50)
        
        # Validate issue type
        valid_issue_types = ['plot_code', 'phy_zone', 'tree_no', 'species_code', 'dbh']
        if issue_type not in valid_issue_types:
            return JsonResponse({
                'success': False,
                'error': f'Invalid issue_type. Must be one of: {", ".join(valid_issue_types)}'
            }, status=400)
        
        # Validate pagination parameters
        try:
            page = int(page)
            page_size = int(page_size)
        except (ValueError, TypeError):
            return JsonResponse({
                'success': False,
                'error': 'page and page_size must be valid integers'
            }, status=400)
        
        if page < 1:
            return JsonResponse({
                'success': False,
                'error': 'page must be a positive integer'
            }, status=400)
        
        if page_size < 1 or page_size > 1000:
            return JsonResponse({
                'success': False,
                'error': 'page_size must be a positive integer between 1 and 1000'
            }, status=400)
        
        # Initialize data quality service and get ignored records
        quality_service = DataQualityService(project)
        details = quality_service.get_ignored_records(issue_type, filters, page, page_size)
        
        return JsonResponse({
            'success': True,
            'details': details
        })
        
    except Project.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Project not found'
        }, status=404)
    except DataQualityError as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=400)
    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON data'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

# Data Cleaning API Views
@csrf_exempt
@require_http_methods(["GET"])
def api_project_data_cleaning_summary(request, project_id):
    """API endpoint to get data cleaning summary for a project"""
    try:
        project = Project.objects.get(id=project_id)
        
        # Get project schema name
        schema_name = project.get_schema_name()
        
        # Get total records count
        with connections['default'].cursor() as cursor:
            cursor.execute(SQL("SET search_path TO {}").format(Identifier(schema_name)))
            cursor.execute("SELECT COUNT(*) FROM tree_biometric_calc")
            total_records = cursor.fetchone()[0]
            
            # Get ignored records count
            cursor.execute("SELECT COUNT(*) FROM tree_biometric_calc WHERE ignore = TRUE")
            ignored_records = cursor.fetchone()[0]
        
        return JsonResponse({
            'success': True,
            'summary': {
                'totalRecords': total_records,
                'ignoredRecords': ignored_records
            }
        })
        
    except Project.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Project not found'
        }, status=404)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@csrf_exempt
@require_http_methods(["POST"])
def api_project_data_cleaning_remove_ignored(request, project_id):
    """API endpoint to remove ignored records from a project"""
    try:
        project = Project.objects.get(id=project_id)
        
        # Get project schema name
        schema_name = project.get_schema_name()
        
        # Get count of ignored records before removal
        with connections['default'].cursor() as cursor:
            cursor.execute(SQL("SET search_path TO {}").format(Identifier(schema_name)))
            cursor.execute("SELECT COUNT(*) FROM tree_biometric_calc WHERE ignore = TRUE")
            ignored_count_before = cursor.fetchone()[0]
        
        if ignored_count_before == 0:
            return JsonResponse({
                'success': True,
                'message': 'No ignored records to remove',
                'removed_count': 0
            })
        
        # Remove ignored records
        with connections['default'].cursor() as cursor:
            cursor.execute(SQL("SET search_path TO {}").format(Identifier(schema_name)))
            cursor.execute("DELETE FROM tree_biometric_calc WHERE ignore = TRUE")
            removed_count = cursor.rowcount
        
        # Verify removal
        with connections['default'].cursor() as cursor:
            cursor.execute(SQL("SET search_path TO {}").format(Identifier(schema_name)))
            cursor.execute("SELECT COUNT(*) FROM tree_biometric_calc WHERE ignore = TRUE")
            ignored_count_after = cursor.fetchone()[0]
        
        if ignored_count_after > 0:
            return JsonResponse({
                'success': False,
                'error': f'Failed to remove all ignored records. {ignored_count_after} records still remain.'
            }, status=500)
        
        return JsonResponse({
            'success': True,
            'message': f'Successfully removed {removed_count} ignored records',
            'removed_count': removed_count,
            'ignored_count_before': ignored_count_before,
            'ignored_count_after': ignored_count_after
        })
        
    except Project.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Project not found'
        }, status=404)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@csrf_exempt
@require_http_methods(["POST"])
def api_project_data_cleaning_view_records(request, project_id):
    """API endpoint to view all records with pagination and filtering"""
    try:
        project = Project.objects.get(id=project_id)
        data = json.loads(request.body)
        
        # Get filter parameters
        filters = data.get('filters', {})
        page = data.get('page', 1)
        page_size = data.get('page_size', 50)
        
        # Validate pagination parameters
        try:
            page = int(page)
            page_size = int(page_size)
        except (ValueError, TypeError):
            return JsonResponse({
                'success': False,
                'error': 'page and page_size must be valid integers'
            }, status=400)
        
        if page < 1:
            return JsonResponse({
                'success': False,
                'error': 'page must be a positive integer'
            }, status=400)
        
        if page_size < 1 or page_size > 1000:
            return JsonResponse({
                'success': False,
                'error': 'page_size must be a positive integer between 1 and 1000'
            }, status=400)
        
        # Get project schema name
        schema_name = project.get_schema_name()
        
        # Build WHERE clause based on filters
        where_conditions = []
        params = []
        
        if filters.get('plot_code'):
            where_conditions.append("plot_code ILIKE %s")
            params.append(f"%{filters['plot_code']}%")
        
        if filters.get('phy_zone'):
            where_conditions.append("phy_zone = %s")
            params.append(filters['phy_zone'])
        
        if filters.get('species_code'):
            where_conditions.append("species_code = %s")
            params.append(filters['species_code'])
        
        if filters.get('tree_no'):
            where_conditions.append("tree_no = %s")
            params.append(filters['tree_no'])
        
        # Add base condition to exclude ignored records
        where_conditions.append("ignore = FALSE")
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "ignore = FALSE"
        
        # Get total count for pagination
        with connections['default'].cursor() as cursor:
            cursor.execute(SQL("SET search_path TO {}").format(Identifier(schema_name)))
            
            count_query = f"SELECT COUNT(*) FROM tree_biometric_calc WHERE {where_clause}"
            cursor.execute(count_query, params)
            total_records = cursor.fetchone()[0]
            
            # Calculate pagination
            total_pages = (total_records + page_size - 1) // page_size
            offset = (page - 1) * page_size
            
            # Get records with pagination
            records_query = f"""
                SELECT 
                    t.calc_id, t.plot_code, t.phy_zone, t.tree_no, t.species_code, t.dbh, t.height,
                    t.plot_col, t.plot_row, t.plot_number, t.plot_x, t.plot_y, t.tree_x, t.tree_y,
                    t.quality_class, t.crown_class, t.base_tree_height, t.crown_height, 
                    t.base_crown_height, t.base_slope, t.age, t.radial_growth, t.ignore, 
                    t.created_date, t.updated_date, f.species_name
                FROM tree_biometric_calc t
                LEFT JOIN public.forest_species f ON t.species_code = f.code
                WHERE {where_clause}
                ORDER BY t.plot_code, t.tree_no
                LIMIT %s OFFSET %s
            """
            
            cursor.execute(records_query, params + [page_size, offset])
            records = []
            
            for row in cursor.fetchall():
                record = {
                    'calc_id': row[0],
                    'plot_code': row[1],
                    'phy_zone': row[2],
                    'tree_no': row[3],
                    'species_code': row[4],
                    'dbh': row[5],
                    'height': row[6],
                    'plot_col': row[7],
                    'plot_row': row[8],
                    'plot_number': row[9],
                    'plot_x': row[10],
                    'plot_y': row[11],
                    'tree_x': row[12],
                    'tree_y': row[13],
                    'quality_class': row[14],
                    'crown_class': row[15],
                    'base_tree_height': row[16],
                    'crown_height': row[17],
                    'base_crown_height': row[18],
                    'base_slope': row[19],
                    'age': row[20],
                    'radial_growth': row[21],
                    'ignore': row[22],
                    'created_date': row[23].isoformat() if row[23] else None,
                    'updated_date': row[24].isoformat() if row[24] else None,
                    'species_name': row[25]
                }
                records.append(record)
        
        return JsonResponse({
            'success': True,
            'records': records,
            'pagination': {
                'page': page,
                'page_size': page_size,
                'total_records': total_records,
                'total_pages': total_pages,
                'has_next': page < total_pages,
                'has_previous': page > 1
            },
            'filters': filters
        })
        
    except Project.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Project not found'
        }, status=404)
    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON data'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@csrf_exempt
@require_http_methods(["GET"])
def api_hd_model_list(request):
    """API endpoint to list all HD models"""
    try:
        from .models import HDModel
        hd_models = HDModel.objects.all().order_by('id').values('code', 'name', 'description')
        return JsonResponse({
            'success': True,
            'hd_models': list(hd_models)
        })
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@csrf_exempt
@require_http_methods(["GET"])
def api_project_hd_model_physiography_summary(request, project_id):
    """API endpoint to get physiography zone summary for HD modeling"""
    try:
        project = Project.objects.get(id=project_id)
        
        # Get project schema name
        schema_name = project.get_schema_name()
        
        # Execute SQL query to get physiography zone summary with names
        with connections['default'].cursor() as cursor:
            cursor.execute(SQL("SET search_path TO {}").format(Identifier(schema_name)))
            
            # Execute the summary query with physiography names, excluding ignored records
            cursor.execute("""
                SELECT 
                    t.phy_zone,
                    p.name AS physiography_name,
                    COUNT(DISTINCT t.species_code) AS species_count,
                    COUNT(t.tree_no) AS tree_count,
                    COUNT(CASE WHEN t.hd_model_code IS NOT NULL THEN 1 END) AS assigned_hd_model_count,
                    COUNT(CASE WHEN t.hd_model_code IS NULL THEN 1 END) AS unassigned_hd_model_count,
                    COUNT(DISTINCT CASE WHEN t.hd_model_code IS NULL THEN t.species_code END) AS unassigned_species_count,
                    COUNT(CASE WHEN t.crown_class = 6 THEN 1 END) AS broken_trees,
                    COUNT(CASE WHEN t.crown_class != 6 OR t.crown_class IS NULL THEN 1 END) AS non_broken_trees
                FROM tree_biometric_calc t
                LEFT JOIN public.physiography p ON t.phy_zone = p.code
                WHERE t.ignore = FALSE
                GROUP BY t.phy_zone, p.name
                ORDER BY t.phy_zone
            """)
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'phy_zone': row[0],
                    'physiography_name': row[1] or f'Zone {row[0]}',  # Fallback if name is null
                    'species_count': row[2],
                    'tree_count': row[3],
                    'assigned_hd_model_count': row[4],
                    'unassigned_hd_model_count': row[5],
                    'unassigned_species_count': row[6],
                    'broken_trees': row[7],
                    'non_broken_trees': row[8]
                })
        
        return JsonResponse({
            'success': True,
            'physiography_summary': results,
            'project_id': project_id,
            'project_name': project.name
        })
        
    except Project.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Project not found'
        }, status=404)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@csrf_exempt
@require_http_methods(["POST"])
def api_project_hd_model_assign_models(request, project_id):
    """API endpoint to assign HD models to trees based on species and physiography"""
    try:
        project = Project.objects.get(id=project_id)
        
        # Get project schema name
        schema_name = project.get_schema_name()
        
        with connections['default'].cursor() as cursor:
            cursor.execute(SQL("SET search_path TO {}").format(Identifier(schema_name)))
            
            # Update hd_model_code by joining with species_hd_model_map
            cursor.execute("""
                UPDATE tree_biometric_calc t
                SET hd_model_code = m.hd_model_code,
                    updated_date = CURRENT_TIMESTAMP
                FROM public.species_hd_model_map m
                WHERE t.species_code = m.species_code 
                AND t.phy_zone = m.physio_code
                AND t.ignore = FALSE
                AND t.hd_model_code IS NULL
            """)
            
            updated_count = cursor.rowcount
            
            # Get updated counts for physiography zones
            cursor.execute("""
                SELECT 
                    t.phy_zone,
                    p.name AS physiography_name,
                    COUNT(DISTINCT t.species_code) AS species_count,
                    COUNT(t.tree_no) AS tree_count,
                    COUNT(CASE WHEN t.hd_model_code IS NOT NULL THEN 1 END) AS assigned_hd_model_count,
                    COUNT(CASE WHEN t.hd_model_code IS NULL THEN 1 END) AS unassigned_hd_model_count,
                    COUNT(DISTINCT CASE WHEN t.hd_model_code IS NULL THEN t.species_code END) AS unassigned_species_count
                FROM tree_biometric_calc t
                LEFT JOIN public.physiography p ON t.phy_zone = p.code
                WHERE t.ignore = FALSE
                GROUP BY t.phy_zone, p.name
                ORDER BY t.phy_zone
            """)
            
            updated_summary = []
            for row in cursor.fetchall():
                updated_summary.append({
                    'phy_zone': row[0],
                    'physiography_name': row[1] or f'Zone {row[0]}',
                    'species_count': row[2],
                    'tree_count': row[3],
                    'assigned_hd_model_count': row[4],
                    'unassigned_hd_model_count': row[5],
                    'unassigned_species_count': row[6]
                })
        
        return JsonResponse({
            'success': True,
            'message': f'Successfully assigned HD models to {updated_count} trees',
            'updated_count': updated_count,
            'physiography_summary': updated_summary,
            'project_id': project_id,
            'project_name': project.name
        })
        
    except Project.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Project not found'
        }, status=404)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@csrf_exempt
@require_http_methods(["POST"])
def api_project_hd_model_update_species_mapping(request, project_id):
    """API endpoint to update species-HD model mappings"""
    try:
        project = Project.objects.get(id=project_id)
        data = json.loads(request.body)
        
        # Validate required fields
        if 'mappings' not in data:
            return JsonResponse({
                'success': False,
                'error': 'mappings field is required'
            }, status=400)
        
        mappings = data['mappings']
        if not isinstance(mappings, list):
            return JsonResponse({
                'success': False,
                'error': 'mappings must be a list'
            }, status=400)
        
        # Track results for each mapping
        results = []
        success_count = 0
        error_count = 0
        
        for mapping in mappings:
            try:
                # Validate mapping data
                required_fields = ['species_code', 'hd_model_code', 'hd_a', 'hd_b']
                for field in required_fields:
                    if field not in mapping or mapping[field] is None or mapping[field] == '':
                        raise ValueError(f'Missing required field: {field}')
                
                species_code = int(mapping['species_code'])
                hd_model_code = int(mapping['hd_model_code'])
                hd_a = float(mapping['hd_a'])
                hd_b = float(mapping['hd_b']) if mapping['hd_b'] is not None else None
                hd_c = float(mapping['hd_c']) if mapping.get('hd_c') is not None else None
                phy_zone = int(mapping.get('phy_zone', 0))
                
                # Check if mapping already exists
                from .models import SpeciesHDModelMap
                existing_mapping = SpeciesHDModelMap.objects.filter(
                    species_id=species_code,
                    physiography_id=phy_zone
                ).first()
                
                if existing_mapping:
                    # Update existing mapping
                    existing_mapping.hd_model_id = hd_model_code
                    existing_mapping.hd_a = hd_a
                    existing_mapping.hd_b = hd_b
                    existing_mapping.hd_c = hd_c
                    existing_mapping.save()
                    
                    results.append({
                        'species_code': species_code,
                        'hd_model_code': hd_model_code,
                        'phy_zone': phy_zone,
                        'status': 'updated',
                        'message': 'Existing mapping updated successfully'
                    })
                    success_count += 1
                else:
                    # Create new mapping
                    SpeciesHDModelMap.objects.create(
                        species_id=species_code,
                        hd_model_id=hd_model_code,
                        physiography_id=phy_zone,
                        hd_a=hd_a,
                        hd_b=hd_b,
                        hd_c=hd_c
                    )
                    
                    results.append({
                        'species_code': species_code,
                        'hd_model_code': hd_model_code,
                        'phy_zone': phy_zone,
                        'status': 'created',
                        'message': 'New mapping created successfully'
                    })
                    success_count += 1
                    
            except ValueError as e:
                results.append({
                    'species_code': mapping.get('species_code', 'unknown'),
                    'hd_model_code': mapping.get('hd_model_code', 'unknown'),
                    'phy_zone': mapping.get('phy_zone', 'unknown'),
                    'status': 'error',
                    'message': f'Validation error: {str(e)}'
                })
                error_count += 1
            except Exception as e:
                results.append({
                    'species_code': mapping.get('species_code', 'unknown'),
                    'hd_model_code': mapping.get('hd_model_code', 'unknown'),
                    'phy_zone': mapping.get('phy_zone', 'unknown'),
                    'status': 'error',
                    'message': f'Database error: {str(e)}'
                })
                error_count += 1
        
        # Update tree_biometric_calc records with new hd_model_code
        if success_count > 0:
            try:
                schema_name = project.get_schema_name()
                with connections['default'].cursor() as cursor:
                    cursor.execute(SQL("SET search_path TO {}").format(Identifier(schema_name)))
                    
                    # Update hd_model_code for trees based on new mappings
                    for mapping in mappings:
                        if mapping.get('status') in ['created', 'updated']:
                            cursor.execute("""
                                UPDATE tree_biometric_calc 
                                SET hd_model_code = %s, updated_date = CURRENT_TIMESTAMP
                                WHERE species_code = %s AND phy_zone = %s AND ignore = FALSE
                            """, [mapping['hd_model_code'], mapping['species_code'], mapping['phy_zone']])
                    
                    updated_trees_count = cursor.rowcount
                    
            except Exception as e:
                # Log the error but don't fail the entire operation
                print(f"Warning: Failed to update tree_biometric_calc: {str(e)}")
                updated_trees_count = 0
        else:
            updated_trees_count = 0
        
        return JsonResponse({
            'success': True,
            'message': f'Processed {len(mappings)} mappings: {success_count} successful, {error_count} errors. Updated {updated_trees_count} tree records.',
            'results': results,
            'summary': {
                'total': len(mappings),
                'successful': success_count,
                'errors': error_count,
                'trees_updated': updated_trees_count
            }
        })
        
    except Project.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Project not found'
        }, status=404)
    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON data'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@csrf_exempt
@require_http_methods(["GET"])
def api_project_hd_model_unassigned_records(request, project_id):
    """API endpoint to get unassigned HD model records grouped by species and plot"""
    try:
        project = Project.objects.get(id=project_id)
        
        # Get query parameters
        phy_zone = request.GET.get('phy_zone')
        page = int(request.GET.get('page', 1))
        page_size = int(request.GET.get('page_size', 20))
        
        # Validate pagination parameters
        if page < 1:
            page = 1
        if page_size < 1 or page_size > 1000:
            page_size = 20
        
        # Get project schema name
        schema_name = project.get_schema_name()
        
        with connections['default'].cursor() as cursor:
            cursor.execute(SQL("SET search_path TO {}").format(Identifier(schema_name)))
            
            # Build the query to get unassigned HD model records
            query = """
                SELECT 
                    t.species_code,
                    s.species_name
                FROM tree_biometric_calc t
                LEFT JOIN public.forest_species s ON t.species_code = s.code
                WHERE t.ignore = FALSE 
                AND t.hd_model_code IS NULL
            """
            
            params = []
            
            # Add physiography zone filter if provided
            if phy_zone:
                query += " AND t.phy_zone = %s"
                params.append(phy_zone)
            
            query += """
                GROUP BY t.species_code, s.species_name
                ORDER BY t.species_code
            """
            
            # Get total count first
            count_query = f"""
                SELECT COUNT(*) FROM (
                    {query}
                ) as subquery
            """
            cursor.execute(count_query, params)
            total_records = cursor.fetchone()[0]
            
            # Add pagination
            query += " LIMIT %s OFFSET %s"
            offset = (page - 1) * page_size
            params.extend([page_size, offset])
            
            cursor.execute(query, params)
            
            records = []
            for row in cursor.fetchall():
                records.append({
                    'species_code': row[0],
                    'species_name': row[1] or 'Unknown'
                })
        
        return JsonResponse({
            'success': True,
            'records': records,
            'total_records': total_records,
            'page': page,
            'page_size': page_size,
            'total_pages': (total_records + page_size - 1) // page_size,
            'project_id': project_id,
            'project_name': project.name,
            'filters': {
                'phy_zone': phy_zone
            }
        })
        
    except Project.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Project not found'
        }, status=404)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)
		
		
@csrf_exempt
@require_http_methods(["POST"])
def api_project_height_prediction(request, project_id):
    """API endpoint to run height prediction for trees using HD models"""
    try:
        project = Project.objects.get(id=project_id)
        
        # Get project schema name
        schema_name = project.get_schema_name()
        
        with connections['default'].cursor() as cursor:
            cursor.execute(SQL("SET search_path TO {}").format(Identifier(schema_name)))
            
            # Get request data to check for phy_zone filter
            request_data = json.loads(request.body) if request.body else {}
            phy_zone_filter = request_data.get('phy_zone')
            
            # Build the query with species_hd_model_map join
            base_query = """
                SELECT 
                    t.calc_id,
                    t.plot_code,
                    t.species_code,
                    s.species_name,
                    t.dbh,
                    t.hd_model_code,
                    hd.expression,
                    hd.name as model_name,
                    t.phy_zone,
                    shm.hd_a,
                    shm.hd_b,
                    shm.hd_c
                FROM tree_biometric_calc t
                LEFT JOIN public.forest_species s ON t.species_code = s.code
                LEFT JOIN public.hd_model hd ON t.hd_model_code = hd.code
                LEFT JOIN public.species_hd_model_map shm ON (
                    t.species_code = shm.species_code 
                    AND t.phy_zone = shm.physio_code
                )
                WHERE t.ignore = FALSE 
                AND t.hd_model_code IS NOT NULL
                AND t.dbh IS NOT NULL
                AND t.dbh > 0
            """
            
            # Add phy_zone filter if specified
            if phy_zone_filter:
                base_query += " AND t.phy_zone = %s"
                cursor.execute(base_query + " ORDER BY t.plot_code, t.species_code", [phy_zone_filter])
            else:
                cursor.execute(base_query + " ORDER BY t.plot_code, t.species_code")
            
            trees_data = cursor.fetchall()
            
            if not trees_data:
                return JsonResponse({
                    'success': False,
                    'error': 'No trees found with assigned HD models for height prediction'
                }, status=400)
            

            # Prepare symbols for expression evaluation
            d = symbols('d')
            bh, a, b, c = symbols('bh a b c')
            symbol_dict = {
                'd': d,
                'exp': exp,
                'log': log,
                'sqrt': sqrt,
                'bh': bh,
                'a': a,
                'b': b,
                'c': c
            }
            
            updated_count = 0
            results = []
            errors = []
            
            for tree_data in trees_data:
                calc_id, plot_code, species_code, species_name, dbh, hd_model_code, expression, model_name, phy_zone, hd_a, hd_b, hd_c = tree_data
                
                try:
                    # Parse and evaluate the HD model expression
                    expr = parse_expr(expression, local_dict=symbol_dict)
                    
                    # Use actual parameters from species_hd_model_map, with fallbacks
                    params = {
                        'a': float(hd_a) if hd_a is not None else 1.0,
                        'b': float(hd_b) if hd_b is not None else 1.0,
                        'c': float(hd_c) if hd_c is not None else 0.0,
                        'bh': getattr(settings, 'BH', 1.3)
                    }
                    
                    # Calculate predicted height
                    predicted_height = float(expr.subs({'d': dbh, **params}).evalf())
                    
                    # Update the tree record with predicted height
                    cursor.execute("""
                        UPDATE tree_biometric_calc 
                        SET height_predicted = %s, updated_date = CURRENT_TIMESTAMP
                        WHERE calc_id = %s
                    """, [predicted_height, calc_id])
                    
                    updated_count += 1
                    
                    results.append({
                        'plot_code': plot_code,
                        'species_code': species_code,
                        'species_name': species_name or 'Unknown',
                        'dbh': dbh,
                        'height_predicted': predicted_height,
                        'model_name': model_name,
                        'phy_zone': phy_zone,
                        'hd_a': params['a'],
                        'hd_b': params['b'],
                        'hd_c': params['c']
                    })
                    
                except (SympifyError, Exception) as e:
                    errors.append({
                        'plot_code': plot_code,
                        'species_code': species_code,
                        'species_name': species_name or 'Unknown',
                        'phy_zone': phy_zone,
                        'error': str(e)
                    })
                    continue
        
        # Create appropriate message based on phy_zone filter
        if phy_zone_filter:
            message = f'Height prediction completed for phy_zone {phy_zone_filter}: {updated_count} trees updated'
        else:
            message = f'Height prediction completed for all zones: {updated_count} trees updated'
        
        return JsonResponse({
            'success': True,
            'message': message,
            'updated_count': updated_count,
            'total_trees': len(trees_data),
            'errors_count': len(errors),
            'phy_zone_filter': phy_zone_filter,
            'results': results[:100],  # Limit results for response size
            'errors': errors[:50] if errors else []  # Limit errors for response size
        })
        
    except Project.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Project not found'
        }, status=404)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)
		
		

@csrf_exempt
@require_http_methods(["GET"])
def api_project_height_prediction_status(request, project_id):
    """API endpoint to check height prediction status for a specific phy_zone"""
    try:
        project = Project.objects.get(id=project_id)
        
        # Get query parameters
        phy_zone = request.GET.get('phy_zone')
        
        if not phy_zone:
            return JsonResponse({
                'success': False,
                'error': 'phy_zone parameter is required'
            }, status=400)
        
        # Get project schema name
        schema_name = project.get_schema_name()
        
        with connections['default'].cursor() as cursor:
            cursor.execute(SQL("SET search_path TO {}").format(Identifier(schema_name)))
            
            # Get total trees count for the phy_zone
            cursor.execute("""
                SELECT COUNT(*) 
                FROM tree_biometric_calc 
                WHERE phy_zone = %s 
                AND ignore = FALSE 
                AND hd_model_code IS NOT NULL
                AND dbh IS NOT NULL
                AND dbh > 0
            """, [phy_zone])
            total_trees = cursor.fetchone()[0]
            
            # Get trees with height_predicted (not null)
            cursor.execute("""
                SELECT COUNT(*) 
                FROM tree_biometric_calc 
                WHERE phy_zone = %s 
                AND ignore = FALSE 
                AND hd_model_code IS NOT NULL
                AND dbh IS NOT NULL
                AND dbh > 0
                AND height_predicted IS NOT NULL
            """, [phy_zone])
            predicted_trees = cursor.fetchone()[0]
            
            # Get trees without height_predicted (null)
            cursor.execute("""
                SELECT COUNT(*) 
                FROM tree_biometric_calc 
                WHERE phy_zone = %s 
                AND ignore = FALSE 
                AND hd_model_code IS NOT NULL
                AND dbh IS NOT NULL
                AND dbh > 0
                AND height_predicted IS NULL
            """, [phy_zone])
            unpredicted_trees = cursor.fetchone()[0]
        
        # Determine status
        if total_trees == 0:
            status = 'no_data'
            message = 'No trees found for height prediction in this zone'
        elif unpredicted_trees == 0:
            status = 'complete'
            message = f'Height prediction completed successfully for all {total_trees} trees'
        elif predicted_trees == 0:
            status = 'not_started'
            message = f'Height prediction not yet started for {total_trees} trees'
        else:
            status = 'partial'
            message = f'Height prediction partially completed: {predicted_trees} of {total_trees} trees predicted'
        
        return JsonResponse({
            'success': True,
            'status': status,
            'message': message,
            'total_trees': total_trees,
            'predicted_trees': predicted_trees,
            'unpredicted_trees': unpredicted_trees,
            'phy_zone': phy_zone,
            'project_id': project_id,
            'project_name': project.name
        })
        
    except Project.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Project not found'
        }, status=404)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@csrf_exempt
@require_http_methods(["GET"])
def api_project_hd_relation_data(request, project_id):
    """API endpoint to get H-D relation data for a specific phy_zone or plot_code"""
    try:
        project = Project.objects.get(id=project_id)
        
        # Get query parameters
        plot_code = request.GET.get('plot_code')
        phy_zone = request.GET.get('phy_zone')
        
        if not plot_code and not phy_zone:
            return JsonResponse({
                'success': False,
                'error': 'Either plot_code or phy_zone parameter is required'
            }, status=400)
        
        # Get project schema name
        schema_name = project.get_schema_name()
        
        with connections['default'].cursor() as cursor:
            cursor.execute(SQL("SET search_path TO {}").format(Identifier(schema_name)))
            
            # Build query based on available parameters
            if plot_code:
                # Get H-D relation data for the specified plot code
                cursor.execute("""
                    SELECT 
                        t.plot_code,
                        t.species_code,
                        s.species_name,
                        t.dbh,
                        t.height_predicted,
                        t.phy_zone,
                        hd.name as model_name
                    FROM tree_biometric_calc t
                    LEFT JOIN public.forest_species s ON t.species_code = s.code
                    LEFT JOIN public.hd_model hd ON t.hd_model_code = hd.code
                    WHERE t.plot_code = %s 
                    AND t.ignore = FALSE 
                    AND t.height_predicted IS NOT NULL
                    AND t.dbh IS NOT NULL
                    AND t.dbh > 0
                    ORDER BY t.species_code, t.dbh
                """, [plot_code])
            else:
                # Get H-D relation data for the specified phy_zone
                cursor.execute("""
                    SELECT 
                        t.plot_code,
                        t.species_code,
                        s.species_name,
                        t.dbh,
                        t.height_predicted,
                        t.phy_zone,
                        hd.name as model_name
                    FROM tree_biometric_calc t
                    LEFT JOIN public.forest_species s ON t.species_code = s.code
                    LEFT JOIN public.hd_model hd ON t.hd_model_code = hd.code
                    WHERE t.phy_zone = %s 
                    AND t.ignore = FALSE 
                    AND t.height_predicted IS NOT NULL
                    AND t.dbh IS NOT NULL
                    AND t.dbh > 0
                    ORDER BY t.species_code, t.dbh
                """, [phy_zone])
            
            chart_data = []
            for row in cursor.fetchall():
                chart_data.append({
                    'plot_code': row[0],
                    'species_code': row[1],
                    'species_name': row[2] or 'Unknown',
                    'dbh': float(row[3]),
                    'height_predicted': float(row[4]),
                    'phy_zone': row[5],
                    'model_name': row[6] or 'Unknown Model'
                })
        
        # Determine response parameters
        if plot_code:
            response_data = {
                'success': True,
                'chart_data': chart_data,
                'plot_code': plot_code,
                'total_trees': len(chart_data),
                'project_id': project_id,
                'project_name': project.name
            }
        else:
            response_data = {
                'success': True,
                'chart_data': chart_data,
                'phy_zone': phy_zone,
                'total_trees': len(chart_data),
                'project_id': project_id,
                'project_name': project.name
            }
        
        return JsonResponse(response_data)
        
    except Project.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Project not found'
        }, status=404)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)
