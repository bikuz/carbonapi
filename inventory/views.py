from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from django.core.files.storage import default_storage
from django.conf import settings
import os
import uuid
# from .models import SchemaImport

from .utils import (
    extract_zip_file, 
    analyze_sql_file, 
    schema_exists,
    execute_sql_script,

    drop_schema_if_exists,
    ensure_schema_import_table_exists,
    create_schema_import_record,
    get_schema_import_record,
    update_schema_import_record,
    schema_exists_and_has_tables,
    compare_schema_tables,
    merge_schemas,
    merge_multiple_schemas,
    merge_multiple_schemas_optimized,
    merge_schemas_incremental,
    get_table_dependencies,
    get_table_creation_order,
    get_table_structure,
    get_all_schemas,
    get_schemas_with_info,
    get_imported_schemas_with_info,
    get_all_available_schemas_with_info,
    cleanup_temp_directory,
    cleanup_old_temp_directories,
    cleanup_failed_imports,
    delete_schema_completely
)

# @csrf_exempt
# @require_POST
# def upload_sql_zip(request):
#     """Handle zip file upload and initial analysis"""
#     if not request.FILES.get('zip_file'):
#         return JsonResponse({'error': 'No file uploaded'}, status=400)
    
#     # Ensure the tracking table exists
#     ensure_schema_import_table_exists()

#     # Save uploaded file
#     zip_file = request.FILES['zip_file']
#     import_id = uuid.uuid4()
#     temp_dir = os.path.join(settings.MEDIA_ROOT, 'temp_sql_imports', str(import_id))
#     os.makedirs(temp_dir, exist_ok=True)
#     zip_path = os.path.join(temp_dir, zip_file.name)
    
#     with open(zip_path, 'wb+') as destination:
#         for chunk in zip_file.chunks():
#             destination.write(chunk)
    
#     # Extract zip file
#     try:
#         sql_files = extract_zip_file(zip_path, temp_dir)
#         if not sql_files:
#             return JsonResponse({'error': 'No SQL files found in zip'}, status=400)
        
#         # Analyze first SQL file (assuming one SQL file per zip)
#         sql_path = os.path.join(temp_dir, sql_files[0])
#         analysis = analyze_sql_file(sql_path)
        
#         # Create import record in NFI_tables
#         relative_path = os.path.join('temp_sql_imports', str(import_id), zip_file.name)
#         import_id = create_schema_import_record(
#             uploaded_file=relative_path,
#             schema_name=analysis['schema_name']
#         )
        
#         response_data = {
#             'import_id': import_id,
#             'has_schema_creation': analysis['has_schema_creation'],
#             'schema_name': analysis['schema_name'],
#             'schema_exists': schema_exists(analysis['schema_name']) if analysis['schema_name'] else False,
#             'message': 'File uploaded and analyzed successfully',
#             'database': 'NFI_tables'  # Inform frontend which database is being used
#         }
        
#         return JsonResponse(response_data)
    
#     except Exception as e:
#         return JsonResponse({'error': str(e)}, status=500)

# @csrf_exempt
# @require_POST
# def confirm_import(request):
#     """Handle user confirmation and execute import in NFI database"""
#     import_id = request.POST.get('import_id')
#     schema_name = request.POST.get('schema_name')
#     confirm_replace = request.POST.get('confirm_replace', 'false').lower() == 'true'
    
#     try:
#         # Get record from NFI_tables
#         import_record = get_schema_import_record(import_id)
#         if not import_record:
#             return JsonResponse({'error': 'Import record not found'}, status=404)
        
#         # Validate schema name
#         if not schema_name:
#             return JsonResponse({'error': 'Schema name is required'}, status=400)
        
#         # Check if schema exists in NFI database and replacement not confirmed
#         if schema_exists(schema_name) and not confirm_replace:
#             return JsonResponse({
#                 'error': f'Schema "{schema_name}" already exists in NFI_tables database',
#                 'schema_exists': True,
#                 'schema_name': schema_name,
#                 'database': 'NFI_tables'
#             }, status=400)
        
#         # Update import record in NFI_tables
#         update_schema_import_record(
#             import_id,
#             schema_name=schema_name,
#             status='processing'
#         )
        
#         # Prepare paths
#         temp_dir = os.path.join(settings.MEDIA_ROOT, os.path.dirname(import_record['uploaded_file']))
#         sql_files = [f for f in os.listdir(temp_dir) if f.endswith('.sql')]
        
#         if not sql_files:
#             import_record.status = 'failed'
#             import_record.message = 'No SQL files found'
#             import_record.save()
#             return JsonResponse({'error': 'No SQL files found'}, status=400)
        
#         # Process each SQL file in NFI database
#         for sql_file in sql_files:
#             sql_path = os.path.join(temp_dir, sql_file)
#             with open(sql_path, 'r', encoding='utf-8') as f:
#                 sql_content = f.read()
            
#             success, error = execute_sql_script(sql_content, schema_name)
#             if not success:
#                 import_record.status = 'failed'
#                 import_record.message = error
#                 import_record.save()
#                 return JsonResponse({
#                     'error': error,
#                     'database': 'NFI_tables'
#                 }, status=500)
        
#         # Mark as completed in default database
#         import_record.status = 'completed'
#         import_record.save()
        
#         return JsonResponse({
#             'success': True,
#             'message': f'SQL import completed successfully in NFI_tables database',
#             'schema_name': schema_name,
#             'database': 'NFI_tables'
#         })
    
#     except SchemaImport.DoesNotExist:
#         return JsonResponse({'error': 'Import record not found'}, status=404)
#     except Exception as e:
#         return JsonResponse({'error': str(e)}, status=500)
    




@csrf_exempt
@require_POST
def upload_sql_zip(request):
    """Handle zip file upload and initial analysis"""
    if not request.FILES.get('zip_file'):
        return JsonResponse({'error': 'No file uploaded'}, status=400)
    
    # Ensure the tracking table exists
    ensure_schema_import_table_exists()
    
    # Save uploaded file
    zip_file = request.FILES['zip_file']
    temp_import_id = uuid.uuid4()  # Store the original UUID for temp directory
    temp_dir = os.path.join(settings.MEDIA_ROOT, 'temp_sql_imports', str(temp_import_id))
    os.makedirs(temp_dir, exist_ok=True)
    zip_path = os.path.join(temp_dir, zip_file.name)
    
    with open(zip_path, 'wb+') as destination:
        for chunk in zip_file.chunks():
            destination.write(chunk)
    
    # Extract zip file
    try:
        sql_files = extract_zip_file(zip_path, temp_dir)
        if not sql_files:
            return JsonResponse({'error': 'No SQL files found in zip'}, status=400)
        
        # Analyze first SQL file (assuming one SQL file per zip)
        sql_path = os.path.join(temp_dir, sql_files[0])
        analysis = analyze_sql_file(sql_path)
        
        # Create import record in NFI_tables
        relative_path = os.path.join('temp_sql_imports', str(temp_import_id), zip_file.name)
        import_id = create_schema_import_record(
            uploaded_file=relative_path,
            schema_name=analysis['schema_name']
        )
        
        # print(analysis['schema_name'])
        isSchemaExist= schema_exists(analysis['schema_name']) if analysis['schema_name'] else False
        # print(isSchemaExist)

        response_data = {
            'import_id': import_id,
            'has_schema_creation': analysis['has_schema_creation'],
            'schema_name': analysis['schema_name'],
            'schema_exists':isSchemaExist,
            'message': 'File uploaded and analyzed successfully',
            'database': 'NFI_tables'
        }
        
        return JsonResponse(response_data)
    
    except Exception as e:
        # Clean up temp directory on error
        cleanup_temp_directory(temp_import_id)
        return JsonResponse({'error': str(e)}, status=500)


@csrf_exempt
@require_POST
def confirm_import(request):
    """Handle user confirmation and execute import with optional schema replacement"""
    import_id = request.POST.get('import_id')
    schema_name = request.POST.get('schema_name')
    confirm_replace = request.POST.get('confirm_replace', 'false').lower() == 'true'
    
    try:
        # Get record from NFI_tables
        import_record = get_schema_import_record(import_id)
        if not import_record:
            return JsonResponse({'error': 'Import record not found'}, status=404)
        
        # Validate schema name
        if not schema_name:
            return JsonResponse({'error': 'Schema name is required'}, status=400)
        
        # Clean schema name (remove quotes if present)
        clean_schema_name = schema_name.strip('"')
        
        # Check if schema exists in NFI database
        schema_already_exists = schema_exists(clean_schema_name)
        
        # If schema exists and user didn't confirm replacement
        if schema_already_exists and not confirm_replace:
            return JsonResponse({
                'error': f'Schema "{clean_schema_name}" already exists in NFI_tables',
                'schema_exists': True,
                'schema_name': clean_schema_name,
                'database': 'NFI_tables'
            }, status=400)
        
        # Update import record in NFI_tables
        update_schema_import_record(
            import_id,
            schema_name=clean_schema_name,
            status='processing'
        )
        
        # If schema exists and user confirmed replacement, drop it first
        if schema_already_exists and confirm_replace:
            success, message = drop_schema_if_exists(clean_schema_name)
            if not success:
                update_schema_import_record(
                    import_id,
                    status='failed',
                    message=message
                )
                return JsonResponse({
                    'error': message,
                    'database': 'NFI_tables'
                }, status=500)
            print(message)  # Log success
        
        # Rest of the function remains the same...
        # Prepare paths
        temp_dir = os.path.join(settings.MEDIA_ROOT, os.path.dirname(import_record['uploaded_file']))
        
        # Extract temp_import_id from the uploaded_file path for cleanup
        temp_import_id = os.path.basename(os.path.dirname(import_record['uploaded_file']))
        
        sql_files = [f for f in os.listdir(temp_dir) if f.endswith('.sql')]
        
        if not sql_files:
            update_schema_import_record(
                import_id,
                status='failed',
                message='No SQL files found'
            )
            # Clean up temp directory on error
            cleanup_temp_directory(temp_import_id)
            return JsonResponse({'error': 'No SQL files found'}, status=400)
        
        # Process each SQL file in NFI database
        for sql_file in sql_files:
            sql_path = os.path.join(temp_dir, sql_file)
            with open(sql_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            success, error = execute_sql_script(sql_content, clean_schema_name)
            if not success:
                update_schema_import_record(
                    import_id,
                    status='failed',
                    message=error
                )
                # Clean up temp directory on error
                cleanup_temp_directory(temp_import_id)
                return JsonResponse({
                    'error': error,
                    'database': 'NFI_tables'
                }, status=500)
        
        # Mark as completed in NFI_tables
        update_schema_import_record(
            import_id,
            status='completed',
            completed_at='NOW()'
        )
        
        # Clean up temp directory on successful import
        cleanup_temp_directory(temp_import_id)
        
        return JsonResponse({
            'success': True,
            'message': f'Schema "{clean_schema_name}" imported successfully',
            'schema_name': clean_schema_name,
            'database': 'NFI_tables',
            'replaced_existing': schema_already_exists
        })
    
    except Exception as e:
        error_msg = str(e)
        update_schema_import_record(
            import_id,
            status='failed',
            message=error_msg
        )
        # Clean up temp directory on error
        cleanup_temp_directory(temp_import_id)
        return JsonResponse({'error': error_msg}, status=500)
    

@csrf_exempt
@require_POST
def merge_schemas_api(request):
    """
    API endpoint to merge two schemas into a target schema
    Expected POST data:
    - source_schema1: First source schema name
    - source_schema2: Second source schema name  
    - target_schema: Target schema name
    - create_new_schema: Boolean (optional, default True)
    """
    try:
        # Get parameters from request
        source_schema1 = request.POST.get('source_schema1')
        source_schema2 = request.POST.get('source_schema2')
        target_schema = request.POST.get('target_schema')
        create_new_schema = request.POST.get('create_new_schema', 'true').lower() == 'true'
        
        # Validate required parameters
        if not all([source_schema1, source_schema2, target_schema]):
            return JsonResponse({
                'error': 'Missing required parameters: source_schema1, source_schema2, target_schema'
            }, status=400)
        
        # Clean schema names
        source_schema1 = source_schema1.strip().strip('"')
        source_schema2 = source_schema2.strip().strip('"')
        target_schema = target_schema.strip().strip('"')
        
        # Check if source schemas exist
        schema1_exists, schema1_table_count, schema1_tables = schema_exists_and_has_tables(source_schema1)
        schema2_exists, schema2_table_count, schema2_tables = schema_exists_and_has_tables(source_schema2)
        
        if not schema1_exists:
            return JsonResponse({
                'error': f'Source schema "{source_schema1}" does not exist'
            }, status=404)
        
        if not schema2_exists:
            return JsonResponse({
                'error': f'Source schema "{source_schema2}" does not exist'
            }, status=404)
        
        # Compare table structures between schemas
        are_equal, schema1_table_list, schema2_table_list, differences = compare_schema_tables(source_schema1, source_schema2)
        
        if not are_equal:
            return JsonResponse({
                'error': 'Schemas have different table structures and cannot be merged',
                'details': {
                    'schema1_tables': schema1_table_list,
                    'schema2_tables': schema2_table_list,
                    'differences': differences
                }
            }, status=400)
        
        # Check if target schema exists (if not creating new)
        if not create_new_schema:
            target_exists, target_table_count, target_tables = schema_exists_and_has_tables(target_schema)
            if not target_exists:
                return JsonResponse({
                    'error': f'Target schema "{target_schema}" does not exist and create_new_schema is False'
                }, status=404)
        
        # Perform the merge
        success, message, details = merge_schemas(
            source_schema1, 
            source_schema2, 
            target_schema, 
            create_new_schema
        )
        
        if success:
            return JsonResponse({
                'success': True,
                'message': message,
                'details': details,
                'source_schema1': source_schema1,
                'source_schema2': source_schema2,
                'target_schema': target_schema,
                'create_new_schema': create_new_schema
            })
        else:
            return JsonResponse({
                'error': message
            }, status=500)
            
    except Exception as e:
        return JsonResponse({
            'error': f'Unexpected error: {str(e)}'
        }, status=500)

@csrf_exempt
@require_POST
def merge_multiple_schemas_api(request):
    """
    API endpoint to merge multiple schemas into a target schema
    Expected POST data:
    - source_schemas: Comma-separated list of source schema names
    - target_schema: Target schema name
    - create_new_schema: Boolean (optional, default True)
    - merge_strategy: 'union', 'priority', or 'intersection' (optional, default 'union')
    """
    try:
        # Get parameters from request
        source_schemas_str = request.POST.get('source_schemas')
        target_schema = request.POST.get('target_schema')
        create_new_schema = request.POST.get('create_new_schema', 'true').lower() == 'true'
        merge_strategy = request.POST.get('merge_strategy', 'union')
        
        # Validate required parameters
        if not source_schemas_str or not target_schema:
            return JsonResponse({
                'error': 'Missing required parameters: source_schemas, target_schema'
            }, status=400)
        
        # Parse source schemas from comma-separated string
        source_schemas = [schema.strip().strip('"') for schema in source_schemas_str.split(',') if schema.strip()]
        
        if len(source_schemas) < 2:
            return JsonResponse({
                'error': 'At least 2 source schemas must be provided'
            }, status=400)
        
        # Clean target schema name
        target_schema = target_schema.strip().strip('"')
        
        # Validate merge strategy
        valid_strategies = ['union', 'priority', 'intersection']
        if merge_strategy not in valid_strategies:
            return JsonResponse({
                'error': f'Invalid merge_strategy. Must be one of: {", ".join(valid_strategies)}'
            }, status=400)
        
        # Check if all source schemas exist
        schema_validation = {}
        for schema in source_schemas:
            exists, table_count, tables = schema_exists_and_has_tables(schema)
            schema_validation[schema] = {
                'exists': exists,
                'table_count': table_count,
                'tables': tables
            }
            
            if not exists:
                return JsonResponse({
                    'error': f'Source schema "{schema}" does not exist'
                }, status=404)
        
        # Check if target schema exists (if not creating new)
        if not create_new_schema:
            target_exists, target_table_count, target_tables = schema_exists_and_has_tables(target_schema)
            if not target_exists:
                return JsonResponse({
                    'error': f'Target schema "{target_schema}" does not exist and create_new_schema is False'
                }, status=404)
        
        # Perform the merge
        success, message, details = merge_multiple_schemas(
            source_schemas=source_schemas,
            target_schema=target_schema,
            create_new_schema=create_new_schema,
            merge_strategy=merge_strategy
        )
        
        if success:
            return JsonResponse({
                'success': True,
                'message': message,
                'details': details,
                'source_schemas': source_schemas,
                'target_schema': target_schema,
                'create_new_schema': create_new_schema,
                'merge_strategy': merge_strategy,
                'schema_validation': schema_validation
            })
        else:
            return JsonResponse({
                'error': message
            }, status=500)
            
    except Exception as e:
        return JsonResponse({
            'error': f'Unexpected error: {str(e)}'
        }, status=500)

@csrf_exempt
@require_POST
def merge_schemas_incremental_api(request):
    """
    API endpoint to merge multiple schemas incrementally with batching for large tables
    Expected POST data:
    - source_schemas: Comma-separated list of source schema names
    - target_schema: Target schema name
    - create_new_schema: Boolean (optional, default True)
    - batch_size: Integer (optional, default 1000)
    """
    try:
        # Get parameters from request
        source_schemas_str = request.POST.get('source_schemas')
        target_schema = request.POST.get('target_schema')
        create_new_schema = request.POST.get('create_new_schema', 'true').lower() == 'true'
        batch_size = request.POST.get('batch_size', '1000')
        
        # Validate required parameters
        if not source_schemas_str or not target_schema:
            return JsonResponse({
                'error': 'Missing required parameters: source_schemas, target_schema'
            }, status=400)
        
        # Parse source schemas from comma-separated string
        source_schemas = [schema.strip().strip('"') for schema in source_schemas_str.split(',') if schema.strip()]
        
        if len(source_schemas) < 2:
            return JsonResponse({
                'error': 'At least 2 source schemas must be provided'
            }, status=400)
        
        # Clean target schema name
        target_schema = target_schema.strip().strip('"')
        
        # Validate and parse batch_size
        try:
            batch_size = int(batch_size)
            if batch_size <= 0:
                return JsonResponse({
                    'error': 'batch_size must be a positive integer'
                }, status=400)
        except ValueError:
            return JsonResponse({
                'error': 'batch_size must be a valid integer'
            }, status=400)
        
        # Check if all source schemas exist
        schema_validation = {}
        for schema in source_schemas:
            exists, table_count, tables = schema_exists_and_has_tables(schema)
            schema_validation[schema] = {
                'exists': exists,
                'table_count': table_count,
                'tables': tables
            }
            
            if not exists:
                return JsonResponse({
                    'error': f'Source schema "{schema}" does not exist'
                }, status=404)
        
        # Check if target schema exists (if not creating new)
        if not create_new_schema:
            target_exists, target_table_count, target_tables = schema_exists_and_has_tables(target_schema)
            if not target_exists:
                return JsonResponse({
                    'error': f'Target schema "{target_schema}" does not exist and create_new_schema is False'
                }, status=404)
        
        # Perform the incremental merge
        success, message, details = merge_schemas_incremental(
            source_schemas=source_schemas,
            target_schema=target_schema,
            create_new_schema=create_new_schema,
            batch_size=batch_size
        )
        
        if success:
            return JsonResponse({
                'success': True,
                'message': message,
                'details': details,
                'source_schemas': source_schemas,
                'target_schema': target_schema,
                'create_new_schema': create_new_schema,
                'batch_size': batch_size,
                'schema_validation': schema_validation
            })
        else:
            return JsonResponse({
                'error': message
            }, status=500)
            
    except Exception as e:
        return JsonResponse({
            'error': f'Unexpected error: {str(e)}'
        }, status=500)

@csrf_exempt
def get_schema_info_api(request):
    """
    API endpoint to get information about schemas
    Expected GET parameters:
    - schema_names: Comma-separated list of schema names (optional, if not provided returns all schemas)
    """
    try:
        schema_names_str = request.GET.get('schema_names', '')
        
        if schema_names_str:
            # Get specific schemas
            schema_names = [name.strip().strip('"') for name in schema_names_str.split(',') if name.strip()]
            schema_info = {}
            
            for schema_name in schema_names:
                exists, table_count, tables = schema_exists_and_has_tables(schema_name)
                schema_info[schema_name] = {
                    'exists': exists,
                    'table_count': table_count,
                    'tables': tables
                }
        else:
            # Get all schemas (this would require a new utility function)
            # For now, return error suggesting to provide specific schema names
            return JsonResponse({
                'error': 'Please provide schema_names parameter to get specific schema information'
            }, status=400)
        
        return JsonResponse({
            'success': True,
            'schema_info': schema_info
        })
        
    except Exception as e:
        return JsonResponse({
            'error': f'Unexpected error: {str(e)}'
        }, status=500)

@csrf_exempt
def compare_schemas_api(request):
    """
    API endpoint to compare multiple schemas
    Expected GET parameters:
    - schema_names: Comma-separated list of schema names (at least 2 required)
    """
    try:
        schema_names_str = request.GET.get('schema_names', '')
        
        if not schema_names_str:
            return JsonResponse({
                'error': 'schema_names parameter is required'
            }, status=400)
        
        schema_names = [name.strip().strip('"') for name in schema_names_str.split(',') if name.strip()]
        
        if len(schema_names) < 2:
            return JsonResponse({
                'error': 'At least 2 schema names must be provided for comparison'
            }, status=400)
        
        # Validate all schemas exist
        for schema_name in schema_names:
            exists, _, _ = schema_exists_and_has_tables(schema_name)
            if not exists:
                return JsonResponse({
                    'error': f'Schema "{schema_name}" does not exist'
                }, status=404)
        
        # Compare schemas pairwise
        comparisons = {}
        for i in range(len(schema_names)):
            for j in range(i + 1, len(schema_names)):
                schema1 = schema_names[i]
                schema2 = schema_names[j]
                
                are_equal, schema1_tables, schema2_tables, differences = compare_schema_tables(schema1, schema2)
                
                comparisons[f'{schema1}_vs_{schema2}'] = {
                    'are_equal': are_equal,
                    'schema1_tables': schema1_tables,
                    'schema2_tables': schema2_tables,
                    'differences': differences
                }
        
        return JsonResponse({
            'success': True,
            'schema_names': schema_names,
            'comparisons': comparisons
        })
        
    except Exception as e:
        return JsonResponse({
            'error': f'Unexpected error: {str(e)}'
        }, status=500)

@csrf_exempt
def list_schemas_api(request):
    """
    API endpoint to list all schemas in the database
    Expected GET parameters:
    - detailed: Boolean (optional, default False) - if True, returns detailed info including table counts and sizes
    - include_empty: Boolean (optional, default True) - if False, excludes schemas with no tables
    - imported_only: Boolean (optional, default False) - if True, returns only imported schemas from schema_import table
    """
    try:
        # Get parameters from request
        detailed = request.GET.get('detailed', 'false').lower() == 'true'
        include_empty = request.GET.get('include_empty', 'true').lower() == 'true'
        imported_only = request.GET.get('imported_only', 'false').lower() == 'true'
        
        if imported_only:
            # Get both imported and merged schemas
            if detailed:
                schemas = get_all_available_schemas_with_info()
                
                # Filter out empty schemas if requested
                if not include_empty:
                    schemas = [schema for schema in schemas if schema['table_count'] > 0]
                
                return JsonResponse({
                    'success': True,
                    'schemas': schemas,
                    'total_schemas': len(schemas),
                    'detailed': True,
                    'imported_only': True
                })
            else:
                # Get simple list of schema names
                schemas = get_all_available_schemas_with_info()
                schema_names = [schema['schema_name'] for schema in schemas]
                
                # Filter out empty schemas if requested
                if not include_empty:
                    schema_names = [schema['schema_name'] for schema in schemas if schema['table_count'] > 0]
                
                return JsonResponse({
                    'success': True,
                    'schemas': schema_names,
                    'total_schemas': len(schema_names),
                    'detailed': False,
                    'imported_only': True
                })
        else:
            # Get all schemas (original functionality)
            if detailed:
                # Get detailed schema information
                schemas = get_schemas_with_info()
                
                # Filter out empty schemas if requested
                if not include_empty:
                    schemas = [schema for schema in schemas if schema['table_count'] > 0]
                
                return JsonResponse({
                    'success': True,
                    'schemas': schemas,
                    'total_schemas': len(schemas),
                    'detailed': True
                })
            else:
                # Get simple schema list
                schemas = get_all_schemas()
                
                # If not including empty schemas, we need to check each one
                if not include_empty:
                    filtered_schemas = []
                    for schema_name in schemas:
                        exists, table_count, _ = schema_exists_and_has_tables(schema_name)
                        if exists and table_count > 0:
                            filtered_schemas.append(schema_name)
                    schemas = filtered_schemas
                
                return JsonResponse({
                    'success': True,
                    'schemas': schemas,
                    'total_schemas': len(schemas),
                    'detailed': False
                })
        
    except Exception as e:
        return JsonResponse({
            'error': f'Unexpected error: {str(e)}'
        }, status=500)

@csrf_exempt
def cleanup_temp_api(request):
    """
    API endpoint to clean up temporary directory for a specific import ID
    Expected POST parameters:
    - import_id: The import ID to clean up
    """
    try:
        import_id = request.POST.get('import_id')
        
        if not import_id:
            return JsonResponse({
                'error': 'import_id parameter is required'
            }, status=400)
        
        success, message = cleanup_temp_directory(import_id)
        
        if success:
            return JsonResponse({
                'success': True,
                'message': message
            })
        else:
            return JsonResponse({
                'error': message
            }, status=500)
    
    except Exception as e:
        return JsonResponse({
            'error': f'Unexpected error: {str(e)}'
        }, status=500)


@csrf_exempt
def cleanup_old_temp_api(request):
    """
    API endpoint to clean up temporary directories older than specified hours
    Expected POST parameters:
    - max_age_hours: Maximum age in hours (optional, default 24)
    """
    try:
        max_age_hours = request.POST.get('max_age_hours', 24)
        
        try:
            max_age_hours = int(max_age_hours)
        except ValueError:
            return JsonResponse({
                'error': 'max_age_hours must be a valid integer'
            }, status=400)
        
        success, message = cleanup_old_temp_directories(max_age_hours)
        
        if success:
            return JsonResponse({
                'success': True,
                'message': message
            })
        else:
            return JsonResponse({
                'error': message
            }, status=500)
    
    except Exception as e:
        return JsonResponse({
            'error': f'Unexpected error: {str(e)}'
        }, status=500)


@csrf_exempt
def cleanup_failed_imports_api(request):
    """
    API endpoint to clean up temporary directories for failed imports
    """
    try:
        success, message = cleanup_failed_imports()
        
        if success:
            return JsonResponse({
                'success': True,
                'message': message
            })
        else:
            return JsonResponse({
                'error': message
            }, status=500)
    
    except Exception as e:
        return JsonResponse({
            'error': f'Unexpected error: {str(e)}'
        }, status=500)

@csrf_exempt
def test_cleanup_api(request):
    """
    Test API endpoint to debug cleanup functionality
    """
    try:
        import uuid
        import tempfile
        import shutil
        
        # Create a test temp directory
        test_import_id = str(uuid.uuid4())
        test_temp_dir = os.path.join(settings.MEDIA_ROOT, 'temp_sql_imports', test_import_id)
        
        # Ensure the base directory exists
        os.makedirs(os.path.join(settings.MEDIA_ROOT, 'temp_sql_imports'), exist_ok=True)
        
        # Create test directory and files
        os.makedirs(test_temp_dir, exist_ok=True)
        test_file = os.path.join(test_temp_dir, 'test.sql')
        with open(test_file, 'w') as f:
            f.write('CREATE SCHEMA test_schema;')
        
        # Test the cleanup function
        print(f"DEBUG: Testing cleanup with import_id: {test_import_id}")
        print(f"DEBUG: Test directory created: {test_temp_dir}")
        print(f"DEBUG: Directory exists before cleanup: {os.path.exists(test_temp_dir)}")
        
        success, message = cleanup_temp_directory(test_import_id)
        
        print(f"DEBUG: Cleanup result - Success: {success}, Message: {message}")
        print(f"DEBUG: Directory exists after cleanup: {os.path.exists(test_temp_dir)}")
        
        return JsonResponse({
            'success': True,
            'test_import_id': test_import_id,
            'test_directory': test_temp_dir,
            'cleanup_success': success,
            'cleanup_message': message,
            'directory_exists_before': True,
            'directory_exists_after': os.path.exists(test_temp_dir),
            'media_root': str(settings.MEDIA_ROOT),
            'temp_base_dir': os.path.join(settings.MEDIA_ROOT, 'temp_sql_imports'),
            'base_dir_exists': os.path.exists(os.path.join(settings.MEDIA_ROOT, 'temp_sql_imports'))
        })
    
    except Exception as e:
        return JsonResponse({
            'error': f'Test cleanup failed: {str(e)}',
            'media_root': str(settings.MEDIA_ROOT) if hasattr(settings, 'MEDIA_ROOT') else 'Not set'
        }, status=500)

@csrf_exempt
@require_POST
def delete_schema_api(request):
    """
    API endpoint to delete a schema completely
    """
    try:
        schema_name = request.POST.get('schema_name')
        
        if not schema_name:
            return JsonResponse({
                'error': 'schema_name is required'
            }, status=400)
        
        # Delete the schema
        success, message = delete_schema_completely(schema_name)
        
        if success:
            return JsonResponse({
                'success': True,
                'message': message,
                'deleted_schema': schema_name
            })
        else:
            return JsonResponse({
                'error': message
            }, status=500)
    
    except Exception as e:
        return JsonResponse({
            'error': f'Unexpected error: {str(e)}'
        }, status=500)

@csrf_exempt
@require_POST
def merge_multiple_schemas_optimized_api(request):
    """
    Optimized API endpoint to merge multiple schemas with better performance
    """
    try:
        source_schemas = request.POST.getlist('source_schemas[]')
        target_schema = request.POST.get('target_schema')
        create_new_schema = request.POST.get('create_new_schema', 'true').lower() == 'true'
        merge_strategy = request.POST.get('merge_strategy', 'union')
        
        if not source_schemas:
            return JsonResponse({
                'error': 'At least 1 source schema must be provided'
            }, status=400)
        
        # If creating new schema, we need at least 2 schemas
        if create_new_schema and len(source_schemas) < 2:
            return JsonResponse({
                'error': 'At least 2 source schemas must be provided when creating a new target schema'
            }, status=400)
        
        if not target_schema:
            return JsonResponse({
                'error': 'Target schema name is required'
            }, status=400)
        
        if merge_strategy not in ['union', 'priority']:
            return JsonResponse({
                'error': 'Merge strategy must be either "union" or "priority"'
            }, status=400)
        
        # Perform the optimized merge
        success, message, details = merge_multiple_schemas_optimized(
            source_schemas=source_schemas,
            target_schema=target_schema,
            create_new_schema=create_new_schema,
            merge_strategy=merge_strategy
        )
        
        if success:
            return JsonResponse({
                'success': True,
                'message': message,
                'details': details,
                'optimization': 'column_caching_enabled'
            })
        else:
            return JsonResponse({
                'error': message
            }, status=500)
    
    except Exception as e:
        return JsonResponse({
            'error': f'Unexpected error: {str(e)}'
        }, status=500)