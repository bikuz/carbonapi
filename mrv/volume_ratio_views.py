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

# Import volume ratio calculation functions
from .vol_ratio_utils import v_ratio_broken_top_trees, a_par, b_par

@csrf_exempt
@require_http_methods(["POST"])
def api_project_volume_ratio_calculation(request, project_id):
    """API endpoint to run volume ratio calculation for broken trees"""
    try:
        project = Project.objects.get(id=project_id)
        data = json.loads(request.body) if request.body else {}
        
        # Get phy_zone filter if provided
        phy_zone_filter = data.get('phy_zone')
        
        # Get project schema name
        schema_name = project.get_schema_name()
        
        with connections['default'].cursor() as cursor:
            cursor.execute(SQL("SET search_path TO {}").format(Identifier(schema_name)))
            
            # Build query to get trees that need volume ratio calculation
            base_query = """
                SELECT 
                    t.calc_id,
                    t.plot_code,
                    t.species_code,
                    s.species_name,
                    t.dbh,
                    t.height_predicted,
                    t.height,
                    t.crown_class,
                    t.phy_zone,
                    p.name as physiography_name
                FROM tree_biometric_calc t
                LEFT JOIN public.forest_species s ON t.species_code = s.code
                LEFT JOIN public.physiography p ON t.phy_zone = p.code
                WHERE t.ignore = FALSE 
                AND t.dbh IS NOT NULL
                AND t.dbh > 0
                AND t.height_predicted IS NOT NULL
                AND t.height_predicted > 0
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
                    'error': 'No trees found for volume ratio calculation'
                }, status=400)
            
           
            
            updated_count = 0
            broken_trees_count = 0
            case1_count = 0
            case2_count = 0
            case3_count = 0
            non_broken_count = 0
            results = []
            errors = []
            
            for tree_data in trees_data:
                calc_id, plot_code, species_code, species_name, dbh, height_predicted, height_measured, crown_class, phy_zone, physiography_name = tree_data
                
                try:
                    # Calculate volume ratio using the corrected logic
                    volume_ratio = v_ratio_broken_top_trees(
                        d13=dbh,
                        ht=height_predicted,
                        ht_x=height_measured,  # Pass the actual measured height, let the function handle the cases
                        crown_class=crown_class,
                        a_par=a_par,
                        b_par=b_par
                    )
                    
                    # Update the tree record with volume ratio
                    cursor.execute("""
                        UPDATE tree_biometric_calc 
                        SET volume_ratio = %s, updated_date = CURRENT_TIMESTAMP
                        WHERE calc_id = %s
                    """, [volume_ratio, calc_id])
                    
                    updated_count += 1
                    
                    # Categorize results based on the three cases
                    case_type = None
                    if crown_class == 6:  # Broken trees
                        broken_trees_count += 1
                        if height_measured is None or height_measured <= 0:
                            # Case 3: No measured height
                            case3_count += 1
                            case_type = 'Case 3: No measured height'
                        elif height_measured < height_predicted:
                            # Case 1: measured height < predicted height
                            case1_count += 1
                            case_type = 'Case 1: Normal broken tree'
                        else:
                            # Case 2: measured height >= predicted height
                            case2_count += 1
                            case_type = 'Case 2: Unusual case (measured >= predicted)'
                    else:
                        non_broken_count += 1
                        case_type = 'Non-broken tree'
                    
                    results.append({
                        'plot_code': plot_code,
                        'species_code': species_code,
                        'species_name': species_name or 'Unknown',
                        'dbh': dbh,
                        'height_predicted': height_predicted,
                        'height_measured': height_measured,
                        'crown_class': crown_class,
                        'volume_ratio': volume_ratio,
                        'phy_zone': phy_zone,
                        'case_type': case_type
                    })
                    
                except Exception as e:
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
            message = f'Volume ratio calculation completed for phy_zone {phy_zone_filter}: {updated_count} trees updated'
        else:
            message = f'Volume ratio calculation completed for all zones: {updated_count} trees updated'
        
        return JsonResponse({
            'success': True,
            'message': message,
            'updated_count': updated_count,
            'total_trees': len(trees_data),
            'broken_trees': broken_trees_count,
            'processed_trees': broken_trees_count,
            'case1_count': case1_count,
            'case2_count': case2_count,
            'case3_count': case3_count,
            'non_broken_count': non_broken_count,
            'errors_count': len(errors),
            'phy_zone_filter': phy_zone_filter,
            'physiography_name': physiography_name if phy_zone_filter else None,
            'case_summary': {
                'case1_description': 'Normal broken trees (measured height < predicted height)',
                'case2_description': 'Unusual cases (measured height >= predicted height)',
                'case3_description': 'No measured height (assumes 90% break point)',
                'non_broken_description': 'Non-broken trees (volume ratio = 1)'
            },
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
def api_project_volume_ratio_status(request, project_id):
    """API endpoint to check volume ratio calculation status"""
    try:
        project = Project.objects.get(id=project_id)
        
        # Get phy_zone filter if provided
        phy_zone_filter = request.GET.get('phy_zone')
        
        # Get project schema name
        schema_name = project.get_schema_name()
        
        with connections['default'].cursor() as cursor:
            cursor.execute(SQL("SET search_path TO {}").format(Identifier(schema_name)))
            
            # Build base WHERE clause
            base_where = """
                WHERE ignore = FALSE 
                AND dbh IS NOT NULL
                AND dbh > 0
                AND height_predicted IS NOT NULL
                AND height_predicted > 0
            """
            
            # Add phy_zone filter if specified
            if phy_zone_filter:
                base_where += f" AND phy_zone = {phy_zone_filter}"
            
            # Get total trees count
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM tree_biometric_calc 
                {base_where}
            """)
            total_trees = cursor.fetchone()[0]
            
            # Get trees with volume_ratio calculated (not null and > 0)
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM tree_biometric_calc 
                {base_where}
                AND volume_ratio IS NOT NULL
                AND volume_ratio > 0
            """)
            calculated_trees = cursor.fetchone()[0]
            
            # Get trees without volume_ratio
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM tree_biometric_calc 
                {base_where}
                AND (volume_ratio IS NULL OR volume_ratio <= 0)
            """)
            uncalculated_trees = cursor.fetchone()[0]
            
            # Get broken trees count if phy_zone is specified
            broken_trees = 0
            if phy_zone_filter:
                cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM tree_biometric_calc 
                    {base_where}
                    AND crown_class = 6
                """)
                broken_trees = cursor.fetchone()[0]
        
        # Determine status
        if total_trees == 0:
            status = 'no_data'
            message = 'No trees found for volume ratio calculation'
            all_completed = False
        elif uncalculated_trees == 0:
            status = 'complete'
            if phy_zone_filter:
                message = f'Volume ratio calculation completed successfully for zone {phy_zone_filter}: {total_trees} trees'
            else:
                message = f'Volume ratio calculation completed successfully for all {total_trees} trees'
            all_completed = True
        elif calculated_trees == 0:
            status = 'not_started'
            if phy_zone_filter:
                message = f'Volume ratio calculation not yet started for zone {phy_zone_filter}: {total_trees} trees'
            else:
                message = f'Volume ratio calculation not yet started for {total_trees} trees'
            all_completed = False
        else:
            status = 'partial'
            if phy_zone_filter:
                message = f'Volume ratio calculation partially completed for zone {phy_zone_filter}: {calculated_trees} of {total_trees} trees calculated'
            else:
                message = f'Volume ratio calculation partially completed: {calculated_trees} of {total_trees} trees calculated'
            all_completed = False
        
        response_data = {
            'success': True,
            'status': status,
            'message': message,
            'all_completed': all_completed,
            'total_trees': total_trees,
            'calculated_trees': calculated_trees,
            'uncalculated_trees': uncalculated_trees,
            'project_id': project_id,
            'project_name': project.name
        }
        
        # Add phy_zone specific data if filter is provided
        if phy_zone_filter:
            response_data['phy_zone'] = int(phy_zone_filter)
            response_data['broken_trees'] = broken_trees
        
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
