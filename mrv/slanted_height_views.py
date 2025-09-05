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
import math

from mrv.models import Project, Physiography, ProjectDataImportManager
from mrv.serializers import ProjectSerializer
from mrv.data_import_utils import DataImportService, DataImportError
from mrv.data_quality_utils import DataQualityService, DataQualityError
from psycopg2.sql import SQL, Identifier
from django.db import connections
from sympy import symbols, exp, log, sqrt, parse_expr
from sympy.core.sympify import SympifyError
from django.conf import settings

@csrf_exempt
@require_http_methods(["POST"])
def api_project_slanted_height_calculation(request, project_id):
    """API endpoint to run slanted height calculation for trees"""
    try:
        project = Project.objects.get(id=project_id)
        
        # Get request data to check for phy_zone filter
        request_data = json.loads(request.body) if request.body else {}
        phy_zone_filter = request_data.get('phy_zone')
        
        # Get project schema name
        schema_name = project.get_schema_name()
        
        with connections['default'].cursor() as cursor:
            cursor.execute(SQL("SET search_path TO {}").format(Identifier(schema_name)))
            
            # Build the query to get trees that need slanted height calculation
            base_query = """
                SELECT 
                    t.calc_id,
                    t.plot_code,
                    t.species_code,
                    s.species_name,
                    t.dbh,
                    t.height,
                    t.base_tree_height,
                    t.crown_class,
                    t.phy_zone,
                    t.base_slope
                FROM tree_biometric_calc t
                LEFT JOIN public.forest_species s ON t.species_code = s.code
                WHERE t.ignore = FALSE 
                AND t.height IS NOT NULL
                AND t.height > 0
                AND t.crown_class < 6
                AND t.heigth_calculated IS NULL
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
                    'error': 'No trees found that need slanted height calculation'
                }, status=400)
            
            updated_count = 0
            results = []
            errors = []
            
            for tree_data in trees_data:
                calc_id, plot_code, species_code, species_name, dbh, height, base_tree_height, crown_class, phy_zone, base_slope = tree_data
                
                try:
                    # Handle base_tree_height: if null or negative, treat as 0
                    base_height = base_tree_height if base_tree_height is not None and base_tree_height >= 0 else 0
                    
                    # Calculate slanted height using Pythagorean formula
                    corrected_height = math.sqrt((height ** 2) + (base_height ** 2))
                    
                    # Update the tree record with calculated height
                    cursor.execute("""
                        UPDATE tree_biometric_calc 
                        SET heigth_calculated = %s, updated_date = CURRENT_TIMESTAMP
                        WHERE calc_id = %s
                    """, [corrected_height, calc_id])
                    
                    updated_count += 1
                    
                    results.append({
                        'plot_code': plot_code,
                        'species_code': species_code,
                        'species_name': species_name or 'Unknown',
                        'dbh': dbh,
                        'original_height': height,
                        'base_tree_height': base_tree_height,
                        'base_height_used': base_height,
                        'corrected_height': corrected_height,
                        'crown_class': crown_class,
                        'phy_zone': phy_zone,
                        'base_slope': base_slope
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
            message = f'Slanted height calculation completed for phy_zone {phy_zone_filter}: {updated_count} trees updated'
        else:
            message = f'Slanted height calculation completed for all zones: {updated_count} trees updated'
        
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
def api_project_slanted_height_calculation_status(request, project_id):
    """API endpoint to check slanted height calculation status for a specific phy_zone"""
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
            
            # Get total trees count for the phy_zone that need slanted height calculation
            cursor.execute("""
                SELECT COUNT(*) 
                FROM tree_biometric_calc 
                WHERE phy_zone = %s 
                AND ignore = FALSE 
                AND height IS NOT NULL
                AND height > 0
                AND crown_class < 6
            """, [phy_zone])
            total_trees = cursor.fetchone()[0]
            
            # Get trees with calculated height (not null)
            cursor.execute("""
                SELECT COUNT(*) 
                FROM tree_biometric_calc 
                WHERE phy_zone = %s 
                AND ignore = FALSE 
                AND height IS NOT NULL
                AND height > 0
                AND crown_class < 6
                AND heigth_calculated IS NOT NULL
            """, [phy_zone])
            calculated_trees = cursor.fetchone()[0]
            
            # Get trees without calculated height (null)
            cursor.execute("""
                SELECT COUNT(*) 
                FROM tree_biometric_calc 
                WHERE phy_zone = %s 
                AND ignore = FALSE 
                AND height IS NOT NULL
                AND height > 0
                AND crown_class < 6
                AND heigth_calculated IS NULL
            """, [phy_zone])
            uncalculated_trees = cursor.fetchone()[0]
        
        # Determine status
        if total_trees == 0:
            status = 'no_data'
            message = 'No trees found that need slanted height calculation in this zone (requires height > 0)'
        elif uncalculated_trees == 0:
            status = 'complete'
            message = f'Slanted height calculation completed successfully for all {total_trees} trees'
        elif calculated_trees == 0:
            status = 'not_started'
            message = f'Slanted height calculation not yet started for {total_trees} trees'
        else:
            status = 'partial'
            message = f'Slanted height calculation partially completed: {calculated_trees} of {total_trees} trees calculated'
        
        return JsonResponse({
            'success': True,
            'status': status,
            'message': message,
            'total_trees': total_trees,
            'calculated_trees': calculated_trees,
            'uncalculated_trees': uncalculated_trees,
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
