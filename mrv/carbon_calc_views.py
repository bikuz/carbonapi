from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.db import connection
from django.conf import settings
import json
import logging
from .models import Project, Physiography, ForestSpecies, Allometric
import math

logger = logging.getLogger(__name__)

def determine_height_to_use(crown_class, height_measured, height_predicted):
    """
    Determine the height to use for biomass calculation based on tree condition:
    • If the tree is broken (crown_class == 6) and its predicted height (Pre_ht) is less than its measured height (height), use height × 1.1
    • Otherwise, use the measured height
    • If no height was measured (or it was below 1.3m), use the predicted height Pre_ht
    """
    try:
        # Convert to float if they are not None
        height_measured_float = float(height_measured) if height_measured is not None else None
        height_predicted_float = float(height_predicted) if height_predicted is not None else None
        crown_class_int = int(crown_class) if crown_class is not None else None
        
        # If no height was measured or it was below 1.3m, use predicted height
        if height_measured_float is None or height_measured_float < 1.3:
            return height_predicted_float if height_predicted_float is not None else 0
        
        # If tree is broken (crown_class == 6) and predicted height is less than measured height
        if crown_class_int == 6 and height_predicted_float is not None and height_predicted_float < height_measured_float:
            return height_measured_float * 1.1
        
        # Otherwise, use the measured height
        return height_measured_float
        
    except (ValueError, TypeError) as e:
        logger.warning(f"Error determining height to use: {str(e)}. Using predicted height as fallback.")
        return float(height_predicted) if height_predicted is not None else 0

def calculate_tree_biomass(dbh, height_predicted, volume_ratio, allometric_data, crown_class=None, height_measured=None):
    """
    Calculate biomass and carbon for a single tree based on the 5-step process:
    1. Determine height to use
    2. Calculate expansion factor
    3. Calculate tree volume and biomass
    4. Calculate branch and foliage biomass
    5. Scale up to per-hectare and convert to carbon
    """
    try:
        # Step 1: Determine height to use (height_use)
        height_use = determine_height_to_use(crown_class, height_measured, height_predicted)
        
        # Step 2: Calculate expansion factor (exp_fa)
        # Based on tree's diameter at breast height (dbh)
        if dbh < 10:
            exp_fa = 198.94
        elif dbh < 20:
            exp_fa = 49.74
        elif dbh < 30:
            exp_fa = 14.15
        else:
            exp_fa = 7.96
        
        # Step 3: Calculate tree volume and biomass
        # Basal Area (BA_tree_sqm)
        ba_tree_sqm = (math.pi * dbh * dbh) / 40000
        
        # Stem Volume (volume_cum_tree)
        if allometric_data[2] is not None and allometric_data[3] is not None:  # stem_a and stem_b
            if allometric_data[4] is not None:  # stem_c available
                stem_volume = math.exp(allometric_data[2] + allometric_data[3] * math.log(dbh) + allometric_data[4] * math.log(height_use)) / 1000
            else:
                stem_volume = math.exp(allometric_data[2] + allometric_data[3] * math.log(dbh)) / 1000
        else:
            stem_volume = 0
        
        # Max Volume (form factor 0.7)
        max_volume = ba_tree_sqm * height_use * 0.7
        
        # Volume Correction using volume_ratio
        if volume_ratio and volume_ratio > 0:
            volume_final_cum_tree = stem_volume * volume_ratio
        else:
            volume_final_cum_tree = stem_volume
        
        # Ensure volume doesn't exceed max volume
        if volume_final_cum_tree > max_volume:
            volume_final_cum_tree = max_volume
        
        # Stem Biomass (kg per tree)
        density = allometric_data[1] if allometric_data[1] else 0
        stem_kg_tree = volume_final_cum_tree * density
        
        # Step 4: Calculate branch and foliage biomass
        # Interpolate branch and foliage ratios based on diameter
        branch_ratio = interpolate_ratio(dbh, allometric_data[15], allometric_data[16], allometric_data[17])  # branch_s, branch_m, branch_l
        foliage_ratio = interpolate_ratio(dbh, allometric_data[18], allometric_data[19], allometric_data[20])  # foliage_s, foliage_m, foliage_l
        
        # Branch and foliage biomass
        branch_kg_tree = stem_kg_tree * branch_ratio if branch_ratio else 0
        foliage_kg_tree = stem_kg_tree * foliage_ratio if foliage_ratio else 0
        
        # Step 5: Scale up to per-hectare and convert to carbon
        # Calculate additional fields
        
        # Basal area per hectare
        ba_per_ha = ba_tree_sqm * exp_fa
        
        # Volume per hectare
        volume_final_cum_ha = volume_final_cum_tree * exp_fa
        
        # Volume BA tree (basal area * height)
        volume_ba_tree = ba_tree_sqm * height_use
        
        # Total tree biomass (air-dry)
        total_biomass_ad_tree = stem_kg_tree + branch_kg_tree + foliage_kg_tree
        
        # Biomass per hectare (air-dry)
        total_biomass_ad_ton_ha = (total_biomass_ad_tree * exp_fa) / 1000
        
        # Total biomass air-dry (same as above, for consistency)
        total_bio_ad = total_biomass_ad_ton_ha
        
        # Biomass per hectare (oven-dry) - convert from air-dry
        total_biomass_od_ton_ha = total_biomass_ad_ton_ha / 1.1
        
        # Carbon per hectare (47% carbon fraction)
        carbon_ton_ha = total_biomass_od_ton_ha * 0.47
        
        # Carbon per tree (kg)
        carbon_kg_tree = (carbon_ton_ha * 1000) / exp_fa
        
        # Per hectare values
        stem_ton_ha = (stem_kg_tree * exp_fa) / 1000
        branch_ton_ha = (branch_kg_tree * exp_fa) / 1000
        foliage_ton_ha = (foliage_kg_tree * exp_fa) / 1000
        
        return {
            'exp_fa': exp_fa,
            'ba_per_sqm': ba_tree_sqm,
            'ba_per_ha': ba_per_ha,
            'volume_cum_tree': stem_volume,
            'volume_ba_tree': volume_ba_tree,
            'volume_final_cum_tree': volume_final_cum_tree,
            'volume_final_cum_ha': volume_final_cum_ha,
            'branch_ratio': branch_ratio,
            'branch_ratio_final': branch_ratio,
            'foliage_ratio': foliage_ratio,
            'foliage_ratio_final': foliage_ratio,
            'stem_kg_tree': stem_kg_tree,
            'branch_kg_tree': branch_kg_tree,
            'foliage_kg_tree': foliage_kg_tree,
            'stem_ton_ha': stem_ton_ha,
            'branch_ton_ha': branch_ton_ha,
            'foliage_ton_ha': foliage_ton_ha,
            'total_biomass_ad_tree': total_biomass_ad_tree,
            'total_biom_ad_ton_ha': total_biomass_ad_ton_ha,
            'total_bio_ad': total_bio_ad,
            'total_biomass_od_tree': total_biomass_ad_tree / 1.1,
            'total_biom_od_ton_ha': total_biomass_od_ton_ha,
            'carbon_kg_tree': carbon_kg_tree,
            'carbon_ton_ha': carbon_ton_ha
        }
        
    except Exception as e:
        logger.error(f"Error calculating tree biomass: {str(e)}")
        return {
            'exp_fa': 0,
            'ba_per_sqm': 0,
            'ba_per_ha': 0,
            'volume_cum_tree': 0,
            'volume_ba_tree': 0,
            'volume_final_cum_tree': 0,
            'volume_final_cum_ha': 0,
            'branch_ratio': 0,
            'branch_ratio_final': 0,
            'foliage_ratio': 0,
            'foliage_ratio_final': 0,
            'stem_kg_tree': 0,
            'branch_kg_tree': 0,
            'foliage_kg_tree': 0,
            'stem_ton_ha': 0,
            'branch_ton_ha': 0,
            'foliage_ton_ha': 0,
            'total_biomass_ad_tree': 0,
            'total_biom_ad_ton_ha': 0,
            'total_bio_ad': 0,
            'total_biomass_od_tree': 0,
            'total_biom_od_ton_ha': 0,
            'carbon_kg_tree': 0,
            'carbon_ton_ha': 0
        }

def interpolate_ratio(dbh, small_ratio, medium_ratio, large_ratio):
    """
    Interpolate branch/foliage ratio based on diameter
    Small: < 10 cm, Medium: 10-20 cm, Large: > 20 cm
    """
    if not all([small_ratio, medium_ratio, large_ratio]):
        return 0
    
    if dbh < 10:
        return small_ratio
    elif dbh < 20:
        # Linear interpolation between small and medium
        return small_ratio + (medium_ratio - small_ratio) * (dbh - 10) / 10
    else:
        # Linear interpolation between medium and large
        return medium_ratio + (large_ratio - medium_ratio) * (dbh - 20) / 10

@csrf_exempt
@require_http_methods(["GET"])
def api_project_allometric_assignment_status(request, project_id):
    """Get allometric assignment status for a project with physiography zone breakdown"""
    try:
        project = Project.objects.get(id=project_id)
        schema_name = project.get_schema_name()
        
        with connection.cursor() as cursor:
            # Set search path to project schema for tree data
            cursor.execute("SET search_path TO %s", [schema_name])
            
            # Get physiography zone breakdown with allometric assignment status
            cursor.execute("""
                SELECT 
                    tbc.phy_zone,
                    p.name as physiography_name,
                    COUNT(DISTINCT tbc.species_code) as species_count,
                    COUNT(*) as tree_count,
                    COUNT(CASE WHEN tbc.vol_eqn_id IS NOT NULL THEN 1 END) as trees_with_vol_eqn_id,
                    COUNT(CASE WHEN tbc.vol_eqn_id IS NULL THEN 1 END) as trees_without_vol_eqn_id,
                    COUNT(DISTINCT CASE WHEN tbc.vol_eqn_id IS NULL THEN tbc.species_code END) as species_without_vol_eqn_id,
                    COUNT(DISTINCT CASE WHEN tbc.vol_eqn_id IS NULL AND a.species_code IS NOT NULL THEN tbc.species_code END) as species_without_vol_eqn_but_has_allometric,
                    COUNT(DISTINCT CASE WHEN tbc.vol_eqn_id IS NULL AND a.species_code IS NULL THEN tbc.species_code END) as species_without_vol_eqn_no_allometric
                FROM tree_biometric_calc tbc
                LEFT JOIN public.allometric a ON tbc.species_code = a.species_code
                LEFT JOIN public.physiography p ON tbc.phy_zone = p.code
                WHERE tbc.ignore = FALSE AND tbc.crown_class < 7
                GROUP BY tbc.phy_zone, p.name
                ORDER BY tbc.phy_zone
            """)
            
            zone_results = cursor.fetchall()
            
            # Calculate overall project statistics
            total_species = 0
            trees_with_vol_eqn_id = 0
            trees_without_vol_eqn_id = 0
            
            physiography_summary = []
            for zone_result in zone_results:
                phy_zone, physiography_name, species_count, tree_count, zone_trees_with_vol_eqn_id, zone_trees_without_vol_eqn_id, zone_species_without_vol_eqn_id, zone_species_without_vol_eqn_but_has_allometric, zone_species_without_vol_eqn_no_allometric = zone_result
                
                total_species += species_count
                trees_with_vol_eqn_id += zone_trees_with_vol_eqn_id
                trees_without_vol_eqn_id += zone_trees_without_vol_eqn_id
                
                # Determine assignment status based on vol_eqn_id
                if zone_trees_without_vol_eqn_id == 0:
                    # All trees have vol_eqn_id - assignment is complete
                    assignment_status = 'complete'
                elif zone_trees_with_vol_eqn_id == 0:
                    # No trees have vol_eqn_id - assignment not started
                    assignment_status = 'not_started'
                elif zone_species_without_vol_eqn_but_has_allometric > 0:
                    # Some species have allometric equations but vol_eqn_id is missing - need to re-assign
                    assignment_status = 'needs_reassign'
                else:
                    # Some species don't have allometric equations - need manual assignment
                    assignment_status = 'needs_manual_assignment'
                
                physiography_summary.append({
                    'phy_zone': phy_zone,
                    'physiography_name': physiography_name or f'Zone {phy_zone}',
                    'species_count': species_count,
                    'tree_count': tree_count,
                    'trees_with_vol_eqn_id': zone_trees_with_vol_eqn_id,
                    'trees_without_vol_eqn_id': zone_trees_without_vol_eqn_id,
                    'species_without_vol_eqn_id': zone_species_without_vol_eqn_id,
                    'species_without_vol_eqn_but_has_allometric': zone_species_without_vol_eqn_but_has_allometric,
                    'species_without_vol_eqn_no_allometric': zone_species_without_vol_eqn_no_allometric,
                    'assignment_status': assignment_status,
                    'vol_eqn_ids_complete': zone_trees_without_vol_eqn_id == 0
                })
            
            all_assigned = trees_without_vol_eqn_id == 0
            vol_eqn_ids_complete = trees_without_vol_eqn_id == 0
            
            return JsonResponse({
                'success': True,
                'total_species': total_species,
                'all_assigned': all_assigned,
                'trees_with_vol_eqn_id': trees_with_vol_eqn_id,
                'trees_without_vol_eqn_id': trees_without_vol_eqn_id,
                'vol_eqn_ids_complete': vol_eqn_ids_complete,
                'physiography_summary': physiography_summary
            })
            
    except Project.DoesNotExist:
        return JsonResponse({'success': False, 'error': 'Project not found'}, status=404)
    except Exception as e:
        logger.error(f"Error getting allometric assignment status: {str(e)}")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)

@csrf_exempt
@require_http_methods(["GET"])
def api_project_biomass_calculation_status(request, project_id):
    """Get biomass calculation status for a project"""
    try:
        project = Project.objects.get(id=project_id)
        schema_name = project.get_schema_name()
        
        with connection.cursor() as cursor:
            # Set search path to project schema for tree data
            cursor.execute("SET search_path TO %s", [schema_name])
            
            # Check if biomass calculations are complete
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_trees,
                    COUNT(CASE WHEN total_biomass_ad_tree IS NOT NULL AND total_biomass_ad_tree > 0 THEN 1 END) as calculated_trees
                FROM tree_biometric_calc
                WHERE ignore = FALSE AND crown_class < 7
            """)
            
            result = cursor.fetchone()
            total_trees = result[0] if result[0] else 0
            calculated_trees = result[1] if result[1] else 0
            
            all_calculated = total_trees > 0 and calculated_trees == total_trees
            
            return JsonResponse({
                'success': True,
                'total_trees': total_trees,
                'calculated_trees': calculated_trees,
                'remaining_trees': total_trees - calculated_trees,
                'all_calculated': all_calculated
            })
            
    except Project.DoesNotExist:
        return JsonResponse({'success': False, 'error': 'Project not found'}, status=404)
    except Exception as e:
        logger.error(f"Error getting biomass calculation status: {str(e)}")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)

@csrf_exempt
@require_http_methods(["POST"])
def api_project_allometric_assignment(request, project_id):
    """Assign allometric equations to species in a specific physiography zone"""
    try:
        project = Project.objects.get(id=project_id)
        schema_name = project.get_schema_name()
        
        data = json.loads(request.body)
        phy_zone = data.get('phy_zone')
        
        if phy_zone is None:
            return JsonResponse({'success': False, 'error': 'phy_zone is required'}, status=400)
        
        with connection.cursor() as cursor:
            # Get physiography name from main schema first
            cursor.execute("SELECT name FROM physiography WHERE code = %s", [phy_zone])
            physiography_result = cursor.fetchone()
            physiography_name = physiography_result[0] if physiography_result else f"Zone {phy_zone}"
            
            # Set search path to project schema for tree data
            cursor.execute("SET search_path TO %s", [schema_name])
            
            # Get unique species in this physiography zone with species information
            cursor.execute("""
                SELECT DISTINCT tbc.species_code, fs.species_name, fs.species, 
                       (SELECT COUNT(*) FROM tree_biometric_calc tbc2 
                        WHERE tbc2.species_code = tbc.species_code 
                        AND tbc2.phy_zone = tbc.phy_zone 
                        AND tbc2.ignore = FALSE 
                        AND tbc2.crown_class < 7) as tree_count
                FROM tree_biometric_calc tbc
                LEFT JOIN public.forest_species fs ON tbc.species_code = fs.code
                WHERE tbc.phy_zone = %s AND tbc.ignore = FALSE AND tbc.crown_class < 7
                ORDER BY tbc.species_code
            """, [phy_zone])
            
            species_data = cursor.fetchall()
            species_codes = [row[0] for row in species_data]
            
            logger.info(f"Found {len(species_data)} unique species in zone {phy_zone}")
            logger.info(f"Species data: {species_data}")
            
            if not species_codes:
                return JsonResponse({
                    'success': True,
                    'message': f'No species found in {physiography_name}',
                    'total_species': 0,
                    'assigned_species': 0,
                    'unassigned_species': 0,
                    'physiography_name': physiography_name
                })
            
            # Check which species already have allometric equations and vol_eqn_id status
            assigned_count = 0
            unassigned_species = []
            unassigned_species_details = []
            vol_eqn_id_status = {
                'trees_with_vol_eqn_id': 0,
                'trees_missing_vol_eqn_id': 0,
                'trees_updated': 0
            }
            
            for species_data_row in species_data:
                species_code, species_name, species, tree_count = species_data_row
                
                # Check if allometric equation exists for this species
                # Use raw SQL to query public.allometric table
                cursor.execute("SELECT id FROM public.allometric WHERE species_code = %s LIMIT 1", [species_code])
                allometric_result = cursor.fetchone()
                allometric_exists = allometric_result is not None
                allometric_id = allometric_result[0] if allometric_result else None
                
                # Check vol_eqn_id status for this species
                cursor.execute("""
                    SELECT 
                        COUNT(CASE WHEN vol_eqn_id IS NOT NULL THEN 1 END) as with_vol_eqn_id,
                        COUNT(CASE WHEN vol_eqn_id IS NULL THEN 1 END) as without_vol_eqn_id
                    FROM tree_biometric_calc 
                    WHERE species_code = %s AND phy_zone = %s AND ignore = FALSE AND crown_class < 7
                """, [species_code, phy_zone])
                
                vol_eqn_result = cursor.fetchone()
                trees_with_vol_eqn_id = vol_eqn_result[0] if vol_eqn_result else 0
                trees_without_vol_eqn_id = vol_eqn_result[1] if vol_eqn_result else 0
                
                # If allometric equation exists but vol_eqn_id is missing, update it
                trees_updated = 0
                if allometric_exists and allometric_id and trees_without_vol_eqn_id > 0:
                    cursor.execute("""
                        UPDATE tree_biometric_calc 
                        SET vol_eqn_id = %s, updated_date = CURRENT_TIMESTAMP
                        WHERE species_code = %s AND phy_zone = %s AND ignore = FALSE AND crown_class < 7 AND vol_eqn_id IS NULL
                    """, [allometric_id, species_code, phy_zone])
                    trees_updated = cursor.rowcount
                    vol_eqn_id_status['trees_updated'] += trees_updated
                    
                    # Update the counts after the update
                    trees_with_vol_eqn_id += trees_updated
                    trees_without_vol_eqn_id -= trees_updated
                    logger.info(f"Updated {trees_updated} trees for species {species_code} with allometric ID {allometric_id}")
                
                vol_eqn_id_status['trees_with_vol_eqn_id'] += trees_with_vol_eqn_id
                vol_eqn_id_status['trees_missing_vol_eqn_id'] += trees_without_vol_eqn_id
                
                if allometric_exists:
                    assigned_count += 1
                else:
                    unassigned_species.append(species_code)
                    species_detail = {
                        'species_code': species_code,
                        'species_name': species_name or f'Species {species_code}',
                        'species': species or '',
                        'tree_count': tree_count,
                        'allometric_id': allometric_id,
                        'trees_with_vol_eqn_id': trees_with_vol_eqn_id,
                        'trees_without_vol_eqn_id': trees_without_vol_eqn_id,
                        'trees_updated': trees_updated
                    }
                    unassigned_species_details.append(species_detail)
                    logger.info(f"Added unassigned species: {species_detail}")
            
            # Debug: Check current schema
            cursor.execute("SHOW search_path")
            current_schema = cursor.fetchone()
            logger.info(f"Current search_path: {current_schema}")
            logger.info(f"Querying for phy_zone: {phy_zone} (type: {type(phy_zone)})")
            
            # Get total tree count for this zone (with crown_class < 7 filter)
            # Debug: Let's try the same approach as the working query
            cursor.execute("""
                SELECT COUNT(*) 
                FROM tree_biometric_calc tbc
                WHERE tbc.phy_zone = %s AND tbc.ignore = FALSE AND tbc.crown_class < 7
            """, [phy_zone])
            total_trees_result = cursor.fetchone()
            total_trees = total_trees_result[0] if total_trees_result else 0
            
            # Debug: Let's also try with explicit casting
            cursor.execute("""
                SELECT COUNT(*) 
                FROM tree_biometric_calc tbc
                WHERE tbc.phy_zone = %s::integer AND tbc.ignore = FALSE AND tbc.crown_class < 7
            """, [phy_zone])
            total_trees_casted_result = cursor.fetchone()
            total_trees_casted = total_trees_casted_result[0] if total_trees_casted_result else 0
            
            logger.info(f"Tree count with phy_zone = {phy_zone}: {total_trees}")
            logger.info(f"Tree count with phy_zone = {phy_zone}::integer: {total_trees_casted}")
            
            # Debug: Let's try the exact same pattern as the working query but with phy_zone filter
            cursor.execute("""
                SELECT COUNT(*) 
                FROM tree_biometric_calc tbc
                WHERE tbc.phy_zone = %s AND tbc.ignore = FALSE AND tbc.crown_class < 7
            """, [phy_zone])
            total_trees_exact_result = cursor.fetchone()
            total_trees_exact = total_trees_exact_result[0] if total_trees_exact_result else 0
            logger.info(f"Tree count with exact pattern: {total_trees_exact}")
            
            # Debug: Let's check what phy_zone values actually exist in the data
            cursor.execute("""
                SELECT DISTINCT phy_zone, COUNT(*) 
                FROM tree_biometric_calc 
                WHERE ignore = FALSE AND crown_class < 7
                GROUP BY phy_zone 
                ORDER BY phy_zone
            """)
            phy_zone_breakdown = cursor.fetchall()
            logger.info(f"Phy_zone breakdown: {phy_zone_breakdown}")
            
            # Debug: Let's also check the count without the crown_class filter to see the difference
            cursor.execute("""
                SELECT COUNT(*) 
                FROM tree_biometric_calc 
                WHERE phy_zone = %s AND ignore = FALSE
            """, [phy_zone])
            total_trees_all_result = cursor.fetchone()
            total_trees_all = total_trees_all_result[0] if total_trees_all_result else 0
            
            # Debug: Check for any NULL crown_class values
            cursor.execute("""
                SELECT COUNT(*) 
                FROM tree_biometric_calc 
                WHERE phy_zone = %s AND ignore = FALSE AND crown_class IS NULL
            """, [phy_zone])
            null_crown_class_result = cursor.fetchone()
            null_crown_class_count = null_crown_class_result[0] if null_crown_class_result else 0
            
            # Debug: Get breakdown by crown_class values
            cursor.execute("""
                SELECT crown_class, COUNT(*) 
                FROM tree_biometric_calc 
                WHERE phy_zone = %s AND ignore = FALSE 
                GROUP BY crown_class 
                ORDER BY crown_class
            """, [phy_zone])
            crown_class_breakdown = cursor.fetchall()
            
            logger.info(f"Returning response: {len(unassigned_species)} unassigned species, {len(unassigned_species_details)} details")
            logger.info(f"Total trees in zone {phy_zone} (crown_class < 7): {total_trees}")
            logger.info(f"Total trees in zone {phy_zone} (all): {total_trees_all}")
            logger.info(f"Trees with NULL crown_class in zone {phy_zone}: {null_crown_class_count}")
            logger.info(f"Crown class breakdown for zone {phy_zone}: {crown_class_breakdown}")
            
            # Debug: Manual verification query (you can run this directly in your database)
            logger.info(f"Manual verification query for zone {phy_zone}:")
            logger.info(f"SELECT COUNT(*) FROM {schema_name}.tree_biometric_calc WHERE phy_zone = {phy_zone} AND ignore = FALSE AND crown_class < 7;")
            
            # Create appropriate message based on updates
            update_message = ""
            if vol_eqn_id_status['trees_updated'] > 0:
                update_message = f" Updated {vol_eqn_id_status['trees_updated']} tree records with vol_eqn_id."
            
            return JsonResponse({
                'success': True,
                'message': f'Allometric assignment status for {physiography_name}.{update_message}',
                'total_species': len(species_codes),
                'assigned_species': assigned_count,
                'unassigned_species': len(unassigned_species),
                'total_trees': total_trees,
                'physiography_name': physiography_name,
                'unassigned_species_codes': unassigned_species,
                'unassigned_species_details': unassigned_species_details,
                'vol_eqn_id_status': vol_eqn_id_status
            })
            
    except Project.DoesNotExist:
        return JsonResponse({'success': False, 'error': 'Project not found'}, status=404)
    except Exception as e:
        logger.error(f"Error assigning allometric equations: {str(e)}")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)

@csrf_exempt
@require_http_methods(["POST"])
def api_project_biomass_calculation(request, project_id):
    """Calculate biomass and carbon for all trees in the project or a specific zone"""
    try:
        project = Project.objects.get(id=project_id)
        schema_name = project.get_schema_name()
        
        # Parse request data to check for phy_zone parameter
        data = json.loads(request.body) if request.body else {}
        phy_zone = data.get('phy_zone')
        
        with connection.cursor() as cursor:
            # Set search path to project schema for tree data
            cursor.execute("SET search_path TO %s", [schema_name])
            
            # Build query with optional phy_zone filter
            base_query = """
                SELECT 
                    calc_id, species_code, dbh, height_predicted, volume_ratio,
                    exp_fa, no_trees_per_ha, vol_eqn_id, crown_class, height
                FROM tree_biometric_calc
                WHERE ignore = FALSE 
                AND crown_class < 7
                AND dbh IS NOT NULL 
                AND height_predicted IS NOT NULL
                AND volume_ratio IS NOT NULL
            """
            
            if phy_zone is not None:
                base_query += " AND phy_zone = %s"
                cursor.execute(base_query, [phy_zone])
            else:
                cursor.execute(base_query)
            
            trees_data = cursor.fetchall()
            
            if not trees_data:
                zone_message = f' for zone {phy_zone}' if phy_zone is not None else ''
                return JsonResponse({
                    'success': True,
                    'message': f'No trees found for biomass calculation{zone_message}',
                    'total_trees': 0,
                    'calculated_trees': 0
                })
            
            calculated_count = 0
            errors = []
            
            for tree_data in trees_data:
                calc_id, species_code, dbh, height_predicted, volume_ratio, exp_fa, no_trees_per_ha, vol_eqn_id, crown_class, height_measured = tree_data
                
                try:
                    # Get allometric equation using vol_eqn_id if available, otherwise fallback to species_code
                    if vol_eqn_id:
                        cursor.execute("""
                            SELECT species_code, density, stem_a, stem_b, stem_c, 
                                   top_10_a, top_10_b, top_20_a, top_20_b,
                                   bark_stem_a, bark_stem_b, bark_top_10_a, bark_top_10_b, 
                                   bark_top_20_a, bark_top_20_b, branch_s, branch_m, branch_l,
                                   foliage_s, foliage_m, foliage_l
                            FROM public.allometric 
                            WHERE id = %s
                        """, [vol_eqn_id])
                    else:
                        cursor.execute("""
                            SELECT species_code, density, stem_a, stem_b, stem_c, 
                                   top_10_a, top_10_b, top_20_a, top_20_b,
                                   bark_stem_a, bark_stem_b, bark_top_10_a, bark_top_10_b, 
                                   bark_top_20_a, bark_top_20_b, branch_s, branch_m, branch_l,
                                   foliage_s, foliage_m, foliage_l
                            FROM public.allometric 
                            WHERE species_code = %s
                        """, [species_code])
                    
                    allometric_data = cursor.fetchone()
                    
                    if not allometric_data:
                        if vol_eqn_id:
                            errors.append(f"Tree {calc_id}: Allometric equation with ID {vol_eqn_id} not found")
                        else:
                            errors.append(f"Species {species_code}: No allometric equation found")
                        continue
                    
                    # Calculate biomass components using our custom function
                    biomass_results = calculate_tree_biomass(
                        dbh=dbh,
                        height_predicted=height_predicted,
                        volume_ratio=volume_ratio,
                        allometric_data=allometric_data,
                        crown_class=crown_class,
                        height_measured=height_measured
                    )
                    
                    
                    # Update tree record with biomass calculations
                    cursor.execute("""
                        UPDATE tree_biometric_calc SET
                            exp_fa = %s,
                            ba_per_sqm = %s,
                            ba_per_ha = %s,
                            volume_cum_tree = %s,
                            volume_ba_tree = %s,
                            volume_final_cum_tree = %s,
                            volume_final_cum_ha = %s,
                            branch_ratio = %s,
                            branch_ratio_final = %s,
                            foliage_ratio = %s,
                            foliage_ratio_final = %s,
                            stem_kg_tree = %s,
                            branch_kg_tree = %s,
                            foliage_kg_tree = %s,
                            stem_ton_ha = %s,
                            branch_ton_ha = %s,
                            foliage_ton_ha = %s,
                            total_biomass_ad_tree = %s,
                            total_biom_ad_ton_ha = %s,
                            total_bio_ad = %s,
                            total_biomass_od_tree = %s,
                            total_biom_od_ton_ha = %s,
                            carbon_kg_tree = %s,
                            carbon_ton_ha = %s,
                            updated_date = CURRENT_TIMESTAMP
                        WHERE calc_id = %s
                    """, [
                        biomass_results['exp_fa'],
                        biomass_results['ba_per_sqm'],
                        biomass_results['ba_per_ha'],
                        biomass_results['volume_cum_tree'],
                        biomass_results['volume_ba_tree'],
                        biomass_results['volume_final_cum_tree'],
                        biomass_results['volume_final_cum_ha'],
                        biomass_results['branch_ratio'],
                        biomass_results['branch_ratio_final'],
                        biomass_results['foliage_ratio'],
                        biomass_results['foliage_ratio_final'],
                        biomass_results['stem_kg_tree'],
                        biomass_results['branch_kg_tree'],
                        biomass_results['foliage_kg_tree'],
                        biomass_results['stem_ton_ha'],
                        biomass_results['branch_ton_ha'],
                        biomass_results['foliage_ton_ha'],
                        biomass_results['total_biomass_ad_tree'],
                        biomass_results['total_biom_ad_ton_ha'],
                        biomass_results['total_bio_ad'],
                        biomass_results['total_biomass_od_tree'],
                        biomass_results['total_biom_od_ton_ha'],
                        biomass_results['carbon_kg_tree'],
                        biomass_results['carbon_ton_ha'],
                        calc_id
                    ])
                    
                    calculated_count += 1
                    
                except Exception as e:
                    errors.append(f"Tree {calc_id}: {str(e)}")
                    continue
            
            # Calculate summary statistics
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_trees,
                    SUM(total_biomass_ad_tree) as total_biomass_kg,
                    SUM(total_biom_ad_ton_ha) as total_biomass_ton_ha,
                    SUM(carbon_kg_tree) as total_carbon_kg,
                    SUM(carbon_ton_ha) as total_carbon_ton_ha
                FROM tree_biometric_calc
                WHERE ignore = FALSE
                AND crown_class < 7
                AND total_biomass_ad_tree IS NOT NULL
            """)
            
            summary_result = cursor.fetchone()
            
            # Calculate CO2 equivalent (carbon * 3.67)
            total_carbon_ton_ha = summary_result[4] if summary_result[4] else 0
            co2_equivalent_ton_ha = total_carbon_ton_ha * 3.67
            
            zone_message = f' for zone {phy_zone}' if phy_zone is not None else ''
            return JsonResponse({
                'success': True,
                'message': f'Biomass calculation completed successfully{zone_message}',
                'total_trees': len(trees_data),
                'calculated_trees': calculated_count,
                'errors_count': len(errors),
                'phy_zone': phy_zone,
                'summary': {
                    'total_trees': summary_result[0] if summary_result[0] else 0,
                    'total_biomass': total_carbon_ton_ha,  # Using carbon as proxy for biomass
                    'total_carbon': total_carbon_ton_ha,
                    'co2_equivalent': co2_equivalent_ton_ha
                },
                'biomass_results': {
                    'total_biomass_kg': summary_result[1] if summary_result[1] else 0,
                    'total_biomass_ton_ha': summary_result[2] if summary_result[2] else 0
                },
                'carbon_results': {
                    'total_carbon_kg': summary_result[3] if summary_result[3] else 0,
                    'total_carbon_ton_ha': total_carbon_ton_ha
                },
                'errors': errors[:10] if errors else []  # Limit errors for response size
            })
            
    except Project.DoesNotExist:
        return JsonResponse({'success': False, 'error': 'Project not found'}, status=404)
    except Exception as e:
        logger.error(f"Error calculating biomass: {str(e)}")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)

@csrf_exempt
@require_http_methods(["POST"])
def api_save_allometric_assignments(request, project_id):
    """Save allometric assignments to public.allometric table and update vol_eqn_id in tree records"""
    try:
        project = Project.objects.get(id=project_id)
        data = json.loads(request.body)
        allometric_data = data.get('allometric_data', [])
        
        if not allometric_data:
            return JsonResponse({'success': False, 'error': 'No allometric data provided'}, status=400)
        
        schema_name = project.get_schema_name()
        
        with connection.cursor() as cursor:
            saved_count = 0
            updated_trees_count = 0
            errors = []
            
            for species_data in allometric_data:
                try:
                    species_code = species_data.get('species_code')
                    density = species_data.get('density')
                    stem_a = species_data.get('stem_a')
                    stem_b = species_data.get('stem_b')
                    stem_c = species_data.get('stem_c')
                    top_10_a = species_data.get('top_10_a')
                    top_10_b = species_data.get('top_10_b')
                    top_20_a = species_data.get('top_20_a')
                    top_20_b = species_data.get('top_20_b')
                    bark_stem_a = species_data.get('bark_stem_a')
                    bark_stem_b = species_data.get('bark_stem_b')
                    bark_top_10_a = species_data.get('bark_top_10_a')
                    bark_top_10_b = species_data.get('bark_top_10_b')
                    bark_top_20_a = species_data.get('bark_top_20_a')
                    bark_top_20_b = species_data.get('bark_top_20_b')
                    branch_s = species_data.get('branch_s')
                    branch_m = species_data.get('branch_m')
                    branch_l = species_data.get('branch_l')
                    foliage_s = species_data.get('foliage_s')
                    foliage_m = species_data.get('foliage_m')
                    foliage_l = species_data.get('foliage_l')
                    
                    if not all([species_code, density is not None, stem_a is not None, stem_b is not None]):
                        errors.append(f"Species {species_code}: Missing required fields")
                        continue
                    
                    # Check if species exists in forest_species table
                    cursor.execute("SELECT 1 FROM public.forest_species WHERE code = %s", [species_code])
                    if not cursor.fetchone():
                        errors.append(f"Species {species_code}: Species not found in forest_species table")
                        continue
                    
                    # Insert or update allometric equation and get the ID
                    cursor.execute("""
                        INSERT INTO public.allometric (
                            species_code, density, stem_a, stem_b, stem_c,
                            top_10_a, top_10_b, top_20_a, top_20_b,
                            bark_stem_a, bark_stem_b, bark_top_10_a, bark_top_10_b,
                            bark_top_20_a, bark_top_20_b, branch_s, branch_m, branch_l,
                            foliage_s, foliage_m, foliage_l
                        ) VALUES (
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s
                        )
                        ON CONFLICT (species_code) 
                        DO UPDATE SET
                            density = EXCLUDED.density,
                            stem_a = EXCLUDED.stem_a,
                            stem_b = EXCLUDED.stem_b,
                            stem_c = EXCLUDED.stem_c,
                            top_10_a = EXCLUDED.top_10_a,
                            top_10_b = EXCLUDED.top_10_b,
                            top_20_a = EXCLUDED.top_20_a,
                            top_20_b = EXCLUDED.top_20_b,
                            bark_stem_a = EXCLUDED.bark_stem_a,
                            bark_stem_b = EXCLUDED.bark_stem_b,
                            bark_top_10_a = EXCLUDED.bark_top_10_a,
                            bark_top_10_b = EXCLUDED.bark_top_10_b,
                            bark_top_20_a = EXCLUDED.bark_top_20_a,
                            bark_top_20_b = EXCLUDED.bark_top_20_b,
                            branch_s = EXCLUDED.branch_s,
                            branch_m = EXCLUDED.branch_m,
                            branch_l = EXCLUDED.branch_l,
                            foliage_s = EXCLUDED.foliage_s,
                            foliage_m = EXCLUDED.foliage_m,
                            foliage_l = EXCLUDED.foliage_l
                        RETURNING id
                    """, [
                        species_code, density, stem_a, stem_b, stem_c,
                        top_10_a or 0, top_10_b or 0, top_20_a or 0, top_20_b or 0,
                        bark_stem_a or 0, bark_stem_b or 0, bark_top_10_a or 0, bark_top_10_b or 0,
                        bark_top_20_a or 0, bark_top_20_b or 0, branch_s or 0, branch_m or 0, branch_l or 0,
                        foliage_s or 0, foliage_m or 0, foliage_l or 0
                    ])
                    
                    # Get the allometric equation ID
                    allometric_id = cursor.fetchone()[0]
                    
                    # Update vol_eqn_id in tree_biometric_calc table for this species
                    cursor.execute("SET search_path TO %s", [schema_name])
                    cursor.execute("""
                        UPDATE tree_biometric_calc 
                        SET vol_eqn_id = %s, updated_date = CURRENT_TIMESTAMP
                        WHERE species_code = %s AND ignore = FALSE
                    """, [allometric_id, species_code])
                    
                    trees_updated = cursor.rowcount
                    updated_trees_count += trees_updated
                    
                    saved_count += 1
                    logger.info(f"Updated {trees_updated} trees for species {species_code} with allometric ID {allometric_id}")
                    
                except Exception as e:
                    errors.append(f"Species {species_code}: {str(e)}")
                    continue
            
            return JsonResponse({
                'success': True,
                'message': f'Allometric assignments saved successfully. Updated {updated_trees_count} tree records.',
                'saved_count': saved_count,
                'total_count': len(allometric_data),
                'updated_trees_count': updated_trees_count,
                'errors': errors
            })
            
    except Project.DoesNotExist:
        return JsonResponse({'success': False, 'error': 'Project not found'}, status=404)
    except Exception as e:
        logger.error(f"Error saving allometric assignments: {str(e)}")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)

@csrf_exempt
@require_http_methods(["GET"])
def api_allometric_models(request):
    """Get list of all allometric models"""
    try:
        allometric_models = Allometric.objects.select_related('species').all()
        
        models_data = []
        for model in allometric_models:
            models_data.append({
                'id': model.id,
                'species_code': model.species_code,
                'species_name': model.species.species_name if model.species else 'Unknown',
                'density': model.density,
                'stem_a': model.stem_a,
                'stem_b': model.stem_b,
                'stem_c': model.stem_c,
                'top_10_a': model.top_10_a,
                'top_10_b': model.top_10_b,
                'top_20_a': model.top_20_a,
                'top_20_b': model.top_20_b,
                'bark_stem_a': model.bark_stem_a,
                'bark_stem_b': model.bark_stem_b,
                'bark_top_10_a': model.bark_top_10_a,
                'bark_top_10_b': model.bark_top_10_b,
                'bark_top_20_a': model.bark_top_20_a,
                'bark_top_20_b': model.bark_top_20_b,
                'branch_s': model.branch_s,
                'branch_m': model.branch_m,
                'branch_l': model.branch_l,
                'foliage_s': model.foliage_s,
                'foliage_m': model.foliage_m,
                'foliage_l': model.foliage_l
            })
        
        return JsonResponse({
            'success': True,
            'allometric_models': models_data,
            'total_count': len(models_data)
        })
        
    except Exception as e:
        logger.error(f"Error getting allometric models: {str(e)}")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)
