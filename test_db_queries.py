#!/usr/bin/env python
"""
Test script to directly test database queries for tree counting
"""
import os
import sys
import django
from django.conf import settings

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'carbonapi.settings')
django.setup()

from django.db import connection
from mrv.models import Project

def test_tree_count_queries():
    """Test the tree count queries directly"""
    
    # Replace with your actual project ID
    project_id = 1  # Change this to your actual project ID
    phy_zone = 1    # Change this to the zone you want to test
    
    try:
        # Get project
        project = Project.objects.get(id=project_id)
        schema_name = project.get_schema_name()
        
        print(f"Testing queries for project: {project.name}")
        print(f"Schema: {schema_name}")
        print(f"Zone: {phy_zone}")
        print("=" * 60)
        
        with connection.cursor() as cursor:
            # Set search path to project schema
            cursor.execute("SET search_path TO %s", [schema_name])
            
            # Check current schema
            cursor.execute("SHOW search_path")
            current_schema = cursor.fetchone()
            print(f"Current search_path: {current_schema}")
            
            # Test 1: Count with phy_zone filter (the problematic query)
            cursor.execute("""
                SELECT COUNT(*) 
                FROM tree_biometric_calc 
                WHERE phy_zone = %s AND ignore = FALSE AND crown_class < 7
            """, [phy_zone])
            result1 = cursor.fetchone()
            count1 = result1[0] if result1 else 0
            print(f"Query 1 - Count with phy_zone = {phy_zone}: {count1}")
            
            # Test 2: Count with explicit casting
            cursor.execute("""
                SELECT COUNT(*) 
                FROM tree_biometric_calc 
                WHERE phy_zone = %s::integer AND ignore = FALSE AND crown_class < 7
            """, [phy_zone])
            result2 = cursor.fetchone()
            count2 = result2[0] if result2 else 0
            print(f"Query 2 - Count with phy_zone = {phy_zone}::integer: {count2}")
            
            # Test 3: Count with table alias (like the working query)
            cursor.execute("""
                SELECT COUNT(*) 
                FROM tree_biometric_calc tbc
                WHERE tbc.phy_zone = %s AND tbc.ignore = FALSE AND tbc.crown_class < 7
            """, [phy_zone])
            result3 = cursor.fetchone()
            count3 = result3[0] if result3 else 0
            print(f"Query 3 - Count with table alias: {count3}")
            
            # Test 4: Count without phy_zone filter (like the working query)
            cursor.execute("""
                SELECT COUNT(*) 
                FROM tree_biometric_calc tbc
                WHERE tbc.ignore = FALSE AND tbc.crown_class < 7
            """)
            result4 = cursor.fetchone()
            count4 = result4[0] if result4 else 0
            print(f"Query 4 - Count without phy_zone filter: {count4}")
            
            # Test 5: Check phy_zone values
            cursor.execute("""
                SELECT DISTINCT phy_zone, COUNT(*) 
                FROM tree_biometric_calc 
                WHERE ignore = FALSE AND crown_class < 7
                GROUP BY phy_zone 
                ORDER BY phy_zone
            """)
            phy_zone_breakdown = cursor.fetchall()
            print(f"Phy_zone breakdown: {phy_zone_breakdown}")
            
            # Test 6: Check data types
            cursor.execute("""
                SELECT phy_zone, pg_typeof(phy_zone) as phy_zone_type
                FROM tree_biometric_calc 
                WHERE ignore = FALSE AND crown_class < 7
                LIMIT 5
            """)
            data_types = cursor.fetchall()
            print(f"Sample phy_zone values and types: {data_types}")
            
            # Test 7: Manual verification query
            print(f"\nManual verification query:")
            print(f"SELECT COUNT(*) FROM {schema_name}.tree_biometric_calc WHERE phy_zone = {phy_zone} AND ignore = FALSE AND crown_class < 7;")
            
    except Exception as e:
        print(f"❌ Exception occurred: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_tree_count_queries()
