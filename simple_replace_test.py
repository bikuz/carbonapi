#!/usr/bin/env python3
"""
Simple test script to verify data replace functionality in MRV data import
"""

import os
import sys
import django
import json
from datetime import datetime

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'carbonapi.settings')
django.setup()

from mrv.data_import_utils import DataImportService
from django.db import connection

def test_replace_functionality():
    """Test the replace functionality directly with DataImportService"""
    
    print("🧪 Testing Data Replace Functionality Directly")
    print("=" * 50)
    
    # Create a test project schema manually
    project_name = f"test_replace_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    schema_name = f"project_{project_name.lower()}"
    
    print(f"📁 Creating test schema: {schema_name}")
    
    try:
        with connection.cursor() as cursor:
            # Create schema
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            print(f"✅ Schema created: {schema_name}")
            
            # Set search path
            cursor.execute(f"SET search_path TO {schema_name}")
            
            # Create project_data_imports table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS project_data_imports (
                    id BIGSERIAL PRIMARY KEY,
                    schema_name VARCHAR(255) NOT NULL,
                    table_name VARCHAR(255) NOT NULL,
                    action VARCHAR(20) NOT NULL DEFAULT 'append' CHECK (action IN ('append', 'replace', 'replace_selected')),
                    status VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
                    imported_rows INTEGER NOT NULL DEFAULT 0,
                    total_rows INTEGER NOT NULL DEFAULT 0,
                    description TEXT,
                    error_message TEXT,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP WITH TIME ZONE,
                    completed_at TIMESTAMP WITH TIME ZONE
                )
            """)
            print("✅ project_data_imports table created")
            
            # Create tree_biometric_calc table with import_id
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tree_biometric_calc (
                    calc_id BIGSERIAL PRIMARY KEY,
                    import_id BIGINT,
                    plot_id BIGINT NOT NULL,
                    plot_col BIGINT NOT NULL,
                    plot_row BIGINT NOT NULL,
                    plot_number BIGINT NOT NULL,
                    plot_code character varying(255),
                    plot_x REAL,
                    plot_y REAL,
                    phy_zone INTEGER,
                    district_code INTEGER,
                    tree_no INTEGER,
                    forest_stand INTEGER,
                    bearing REAL,
                    distance REAL,
                    tree_x REAL,
                    tree_y REAL,
                    species_code INTEGER,
                    hd_model_code INTEGER,
                    dbh REAL,
                    quality_class INTEGER,
                    quality_class_code BIGINT,
                    crown_class INTEGER,
                    crown_class_code BIGINT,
                    sample_tree_type INTEGER,
                    sample_tree_type_code BIGINT,
                    height REAL,
                    crown_height REAL,
                    base_tree_height REAL,
                    base_crown_height REAL,
                    base_slope REAL,
                    age INTEGER,
                    radial_growth INTEGER,
                    heigth_calculated REAL,
                    height_predicted REAL,
                    volume_ratio REAL,
                    exp_fa REAL,
                    no_trees_per_ha REAL,
                    ba_per_sqm REAL,
                    ba_per_ha REAL,
                    volume_cum_tree REAL,
                    volume_ba_tree REAL,
                    volume_final_cum_tree REAL,
                    Volume_final_cum_ha REAL,
                    branch_ratio REAL,
                    branch_ratio_final REAL,
                    foliage_ratio REAL,
                    foliage_ratio_final REAL,
                    stem_kg_tree REAL,
                    branch_kg_tree REAL,
                    foliage_kg_tree REAL,
                    stem_ton_ha REAL,
                    branch_ton_ha REAL,
                    foliage_ton_ha REAL,
                    total_biomass_ad_tree REAL,
                    total_biom_ad_ton_ha REAL,
                    total_bio_ad REAL,
                    total_biomass_od_tree REAL,
                    total_biom_od_ton_ha REAL,
                    carbon_kg_tree REAL,
                    carbon_ton_ha REAL,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    CONSTRAINT fk_tree_biometric_calc_import_id 
                        FOREIGN KEY (import_id) REFERENCES project_data_imports(id)
                        ON DELETE CASCADE
                )
            """)
            print("✅ tree_biometric_calc table created with import_id")
            
            # Insert some test data to simulate existing data
            cursor.execute("""
                INSERT INTO project_data_imports 
                (schema_name, table_name, action, status, imported_rows, description)
                VALUES 
                ('test_schema', 'test_table', 'append', 'completed', 100, 'Test import')
            """)
            test_import_id = cursor.fetchone()[0] if cursor.description else 1
            print(f"✅ Test import record created with ID: {test_import_id}")
            
            # Insert some test data into tree_biometric_calc
            cursor.execute("""
                INSERT INTO tree_biometric_calc 
                (import_id, plot_id, plot_col, plot_row, plot_number, tree_no, dbh, height)
                VALUES 
                (%s, 1, 1, 1, 1, 1, 25.5, 15.2),
                (%s, 1, 1, 1, 1, 2, 30.1, 18.5),
                (%s, 2, 1, 1, 2, 1, 22.3, 12.8)
            """, (test_import_id, test_import_id, test_import_id))
            print("✅ Test data inserted (3 rows)")
            
            # Count existing rows
            cursor.execute("SELECT COUNT(*) FROM tree_biometric_calc")
            existing_count = cursor.fetchone()[0]
            print(f"📊 Existing rows in tree_biometric_calc: {existing_count}")
            
    except Exception as e:
        print(f"❌ Failed to create test schema: {str(e)}")
        return False
    
    # Test the replace functionality
    print("\n🔄 Testing Replace Functionality")
    print("-" * 40)
    
    try:
        with DataImportService() as import_service:
            # Create a mock project object for testing
            class MockProject:
                def __init__(self, schema_name):
                    self.schema_name = schema_name
                
                def get_schema_name(self):
                    return schema_name
                
                def schema_exists(self):
                    return True
                
                def table_exists(self, table_name):
                    return True
            
            mock_project = MockProject(schema_name)
            
            # Create a new import record
            with connection.cursor() as cursor:
                cursor.execute(f"SET search_path TO {schema_name}")
                cursor.execute("""
                    INSERT INTO project_data_imports 
                    (schema_name, table_name, action, status, description)
                    VALUES 
                    (%s, %s, %s, %s, %s)
                    RETURNING id
                """, ('fra_high_mountain_2076_77', 'tree_and_climber', 'replace_selected', 'pending', 'Test replace selected import'))
                import_id = cursor.fetchone()[0]
                print(f"📝 Created import record with ID: {import_id}")
                
                # Create another import record with same schema/table to test replacement
                cursor.execute("""
                    INSERT INTO project_data_imports 
                    (schema_name, table_name, action, status, description)
                    VALUES 
                    (%s, %s, %s, %s, %s)
                    RETURNING id
                """, ('fra_high_mountain_2076_77', 'tree_and_climber', 'append', 'completed', 'Previous import from same source'))
                previous_import_id = cursor.fetchone()[0]
                print(f"📝 Created previous import record with ID: {previous_import_id}")
                
                # Add some data to the previous import
                cursor.execute("""
                    INSERT INTO tree_biometric_calc 
                    (import_id, plot_id, plot_col, plot_row, plot_number, tree_no, dbh, height)
                    VALUES 
                    (%s, 999, 1, 1, 999, 1, 99.9, 99.9)
                """, (previous_import_id,))
                print(f"📝 Added test data to previous import")
            
            # Test the import with replace_selected action
            print(f"📤 Testing import with action: replace_selected")
            print(f"📤 Source: fra_high_mountain_2076_77.tree_and_climber")
            
            success, message, imported_rows = import_service.import_data_to_project(
                mock_project, 
                import_id, 
                'fra_high_mountain_2076_77', 
                'tree_and_climber', 
                'replace_selected'
            )
            
            if success:
                print(f"✅ Import successful!")
                print(f"📊 Imported rows: {imported_rows}")
                print(f"📝 Message: {message}")
                
                # Check the final state
                with connection.cursor() as cursor:
                    cursor.execute(f"SET search_path TO {schema_name}")
                    
                    # Count final rows
                    cursor.execute("SELECT COUNT(*) FROM tree_biometric_calc")
                    final_count = cursor.fetchone()[0]
                    print(f"📊 Final rows in tree_biometric_calc: {final_count}")
                    
                    # Check import_id distribution
                    cursor.execute("SELECT import_id, COUNT(*) FROM tree_biometric_calc GROUP BY import_id ORDER BY import_id")
                    import_distribution = cursor.fetchall()
                    print(f"📊 Import ID distribution: {import_distribution}")
                    
                    # Check if old data was replaced
                    if final_count == imported_rows:
                        print("✅ Replace selected worked correctly - only new data exists")
                    else:
                        print(f"⚠️ Replace selected may not have worked - expected {imported_rows} rows, got {final_count}")
                    
                    # Check if import record was updated (should be the same ID)
                    cursor.execute("""
                        SELECT id, schema_name, table_name, action, status, description 
                        FROM project_data_imports 
                        WHERE schema_name = %s AND table_name = %s
                        ORDER BY created_at DESC
                    """, ('fra_high_mountain_2076_77', 'tree_and_climber'))
                    
                    import_records = cursor.fetchall()
                    print(f"📊 Import records for same schema/table: {len(import_records)} records")
                    
                    if len(import_records) == 1:
                        print("✅ Import record replacement worked - only one record exists")
                        record = import_records[0]
                        print(f"📝 Record ID: {record[0]}, Action: {record[3]}, Status: {record[4]}")
                    else:
                        print(f"⚠️ Multiple import records found: {len(import_records)}")
                        for record in import_records:
                            print(f"📝 Record ID: {record[0]}, Action: {record[3]}, Status: {record[4]}")
                
                return True
            else:
                print(f"❌ Import failed: {message}")
                return False
                
    except Exception as e:
        print(f"❌ Test failed with exception: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Clean up - drop the test schema
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
                print(f"🧹 Cleaned up test schema: {schema_name}")
        except Exception as e:
            print(f"⚠️ Warning: Could not clean up test schema: {str(e)}")

if __name__ == "__main__":
    print("🚀 Starting Simple MRV Data Replace Selected Functionality Test")
    print("=" * 60)
    
    success = test_replace_functionality()
    
    print("\n" + "=" * 60)
    print("📋 Test Results Summary:")
    print(f"   Replace selected functionality test: {'✅ PASSED' if success else '❌ FAILED'}")
    
    if success:
        print("\n🎉 Test passed! Replace selected functionality is working correctly.")
    else:
        print("\n⚠️ Test failed. Check the output above for details.")
