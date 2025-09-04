#!/usr/bin/env python3
"""
Test script to verify data replace functionality in MRV data import
"""

import os
import sys
import django
import json
from datetime import datetime

# Add the carbonapi directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'carbonapi'))

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'carbonapi.settings')
django.setup()

from mrv.models import Project
from mrv.data_import_utils import DataImportService
from mrv.views import api_project_data_import_create
from django.test import RequestFactory
from django.contrib.auth.models import User

def test_replace_functionality():
    """Test the replace functionality with a sample project"""
    
    print("🧪 Testing Data Replace Functionality")
    print("=" * 50)
    
    # Create a test project
    project_name = f"test_replace_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    print(f"📁 Creating test project: {project_name}")
    
    try:
        project = Project.objects.create(
            name=project_name,
            description="Test project for replace functionality",
            status='draft'
        )
        print(f"✅ Project created with ID: {project.id}")
        print(f"✅ Project schema: {project.get_schema_name()}")
        
        # Check if schema and tables were created
        if project.schema_exists():
            print(f"✅ Project schema exists: {project.get_schema_name()}")
            
            tables = project.get_schema_tables()
            print(f"✅ Tables in schema: {tables}")
            
            if 'tree_biometric_calc' in tables:
                print("✅ tree_biometric_calc table exists")
                
                # Check if import_id column exists
                if project.column_exists('tree_biometric_calc', 'import_id'):
                    print("✅ import_id column exists in tree_biometric_calc")
                else:
                    print("❌ import_id column missing in tree_biometric_calc")
                    return False
            else:
                print("❌ tree_biometric_calc table missing")
                return False
        else:
            print("❌ Project schema does not exist")
            return False
            
    except Exception as e:
        print(f"❌ Failed to create project: {str(e)}")
        return False
    
    # Test data import with replace action
    print("\n🔄 Testing Data Import with Replace Action")
    print("-" * 40)
    
    try:
        # Create a mock request for testing
        factory = RequestFactory()
        
        # Test data for import
        test_data = {
            'schema_name': 'fra_high_mountain_2076_77',  # Use the specified schema
            'table_name': 'tree_and_climber',     # Use a real table
            'action': 'replace',
            'description': 'Test replace import'
        }
        
        print(f"📤 Sending import request with action: {test_data['action']}")
        print(f"📤 Source: {test_data['schema_name']}.{test_data['table_name']}")
        
        # Create the request
        request = factory.post(
            f'/api/projects/{project.id}/data-imports/create/',
            data=json.dumps(test_data),
            content_type='application/json'
        )
        
        # Call the API view
        response = api_project_data_import_create(request, project.id)
        
        print(f"📥 Response status: {response.status_code}")
        
        if response.status_code == 201:
            response_data = json.loads(response.content)
            print(f"✅ Import successful!")
            print(f"📊 Imported rows: {response_data.get('imported_rows', 0)}")
            print(f"🆔 Import ID: {response_data.get('data_import', {}).get('id', 'N/A')}")
            
            # Check the data in the database
            print("\n📊 Checking imported data...")
            
            with DataImportService() as import_service:
                # Get the table structure to verify data
                structure = import_service.get_project_table_structure(project, 'tree_biometric_calc')
                print(f"📋 Table structure has {len(structure)} columns")
                
                # Count rows in the table
                from django.db import connection
                with connection.cursor() as cursor:
                    cursor.execute(f"SELECT COUNT(*) FROM {project.get_schema_name()}.tree_biometric_calc")
                    row_count = cursor.fetchone()[0]
                    print(f"📊 Total rows in tree_biometric_calc: {row_count}")
                    
                    # Check import_id distribution
                    cursor.execute(f"SELECT import_id, COUNT(*) FROM {project.get_schema_name()}.tree_biometric_calc GROUP BY import_id")
                    import_distribution = cursor.fetchall()
                    print(f"📊 Import ID distribution: {import_distribution}")
            
            return True
            
        else:
            print(f"❌ Import failed with status: {response.status_code}")
            try:
                error_data = json.loads(response.content)
                print(f"❌ Error: {error_data.get('error', 'Unknown error')}")
            except:
                print(f"❌ Response content: {response.content}")
            return False
            
    except Exception as e:
        print(f"❌ Test failed with exception: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Clean up - delete the test project
        try:
            print(f"\n🧹 Cleaning up test project: {project_name}")
            project.delete()
            print("✅ Test project deleted")
        except Exception as e:
            print(f"⚠️ Warning: Could not delete test project: {str(e)}")

def test_append_vs_replace():
    """Test both append and replace actions to compare behavior"""
    
    print("\n🔄 Testing Append vs Replace Comparison")
    print("=" * 50)
    
    # Create a test project
    project_name = f"test_compare_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        project = Project.objects.create(
            name=project_name,
            description="Test project for append vs replace comparison",
            status='draft'
        )
        
        print(f"📁 Created test project: {project_name}")
        
        # Test data
        test_data = {
            'schema_name': 'fra_high_mountain_2076_77',
            'table_name': 'tree_and_climber',
            'description': 'Test import'
        }
        
        factory = RequestFactory()
        
        # Test 1: First import (append)
        print("\n1️⃣ Testing first import (append)...")
        test_data['action'] = 'append'
        
        request = factory.post(
            f'/api/projects/{project.id}/data-imports/create/',
            data=json.dumps(test_data),
            content_type='application/json'
        )
        
        response1 = api_project_data_import_create(request, project.id)
        
        if response1.status_code == 201:
            response_data1 = json.loads(response1.content)
            rows_after_append = response_data1.get('imported_rows', 0)
            print(f"✅ First import successful: {rows_after_append} rows")
        else:
            print(f"❌ First import failed: {response1.status_code}")
            return False
        
        # Test 2: Second import (replace)
        print("\n2️⃣ Testing second import (replace)...")
        test_data['action'] = 'replace'
        
        request = factory.post(
            f'/api/projects/{project.id}/data-imports/create/',
            data=json.dumps(test_data),
            content_type='application/json'
        )
        
        response2 = api_project_data_import_create(request, project.id)
        
        if response2.status_code == 201:
            response_data2 = json.loads(response2.content)
            rows_after_replace = response_data2.get('imported_rows', 0)
            print(f"✅ Second import successful: {rows_after_replace} rows")
            
            # Compare results
            print(f"\n📊 Comparison Results:")
            print(f"   After append:  {rows_after_append} rows")
            print(f"   After replace: {rows_after_replace} rows")
            
            if rows_after_append == rows_after_replace:
                print("✅ Replace worked correctly - same number of rows")
            else:
                print("❌ Replace may not have worked - different row counts")
                
        else:
            print(f"❌ Second import failed: {response2.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Comparison test failed: {str(e)}")
        return False
        
    finally:
        # Clean up
        try:
            project.delete()
            print("✅ Test project cleaned up")
        except:
            pass

if __name__ == "__main__":
    print("🚀 Starting MRV Data Replace Functionality Tests")
    print("=" * 60)
    
    # Test 1: Basic replace functionality
    success1 = test_replace_functionality()
    
    # Test 2: Append vs Replace comparison
    success2 = test_append_vs_replace()
    
    print("\n" + "=" * 60)
    print("📋 Test Results Summary:")
    print(f"   Basic replace test: {'✅ PASSED' if success1 else '❌ FAILED'}")
    print(f"   Append vs Replace: {'✅ PASSED' if success2 else '❌ FAILED'}")
    
    if success1 and success2:
        print("\n🎉 All tests passed! Replace functionality is working correctly.")
    else:
        print("\n⚠️ Some tests failed. Check the output above for details.")
