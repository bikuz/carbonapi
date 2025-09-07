#!/usr/bin/env python
"""
Test script for allometric assignment API function
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

from django.test import RequestFactory
from mrv.carbon_calc_views import api_project_allometric_assignment
import json

def test_allometric_assignment():
    """Test the allometric assignment function for a specific project and zone"""
    
    # Create a mock request
    factory = RequestFactory()
    
    # Replace with your actual project ID
    project_id = 1  # Change this to your actual project ID
    
    # Test data
    test_data = {
        'phy_zone': 1  # Change this to the zone you want to test
    }
    
    # Create POST request
    request = factory.post(
        f'/api/mrv/projects/{project_id}/allometric-assignment/',
        data=json.dumps(test_data),
        content_type='application/json'
    )
    
    print(f"Testing allometric assignment for project {project_id}, zone {test_data['phy_zone']}")
    print("=" * 60)
    
    try:
        # Call the function
        response = api_project_allometric_assignment(request, project_id)
        
        # Parse response
        if hasattr(response, 'content'):
            response_data = json.loads(response.content.decode('utf-8'))
        else:
            response_data = response
            
        print("Response:")
        print(json.dumps(response_data, indent=2))
        
        if response_data.get('success'):
            print(f"\n✅ Success!")
            print(f"Total species: {response_data.get('total_species', 'N/A')}")
            print(f"Assigned species: {response_data.get('assigned_species', 'N/A')}")
            print(f"Total trees: {response_data.get('total_trees', 'N/A')}")
        else:
            print(f"\n❌ Error: {response_data.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"❌ Exception occurred: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_allometric_assignment()
