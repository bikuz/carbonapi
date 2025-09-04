from django.test import TestCase, Client
from django.contrib.auth.models import User
from django.urls import reverse
import json
from .models import Project, Physiography

# Create your tests here.

class ProjectAPITestCase(TestCase):
    def setUp(self):
        """Set up test data"""
        self.client = Client()
        self.user = User.objects.create_user(
            username='testuser',
            password='testpass123'
        )
        
        # Create test physiography
        self.physiography = Physiography.objects.create(
            code=1,
            name='Test Physiography',
            ecological='Test Ecological'
        )
        
        # Create test project
        self.project = Project.objects.create(
            name='test_project',
            description='Test project description',
            status='draft',
            current_phase=1,
            current_step=1,
            created_by=self.user
        )
    
    def test_projects_list_api(self):
        """Test GET /api/mrv/api/projects/"""
        response = self.client.get('/api/mrv/api/projects/')
        self.assertEqual(response.status_code, 200)
        
        data = json.loads(response.content)
        self.assertTrue(data['success'])
        self.assertEqual(len(data['projects']), 1)
        self.assertEqual(data['projects'][0]['name'], 'test_project')
    
    def test_project_detail_api(self):
        """Test GET /api/mrv/api/projects/<id>/"""
        response = self.client.get(f'/api/mrv/api/projects/{self.project.id}/')
        self.assertEqual(response.status_code, 200)
        
        data = json.loads(response.content)
        self.assertTrue(data['success'])
        self.assertEqual(data['project']['name'], 'test_project')
        self.assertEqual(data['project']['description'], 'Test project description')
    
    def test_project_create_api(self):
        """Test POST /api/mrv/api/projects/create/"""
        project_data = {
            'name': 'new_project',
            'description': 'New project description',
            'current_step': 2
        }
        
        response = self.client.post(
            '/api/mrv/api/projects/create/',
            data=json.dumps(project_data),
            content_type='application/json'
        )
        
        self.assertEqual(response.status_code, 201)
        
        data = json.loads(response.content)
        self.assertTrue(data['success'])
        self.assertEqual(data['project']['name'], 'new_project')
        self.assertEqual(data['project']['status'], 'draft')
        self.assertEqual(data['project']['current_phase'], 1)
        self.assertEqual(data['project']['current_step'], 2)
    
    def test_project_update_api(self):
        """Test PUT /api/mrv/api/projects/<id>/update/"""
        update_data = {
            'description': 'Updated description',
            'current_phase': 2,
            'current_step': 3
        }
        
        response = self.client.put(
            f'/api/mrv/api/projects/{self.project.id}/update/',
            data=json.dumps(update_data),
            content_type='application/json'
        )
        
        self.assertEqual(response.status_code, 200)
        
        data = json.loads(response.content)
        self.assertTrue(data['success'])
        self.assertEqual(data['project']['description'], 'Updated description')
        self.assertEqual(data['project']['current_phase'], 2)
        self.assertEqual(data['project']['current_step'], 3)
    
    def test_physiography_list_api(self):
        """Test GET /api/mrv/api/physiography/"""
        response = self.client.get('/api/mrv/api/physiography/')
        self.assertEqual(response.status_code, 200)
        
        data = json.loads(response.content)
        self.assertTrue(data['success'])
        self.assertEqual(len(data['physiography']), 1)
        self.assertEqual(data['physiography'][0]['name'], 'Test Physiography')
    
    def test_project_validation(self):
        """Test project validation"""
        # Test duplicate name
        duplicate_data = {
            'name': 'test_project',  # Already exists
        }
        
        response = self.client.post(
            '/api/mrv/api/projects/create/',
            data=json.dumps(duplicate_data),
            content_type='application/json'
        )
        
        self.assertEqual(response.status_code, 400)
        
        data = json.loads(response.content)
        self.assertFalse(data['success'])
        self.assertIn('already exists', data['error'])
    
    def test_project_name_validation_valid_characters(self):
        """Test that valid project names are accepted"""
        valid_names = [
            'project123',
            'my_project',
            'test-project',
            'Project_2024',
            'forest_analysis_2024',
            'carbon-assessment',
            'ABC123',
            'project_1_2_3'
        ]
        
        for name in valid_names:
            project_data = {
                'name': name,
                'description': f'Test project with name: {name}'
            }
            
            response = self.client.post(
                '/api/mrv/api/projects/create/',
                data=json.dumps(project_data),
                content_type='application/json'
            )
            
            # Clean up created project for next iteration
            if response.status_code == 201:
                Project.objects.filter(name=name).delete()
            
            self.assertEqual(response.status_code, 201, f"Failed for name: {name}")
    
    def test_project_name_validation_invalid_characters(self):
        """Test that invalid project names are rejected"""
        invalid_names = [
            'project with spaces',
            'project@123',
            'project#name',
            'project$name',
            'project%name',
            'project&name',
            'project*name',
            'project+name',
            'project=name',
            'project/name',
            'project\\name',
            'project.name',
            'project,name',
            'project;name',
            'project:name',
            'project!name',
            'project?name',
            'project(name)',
            'project[name]',
            'project{name}',
            'project<name>',
            'project|name',
            'project~name',
            'project`name',
            'project\'name',
            'project"name'
        ]
        
        for name in invalid_names:
            project_data = {
                'name': name,
                'description': f'Test project with invalid name: {name}'
            }
            
            response = self.client.post(
                '/api/mrv/api/projects/create/',
                data=json.dumps(project_data),
                content_type='application/json'
            )
            
            self.assertEqual(response.status_code, 400, f"Should fail for name: {name}")
            
            data = json.loads(response.content)
            self.assertFalse(data['success'])
            self.assertIn('can only contain letters, numbers, underscores (_), and hyphens (-)', data['error'])
    
    def test_project_not_found(self):
        """Test accessing non-existent project"""
        response = self.client.get('/api/mrv/api/projects/999/')
        self.assertEqual(response.status_code, 404)
        
        data = json.loads(response.content)
        self.assertFalse(data['success'])
        self.assertEqual(data['error'], 'Project not found')
