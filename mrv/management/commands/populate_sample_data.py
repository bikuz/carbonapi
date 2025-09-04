from django.core.management.base import BaseCommand
from django.contrib.auth.models import User
from mrv.models import Project, Physiography


class Command(BaseCommand):
    help = 'Populate sample data for MRV projects'

    def handle(self, *args, **options):
        self.stdout.write('Creating sample data for MRV projects...')
        
        # Create sample physiography if it doesn't exist
        physiography_data = [
            {'code': 1, 'name': 'Terai', 'ecological': 'Terai'},
            {'code': 2, 'name': 'Hill', 'ecological': 'Hill'},
            {'code': 3, 'name': 'Mountain', 'ecological': 'Mountain'},
        ]
        
        for physio_data in physiography_data:
            physiography, created = Physiography.objects.get_or_create(
                code=physio_data['code'],
                defaults=physio_data
            )
            if created:
                self.stdout.write(f'Created physiography: {physiography.name}')
        
        # Get or create a test user
        user, created = User.objects.get_or_create(
            username='admin',
            defaults={
                'email': 'admin@example.com',
                'is_staff': True,
                'is_superuser': True
            }
        )
        if created:
            user.set_password('admin123')
            user.save()
            self.stdout.write('Created admin user')
        
        # Create sample projects with valid names (alphanumeric, underscore, hyphen only)
        sample_projects = [
            {
                'name': 'Pine_Forest_Study_2024',
                'description': 'Analysis of pine forest growth patterns in the hill region',
                'status': 'in_progress',
                'current_phase': 2,
                'current_step': 3
            },
            {
                'name': 'Oak_Grove_Carbon_Assessment',
                'description': 'Carbon sequestration study for oak grove in Terai region',
                'status': 'completed',
                'current_phase': 4,
                'current_step': 5
            },
            {
                'name': 'Mixed_Forest_Inventory',
                'description': 'Comprehensive inventory of mixed forest stand in mountain region',
                'status': 'draft',
                'current_phase': 1,
                'current_step': 1
            },
            {
                'name': 'Bamboo_Growth_Study',
                'description': 'Study of bamboo growth patterns and biomass estimation',
                'status': 'in_progress',
                'current_phase': 3,
                'current_step': 2
            },
            {
                'name': 'Riparian_Forest_Analysis',
                'description': 'Analysis of riparian forest health and biodiversity',
                'status': 'draft',
                'current_phase': 1,
                'current_step': 1
            },
            {
                'name': 'Carbon-Sequestration-2024',
                'description': 'Carbon sequestration analysis for mixed forest',
                'status': 'in_progress',
                'current_phase': 2,
                'current_step': 4
            },
            {
                'name': 'Biomass_Estimation_Project',
                'description': 'Biomass estimation for different forest types',
                'status': 'draft',
                'current_phase': 1,
                'current_step': 2
            }
        ]
        
        for project_data in sample_projects:
            project, created = Project.objects.get_or_create(
                name=project_data['name'],
                defaults={
                    'description': project_data['description'],
                    'status': project_data['status'],
                    'current_phase': project_data['current_phase'],
                    'current_step': project_data['current_step'],
                    'created_by': user
                }
            )
            
            if created:
                self.stdout.write(f'Created project: {project.name}')
            else:
                self.stdout.write(f'Project already exists: {project.name}')
        
        self.stdout.write(
            self.style.SUCCESS('Successfully populated sample data!')
        )
        self.stdout.write(f'Created {Project.objects.count()} projects')
        self.stdout.write(f'Created {Physiography.objects.count()} physiography options')
