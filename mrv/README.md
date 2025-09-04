# MRV (Measurement, Reporting, and Verification) App

This Django app provides API endpoints for forest biometrics analysis project management, built to support the carbon frontend application.

## Features

- **Project Management**: Create, read, update, and delete forest analysis projects
- **Project Phases**: Track projects through 4 phases (Data Collection, Analysis, Validation, Reporting)
- **Project Steps**: Track progress within each phase using step numbers
- **Status Tracking**: Monitor project status (Draft, In Progress, Completed, Archived)
- **Progress Tracking**: Automatic progress calculation based on current phase
- **RESTful API**: Full CRUD operations via JSON API endpoints
- **Input Validation**: Strict validation for project names (alphanumeric, underscore, hyphen only)

## Models

### Project
- `name`: Unique project identifier (alphanumeric, underscore, hyphen only)
- `description`: Project description
- `status`: Project status (draft, in_progress, completed, archived)
- `current_phase`: Current phase (1-4)
- `current_step`: Current step within the phase (positive integer)
- `created_by`: User who created the project
- `created_date`: Project creation date
- `last_modified`: Last modification date

### Physiography
- `code`: Unique physiography code
- `name`: Physiography name
- `ecological`: Ecological classification

## Project Name Validation

Project names must only contain:
- **Letters** (a-z, A-Z)
- **Numbers** (0-9)
- **Underscores** (_)
- **Hyphens** (-)

**Valid Examples:**
- `forest_analysis_2024`
- `carbon-assessment`
- `Project123`
- `my_project_name`
- `forest-analysis-2024`

**Invalid Examples:**
- `project with spaces` (spaces not allowed)
- `project@123` (special characters not allowed)
- `project.name` (dots not allowed)
- `project/name` (slashes not allowed)

## API Endpoints

### Project Management
- `GET /api/mrv/api/projects/` - List all projects
- `GET /api/mrv/api/projects/{id}/` - Get project details
- `POST /api/mrv/api/projects/create/` - Create new project
- `PUT /api/mrv/api/projects/{id}/update/` - Update project
- `DELETE /api/mrv/api/projects/{id}/delete/` - Delete project

### Physiography
- `GET /api/mrv/api/physiography/` - List all physiography options

## Setup Instructions

1. **Install Dependencies**
   ```bash
   pip install django djangorestframework django-cors-headers
   ```

2. **Run Migrations**
   ```bash
   python manage.py makemigrations mrv
   python manage.py migrate
   ```

3. **Populate Sample Data** (Optional)
   ```bash
   python manage.py populate_sample_data
   ```

4. **Create Superuser** (Optional)
   ```bash
   python manage.py createsuperuser
   ```

## Database Configuration

The app uses the `NFC_db` database as configured in `settings.py`:

```python
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'NFC_db',
        'USER': 'postgres',
        'PASSWORD': 'P@ssw0rd',
        'HOST': 'localhost',
        'PORT': '5432',
    }
}
```

## Usage Examples

### Creating a Project
```python
from mrv.models import Project

# Create project with valid name
project = Project.objects.create(
    name='forest_analysis_2024',  # Valid: alphanumeric + underscore
    description='Comprehensive forest analysis project',
    current_step=1
)

# This would raise a ValidationError:
# project = Project.objects.create(
#     name='forest analysis 2024',  # Invalid: contains spaces
#     description='This will fail'
# )
```

### API Usage with JavaScript
```javascript
// Create project with valid name
const createProject = async (data) => {
    const response = await fetch('/api/mrv/api/projects/create/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data)
    });
    return response.json();
};

// Valid project creation
const validProject = await createProject({
    name: 'forest_analysis_2024',
    description: 'Valid project name'
});

// Invalid project creation (will return error)
const invalidProject = await createProject({
    name: 'forest analysis 2024',  // Invalid: contains spaces
    description: 'This will fail'
});
// Response: { "success": false, "error": "Project name can only contain letters, numbers, underscores (_), and hyphens (-)." }

// List projects
const getProjects = async () => {
    const response = await fetch('/api/mrv/api/projects/');
    return response.json();
};

// Update project progress
const updateProgress = async (projectId, phase, step) => {
    const response = await fetch(`/api/mrv/api/projects/${projectId}/update/`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            current_phase: phase,
            current_step: step
        })
    });
    return response.json();
};
```

## Testing

Run the test suite:
```bash
python manage.py test mrv
```

The test suite includes comprehensive validation tests for project names, ensuring only valid characters are accepted.

## Admin Interface

Access the Django admin interface at `/admin/` to manage:
- Projects
- Physiography data
- Forest species
- Height-diameter models

## Project Phases

1. **Phase 1 - Data Collection**: Initial data gathering and setup
2. **Phase 2 - Analysis**: Data processing and analysis
3. **Phase 3 - Validation**: Results validation and quality control
4. **Phase 4 - Reporting**: Final report generation and delivery

## Project Steps

Each phase can have multiple steps represented by the `current_step` field. Steps are positive integers starting from 1, allowing for granular progress tracking within each phase.

## Status Values

- `draft`: Project is in draft stage
- `in_progress`: Project is actively being worked on
- `completed`: Project has been completed
- `archived`: Project has been archived

## Integration with Frontend

This API is designed to work with the Svelte frontend application. The API responses are formatted to match the frontend's expected data structure, including:

- Progress percentage calculation
- Status and phase display names
- Date formatting
- Step tracking within phases
- Input validation with clear error messages

## File Structure

```
mrv/
├── __init__.py
├── admin.py              # Django admin configuration
├── apps.py               # App configuration
├── models.py             # Database models with validation
├── serializers.py        # API serializers with validation
├── tests.py              # Test cases including validation tests
├── urls.py               # URL routing
├── views.py              # API views with validation
├── management/           # Management commands
│   └── commands/
│       └── populate_sample_data.py
├── migrations/           # Database migrations
└── README.md            # This file
```

## Contributing

1. Follow Django coding standards
2. Write tests for new features
3. Update documentation as needed
4. Use meaningful commit messages
5. Ensure all project names follow the validation rules

## License

This project is part of the Carbon Frontend application suite.
