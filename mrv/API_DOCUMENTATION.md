# MRV API Documentation

This document describes the API endpoints for the MRV (Measurement, Reporting, and Verification) app for forest biometrics analysis.

## Base URL
All API endpoints are prefixed with `/api/mrv/`

## Authentication
Currently, the API endpoints are open (no authentication required). In production, consider adding authentication middleware.

## Project Management Endpoints

### 1. List All Projects
**GET** `/api/mrv/api/projects/`

Returns a list of all projects.

**Response:**
```json
{
  "success": true,
  "projects": [
    {
      "id": 1,
      "name": "pine_forest_study_2024",
      "description": "Analysis of pine forest growth patterns",
      "status": "in_progress",
      "current_phase": 2,
      "current_step": 3,
      "created_date": "2024-01-15",
      "last_modified": "2024-01-20",
      "created_by_username": "admin",
      "progress_percentage": 50.0,
      "status_display": "In Progress",
      "phase_display": "Phase 2 - Analysis"
    }
  ]
}
```

### 2. Get Project Details
**GET** `/api/mrv/api/projects/{project_id}/`

Returns detailed information about a specific project.

**Response:**
```json
{
  "success": true,
  "project": {
    "id": 1,
    "name": "pine_forest_study_2024",
    "description": "Analysis of pine forest growth patterns",
    "status": "in_progress",
    "current_phase": 2,
    "current_step": 3,
    "created_date": "2024-01-15",
    "last_modified": "2024-01-20",
    "created_by_username": "admin",
    "progress_percentage": 50.0,
    "status_display": "In Progress",
    "phase_display": "Phase 2 - Analysis"
  }
}
```

### 3. Create New Project
**POST** `/api/mrv/api/projects/create/`

Creates a new project.

**Request Body:**
```json
{
  "name": "new_project_name",
  "description": "Project description (optional)",
  "current_step": 1
}
```

**Required Fields:**
- `name`: Unique project identifier

**Optional Fields:**
- `description`: Project description
- `current_step`: Current step within the phase (default: 1)

**Project Name Validation:**
Project names must only contain:
- Letters (a-z, A-Z)
- Numbers (0-9)
- Underscores (_)
- Hyphens (-)

**Examples of Valid Names:**
- `forest_analysis_2024`
- `carbon-assessment`
- `Project123`
- `my_project_name`
- `forest-analysis-2024`

**Examples of Invalid Names:**
- `project with spaces` (spaces not allowed)
- `project@123` (special characters not allowed)
- `project.name` (dots not allowed)
- `project/name` (slashes not allowed)

**Response:**
```json
{
  "success": true,
  "project": {
    "id": 2,
    "name": "new_project_name",
    "description": "Project description",
    "status": "draft",
    "current_phase": 1,
    "current_step": 1,
    "created_date": "2024-01-21",
    "last_modified": "2024-01-21",
    "created_by_username": "admin",
    "progress_percentage": 25.0,
    "status_display": "Draft",
    "phase_display": "Phase 1 - Data Collection"
  },
  "message": "Project created successfully"
}
```

### 4. Update Project
**PUT** `/api/mrv/api/projects/{project_id}/update/`

Updates an existing project.

**Request Body:**
```json
{
  "description": "Updated description",
  "status": "in_progress",
  "current_phase": 2,
  "current_step": 3
}
```

**Response:**
```json
{
  "success": true,
  "project": {
    "id": 1,
    "name": "pine_forest_study_2024",
    "description": "Updated description",
    "status": "in_progress",
    "current_phase": 2,
    "current_step": 3,
    "created_date": "2024-01-15",
    "last_modified": "2024-01-21",
    "created_by_username": "admin",
    "progress_percentage": 50.0,
    "status_display": "In Progress",
    "phase_display": "Phase 2 - Analysis"
  },
  "message": "Project updated successfully"
}
```

### 5. Delete Project
**DELETE** `/api/mrv/api/projects/{project_id}/delete/`

Deletes a project.

**Response:**
```json
{
  "success": true,
  "message": "Project deleted successfully"
}
```

## Physiography Endpoints

### 1. List Physiography Options
**GET** `/api/mrv/api/physiography/`

Returns a list of all physiography options available for projects.

**Response:**
```json
{
  "success": true,
  "physiography": [
    {
      "code": 1,
      "name": "Terai",
      "ecological": "Terai"
    },
    {
      "code": 2,
      "name": "Hill",
      "ecological": "Hill"
    },
    {
      "code": 3,
      "name": "Mountain",
      "ecological": "Mountain"
    }
  ]
}
```

## Project Status Values

- `draft`: Project is in draft stage
- `in_progress`: Project is actively being worked on
- `completed`: Project has been completed
- `archived`: Project has been archived

## Project Phases

- `1`: Phase 1 - Data Collection
- `2`: Phase 2 - Analysis
- `3`: Phase 3 - Validation
- `4`: Phase 4 - Reporting

## Project Steps

The `current_step` field represents the current step within a phase. Steps are positive integers starting from 1. Each phase can have multiple steps, allowing for more granular progress tracking.

## Error Responses

All endpoints return error responses in the following format:

```json
{
  "success": false,
  "error": "Error message description"
}
```

**Common HTTP Status Codes:**
- `200`: Success
- `201`: Created
- `400`: Bad Request (validation errors)
- `404`: Not Found
- `500`: Internal Server Error

**Common Validation Errors:**
- `"Project name is required"`: Name field is missing
- `"Project name can only contain letters, numbers, underscores (_), and hyphens (-)."`: Invalid characters in name
- `"Project with this name already exists"`: Duplicate project name
- `"Current phase must be between 1 and 4."`: Invalid phase number
- `"Current step must be 1 or greater."`: Invalid step number

## Example Usage with Frontend

### Creating a Project
```javascript
const createProject = async (projectData) => {
  const response = await fetch('/api/mrv/api/projects/create/', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(projectData)
  });
  
  const data = await response.json();
  if (data.success) {
    console.log('Project created:', data.project);
    return data.project;
  } else {
    throw new Error(data.error);
  }
};

// Usage with valid name
const newProject = await createProject({
  name: 'forest_analysis_2024',
  description: 'Comprehensive forest analysis project',
  current_step: 1
});

// Usage with invalid name (will throw error)
try {
  const invalidProject = await createProject({
    name: 'forest analysis 2024', // Invalid: contains spaces
    description: 'This will fail'
  });
} catch (error) {
  console.error('Validation error:', error.message);
}
```

### Listing Projects
```javascript
const getProjects = async () => {
  const response = await fetch('/api/mrv/api/projects/');
  const data = await response.json();
  
  if (data.success) {
    return data.projects;
  } else {
    throw new Error(data.error);
  }
};

// Usage
const projects = await getProjects();
projects.forEach(project => {
  console.log(`${project.name} - ${project.status_display} (Phase ${project.current_phase}, Step ${project.current_step})`);
});
```

### Updating Project Progress
```javascript
const updateProjectProgress = async (projectId, phase, step) => {
  const response = await fetch(`/api/mrv/api/projects/${projectId}/update/`, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      current_phase: phase,
      current_step: step
    })
  });
  
  const data = await response.json();
  if (data.success) {
    console.log('Project updated:', data.project);
    return data.project;
  } else {
    throw new Error(data.error);
  }
};

// Usage
const updatedProject = await updateProjectProgress(1, 2, 3);
```
