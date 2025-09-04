"""
Simple serializers for MRV models
"""

from .models import Project, Physiography


class ProjectSerializer:
    """Simple serializer for Project model"""
    
    def __init__(self, instance=None, many=False):
        self.instance = instance
        self.many = many
    
    @property
    def data(self):
        if self.many:
            return [self._serialize_project(proj) for proj in self.instance]
        else:
            return self._serialize_project(self.instance)
    
    def _serialize_project(self, project):
        return {
            'id': project.id,
            'name': project.name,
            'description': project.description,
            'status': project.status,
            'current_phase': project.current_phase,
            'current_step': project.current_step,
            'created_by': project.created_by.id if project.created_by else None,
            'created_date': project.created_date.isoformat() if project.created_date else None,
            'last_modified': project.last_modified.isoformat() if project.last_modified else None,
            'progress_percentage': project.get_progress_percentage(),
            'schema_name': project.get_schema_name()
        }


# ProjectDataImportSerializer is no longer needed since we use 
# ProjectDataImportManager which returns dictionary data directly