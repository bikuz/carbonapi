from django.urls import path
from . import views

urlpatterns = [
    # Test endpoint
    # path('test/', views.test, name='test'),
    
    # Schema import endpoints
    path('upload-sql-zip/', views.upload_sql_zip, name='upload_sql_zip'),
    path('confirm-import/', views.confirm_import, name='confirm_import'),
    
    # Schema merge endpoints
    path('merge-schemas/', views.merge_schemas_api, name='merge_schemas_api'),
    path('merge-multiple-schemas/', views.merge_multiple_schemas_api, name='merge_multiple_schemas_api'),
    path('merge-multiple-schemas-optimized/', views.merge_multiple_schemas_optimized_api, name='merge_multiple_schemas_optimized_api'),
    path('merge-schemas-incremental/', views.merge_schemas_incremental_api, name='merge_schemas_incremental_api'),
    
    # Schema information and comparison endpoints
    path('get-schema-info/', views.get_schema_info_api, name='get_schema_info_api'),
    path('compare-schemas/', views.compare_schemas_api, name='compare_schemas_api'),
    path('list-schemas/', views.list_schemas_api, name='list_schemas_api'),
    
    # Schema management endpoints
    path('delete-schema/', views.delete_schema_api, name='delete_schema_api'),
    
    # Cleanup endpoints
    # path('cleanup-temp-files/', views.cleanup_temp_files_api, name='cleanup_temp_files_api'),
    path('test-cleanup/', views.test_cleanup_api, name='test_cleanup_api'),
]