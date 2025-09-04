from django.urls import path
from . import views

app_name = 'mrv'

urlpatterns = [
    # Project Management API endpoints
    path('projects/', views.api_projects_list, name='api_projects_list'),
    path('projects/create/', views.api_project_create, name='api_project_create'),
    path('projects/<int:project_id>/', views.api_project_detail, name='api_project_detail'),
    path('projects/<int:project_id>/update/', views.api_project_update, name='api_project_update'),
    path('projects/<int:project_id>/delete/', views.api_project_delete, name='api_project_delete'),
    
    # Physiography API endpoints
    path('physiography/', views.api_physiography_list, name='api_physiography_list'),
    path('projects/<int:project_id>/physiography-options/', views.api_project_physiography_options, name='api_project_physiography_options'),
    
    # Forest Species API endpoints
    path('forest-species/', views.api_forest_species_list, name='api_forest_species_list'),
    
    # Project Schema API endpoints
    path('projects/<int:project_id>/schema/', views.api_project_schema_info, name='api_project_schema_info'),
    
    # Data Import API endpoints
    path('projects/<int:project_id>/data-imports/', views.api_project_data_imports_list, name='api_project_data_imports_list'),
    path('projects/<int:project_id>/data-imports/preview/', views.api_project_data_import_preview, name='api_project_data_import_preview'),
    path('projects/<int:project_id>/data-imports/create/', views.api_project_data_import_create, name='api_project_data_import_create'),
    path('projects/<int:project_id>/data-imports/<int:import_id>/', views.api_project_data_import_detail, name='api_project_data_import_detail'),
    path('projects/<int:project_id>/data-imports/<int:import_id>/delete/', views.api_project_data_import_delete, name='api_project_data_import_delete'),
    
    # Data Quality Check API endpoints
    path('projects/<int:project_id>/data-quality-check/', views.api_project_data_quality_check, name='api_project_data_quality_check'),
    path('projects/<int:project_id>/data-quality-check/<str:issue_type>/details/', views.api_project_data_quality_issue_details, name='api_project_data_quality_issue_details'),
    path('projects/<int:project_id>/data-quality-check/update-record/', views.api_project_data_quality_update_record, name='api_project_data_quality_update_record'),
    path('projects/<int:project_id>/data-quality-check/bulk-update/', views.api_project_data_quality_bulk_update, name='api_project_data_quality_bulk_update'),
    path('projects/<int:project_id>/data-quality-check/ignore-records/', views.api_project_data_quality_ignore_records, name='api_project_data_quality_ignore_records'),
    path('projects/<int:project_id>/data-quality-check/unignore-records/', views.api_project_data_quality_unignore_records, name='api_project_data_quality_unignore_records'),
    path('projects/<int:project_id>/data-quality-check/<str:issue_type>/ignored-records/', views.api_project_data_quality_ignored_records, name='api_project_data_quality_ignored_records'),
    
    # Data Cleaning API endpoints
    path('projects/<int:project_id>/data-cleaning/summary/', views.api_project_data_cleaning_summary, name='api_project_data_cleaning_summary'),
    path('projects/<int:project_id>/data-cleaning/remove-ignored/', views.api_project_data_cleaning_remove_ignored, name='api_project_data_cleaning_remove_ignored'),
    path('projects/<int:project_id>/data-cleaning/view-records/', views.api_project_data_cleaning_view_records, name='api_project_data_cleaning_view_records'),
    
    # HD Model API endpoints
    path('hd-model/', views.api_hd_model_list, name='api_hd_model_list'),
    path('projects/<int:project_id>/hd-model/physiography-summary/', views.api_project_hd_model_physiography_summary, name='api_project_hd_model_physiography_summary'),
    path('projects/<int:project_id>/hd-model/assign-models/', views.api_project_hd_model_assign_models, name='api_project_hd_model_assign_models'),
    path('projects/<int:project_id>/hd-model/unassigned-records/', views.api_project_hd_model_unassigned_records, name='api_project_hd_model_unassigned_records'),
    path('projects/<int:project_id>/hd-model/update-species-mapping/', views.api_project_hd_model_update_species_mapping, name='api_project_hd_model_update_species_mapping'),
    
    # Legacy endpoints (commented out for now)
    # path('height-diameter-modeling/', views.height_diameter_modeling, name='height_diameter_modeling'),
    # path('model-diagnostics/<int:model_id>/', views.model_diagnostics, name='model_diagnostics'),
    # path('api/predict-height/', views.api_predict_height, name='api_predict_height'),
    # path('export-height-models/', views.export_height_models, name='export_height_models'),
    # path('import-tree-data/', views.import_tree_data_with_heights, name='import_tree_data_with_heights'),
]
