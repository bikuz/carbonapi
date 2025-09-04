# from django.db import models

# class SchemaImport(models.Model):
#     STATUS_CHOICES = [
#         ('pending', 'Pending'),
#         ('processing', 'Processing'),
#         ('completed', 'Completed'),
#         ('failed', 'Failed'),
#     ]
    
#     uploaded_file = models.FileField(upload_to='temp_sql_imports/')
#     schema_name = models.CharField(max_length=100, blank=True, null=True)
#     status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
#     created_at = models.DateTimeField(auto_now_add=True)
#     completed_at = models.DateTimeField(null=True, blank=True)
#     message = models.TextField(blank=True, null=True)
    
#     class Meta:
#         db_table = 'schema_imports'  # Explicit table name
#         managed = False  # Since we'll create the table via SQL
    
#     def __str__(self):
#         return f"Import {self.id} - {self.status}"