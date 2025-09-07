from django.db import models
import numpy as np
import pandas as pd
import math
from typing import List, Optional
from django.contrib.auth.models import User
from django.utils import timezone
import re
from django.core.validators import RegexValidator
from django.core.exceptions import ValidationError
from django.db import transaction
from django.conf import settings

from sympy import symbols, exp, log, sqrt
from sympy.parsing.sympy_parser import parse_expr
from sympy.core.sympify import SympifyError

# Import Django's default database connection
from django.db import connection
from psycopg2.sql import SQL, Identifier

class Project(models.Model):
    """Model for forest analysis projects"""
    STATUS_CHOICES = [
        ('draft', 'Draft'),
        ('in_progress', 'In Progress'),
        ('completed', 'Completed'),
        ('archived', 'Archived'),
    ]
    
    PHASE_CHOICES = [
        (1, 'Phase 1 - Data Selection & Validation'),
        (2, 'Phase 2 - Height-Diameter Modelling'),
        (3, 'Phase 3 - Volume Ratio Calculation'),
        (4, 'Phase 4 - Carbon Emission Calculation'),
    ]
    
    # Regex validator for project name - only alphanumeric, underscore, and hyphen
    name_validator = RegexValidator(
        regex=r'^[a-zA-Z0-9_-]+$',
        message='Project name can only contain letters, numbers, underscores (_), and hyphens (-).'
    )
    
    name = models.CharField(
        max_length=255, 
        unique=True, 
        help_text="Unique project identifier",
        validators=[name_validator]
    )
    description = models.TextField(blank=True, null=True, help_text="Project description")
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='draft')
    current_phase = models.IntegerField(choices=PHASE_CHOICES, default=1)
    current_step = models.IntegerField(default=1, help_text="Current step within the phase")
    
    # Metadata
    created_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    created_date = models.DateTimeField(auto_now_add=True)
    last_modified = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'mrv_projects'
        ordering = ['-last_modified']
        verbose_name = 'Project'
        verbose_name_plural = 'Projects'
    
    def __str__(self):
        return f"{self.name}"
    
    def clean(self):
        """Custom validation for the model"""
        super().clean()
        
        # Additional validation for name field
        if self.name:
            if not re.match(r'^[a-zA-Z0-9_-]+$', self.name):
                raise ValidationError({
                    'name': 'Project name can only contain letters, numbers, underscores (_), and hyphens (-).'
                })
    
    def get_progress_percentage(self):
        """Calculate progress percentage based on current phase"""
        return ((self.current_phase-1) / 4) * 100
     
    def update_phase(self, new_phase):
        """Update project phase and status"""
        if 1 <= new_phase <= 4:
            self.current_phase = new_phase
            if new_phase == 4:
                self.status = 'completed'
            elif new_phase > 1:
                self.status = 'in_progress'
            self.save()
    
    def get_schema_name(self):
        """Get the schema name for this project"""
        return f"project_{self.name.lower()}"
    
    def create_project_schema(self):
        """Create a new schema for this project"""
        try:
            schema_name = self.get_schema_name()
            with connection.cursor() as cursor:
                # Create schema if it doesn't exist
                cursor.execute(
                    SQL("CREATE SCHEMA IF NOT EXISTS {}").format(Identifier(schema_name))
                )
                
                # Set search path to the new schema
                cursor.execute(SQL("SET search_path TO {}").format(Identifier(schema_name)))
                
                # Create the project_data_imports table FIRST (required for foreign key)
                self._create_data_imports_table(cursor)
                
                # Create the tree_biometric_calc table (with foreign key to project_data_imports)
                self._create_tree_biometric_calc_table(cursor)
                
            return True, f"Schema '{schema_name}' and tables created successfully"
        except Exception as e:
            return False, f"Failed to create schema: {str(e)}"
    
    def _create_tree_biometric_calc_table(self, cursor):
        """Create the tree_biometric_calc table in the project schema"""
        try:
            # Create table with import tracking support
            cursor.execute("""
                CREATE TABLE tree_biometric_calc (
                    calc_id BIGSERIAL PRIMARY KEY,
                    import_id BIGINT,
                    plot_id BIGINT NOT NULL,
                    plot_col BIGINT NOT NULL,
                    plot_row BIGINT NOT NULL,
                    plot_number BIGINT NOT NULL,
                    plot_code character varying(255),
                    plot_x REAL,
                    plot_y REAL,
                    phy_zone INTEGER,
                    district_code INTEGER,
                    tree_no INTEGER,
                    forest_stand INTEGER,
                    bearing REAL,
                    distance REAL,
                    tree_x REAL,
                    tree_y REAL,
                    species_code INTEGER,
                    hd_model_code INTEGER,
                    dbh REAL,
                    quality_class INTEGER,
                    quality_class_code BIGINT,
                    crown_class INTEGER,
                    crown_class_code BIGINT,
                    sample_tree_type INTEGER,
                    sample_tree_type_code BIGINT,
                    height REAL,
                    crown_height REAL,
                    base_tree_height REAL,
                    base_crown_height REAL,
                    base_slope REAL,
                    age INTEGER,
                    radial_growth INTEGER,
                    heigth_calculated REAL,
                    height_predicted REAL,
                    volume_ratio REAL,
                    vol_eqn_id BIGINT,
                    exp_fa REAL,
                    no_trees_per_ha REAL,
                    ba_per_sqm REAL,
                    ba_per_ha REAL,
                    volume_cum_tree REAL,
                    volume_ba_tree REAL,
                    volume_final_cum_tree REAL,
                    Volume_final_cum_ha REAL,
                    branch_ratio REAL,
                    branch_ratio_final REAL,
                    foliage_ratio REAL,
                    foliage_ratio_final REAL,
                    stem_kg_tree REAL,
                    branch_kg_tree REAL,
                    foliage_kg_tree REAL,
                    stem_ton_ha REAL,
                    branch_ton_ha REAL,
                    foliage_ton_ha REAL,
                    total_biomass_ad_tree REAL,
                    total_biom_ad_ton_ha REAL,
                    total_bio_ad REAL,
                    total_biomass_od_tree REAL,
                    total_biom_od_ton_ha REAL,
                    carbon_kg_tree REAL,
                    carbon_ton_ha REAL,
                    ignore BOOLEAN DEFAULT FALSE,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    -- Foreign key reference to project_data_imports
                    CONSTRAINT fk_tree_biometric_calc_import_id 
                        FOREIGN KEY (import_id) REFERENCES project_data_imports(id)
                        ON DELETE CASCADE
                )
            """)
            print(f"Created tree_biometric_calc table with default structure")
        except Exception as e:
            raise Exception(f"Failed to create tree_biometric_calc table: {str(e)}")
    
    def _create_data_imports_table(self, cursor):
        """Create the project_data_imports table in the project schema"""
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS project_data_imports (
                    id BIGSERIAL PRIMARY KEY,
                    schema_name VARCHAR(255) NOT NULL,
                    table_name VARCHAR(255) NOT NULL,
                    action VARCHAR(20) NOT NULL DEFAULT 'append' CHECK (action IN ('append', 'replace', 'replace_selected')),
                    status VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
                    imported_rows INTEGER NOT NULL DEFAULT 0,
                    total_rows INTEGER NOT NULL DEFAULT 0,
                    description TEXT,
                    error_message TEXT,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP WITH TIME ZONE,
                    completed_at TIMESTAMP WITH TIME ZONE
                );
                
                -- Create indexes for better performance
                CREATE INDEX IF NOT EXISTS idx_project_data_imports_status ON project_data_imports(status);
                CREATE INDEX IF NOT EXISTS idx_project_data_imports_created_at ON project_data_imports(created_at);
                CREATE INDEX IF NOT EXISTS idx_project_data_imports_schema_table ON project_data_imports(schema_name, table_name);
            """)
            print(f"Created project_data_imports table in project schema")
        except Exception as e:
            raise Exception(f"Failed to create project_data_imports table: {str(e)}")
    
    def create_additional_tables(self, table_definitions):
        """Create additional tables in the project schema"""
        try:
            schema_name = self.get_schema_name()
            with connection.cursor() as cursor:
                # Set search path to the project schema
                cursor.execute(SQL("SET search_path TO {}").format(Identifier(schema_name)))
                
                for table_name, sql_definition in table_definitions.items():
                    if not self.table_exists(table_name):
                        cursor.execute(sql_definition)
                        print(f"Created table {table_name} in schema {schema_name}")
                    else:
                        print(f"Table {table_name} already exists in schema {schema_name}")
            
            return True, "Additional tables created successfully"
        except Exception as e:
            return False, f"Failed to create additional tables: {str(e)}"
    
    def delete_project_schema(self):
        """Delete the schema for this project"""
        try:
            schema_name = self.get_schema_name()
            with connection.cursor() as cursor:
                # Drop schema and all its contents
                cursor.execute(
                    SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(Identifier(schema_name))
                )
            return True, f"Schema '{schema_name}' deleted successfully"
        except Exception as e:
            return False, f"Failed to delete schema: {str(e)}"
    
    def schema_exists(self):
        """Check if the project schema exists"""
        try:
            schema_name = self.get_schema_name()
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s",
                    (schema_name,)
                )
                return bool(cursor.fetchone())
        except Exception:
            return False
    
    def get_schema_tables(self):
        """Get list of tables in the project schema"""
        try:
            schema_name = self.get_schema_name()
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = %s 
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """, (schema_name,))
                return [row[0] for row in cursor.fetchall()]
        except Exception:
            return []
    
    def table_exists(self, table_name):
        """Check if a specific table exists in the project schema"""
        try:
            schema_name = self.get_schema_name()
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = %s 
                    AND table_name = %s
                    AND table_type = 'BASE TABLE'
                """, (schema_name, table_name))
                return bool(cursor.fetchone())
        except Exception:
            return False
    
    def column_exists(self, table_name, column_name):
        """Check if a specific column exists in a table"""
        try:
            schema_name = self.get_schema_name()
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = %s 
                    AND table_name = %s
                    AND column_name = %s
                """, (schema_name, table_name, column_name))
                return bool(cursor.fetchone())
        except Exception:
            return False
    
    def get_table_structure(self, table_name):
        """Get the structure of a specific table in the project schema"""
        try:
            schema_name = self.get_schema_name()
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns 
                    WHERE table_schema = %s 
                    AND table_name = %s
                    ORDER BY ordinal_position
                """, (schema_name, table_name))
                return cursor.fetchall()
        except Exception:
            return []
    
    def save(self, *args, **kwargs):
        """Override save to create schema when project is created"""
        is_new = self.pk is None
        super().save(*args, **kwargs)
        
        # Create schema for new projects
        if is_new:
            success, message = self.create_project_schema()
            if not success:
                # Log the error but don't fail the save
                print(f"Warning: {message}")
    
    def delete(self, *args, **kwargs):
        """Override delete to remove schema when project is deleted"""
        # Delete schema first
        success, message = self.delete_project_schema()
        if not success:
            # Log the error but continue with deletion
            print(f"Warning: {message}")
        
        # Then delete the project record
        super().delete(*args, **kwargs)

class Physiography(models.Model):
    code = models.IntegerField(unique=True)
    name = models.CharField(max_length=100)
    ecological = models.CharField(max_length=100)

    def __str__(self):
        return self.name

    class Meta:
        db_table = 'physiography'
        verbose_name = 'physiography'
        verbose_name_plural = 'physiography'

# ProjectDataImport is now created as a table within each project schema
# instead of a Django model to maintain project data isolation

class ProjectDataImportManager:
    """Manager class to handle project data imports within project schemas"""
    
    def __init__(self, project):
        self.project = project
        self.schema_name = project.get_schema_name()
    
    def create_import_record(self, schema_name, table_name, action='append', description=''):
        """Create a new import record in the project schema"""
        from django.db import connection
        from django.utils import timezone
        
        with connection.cursor() as cursor:
            cursor.execute(
                SQL("SET search_path TO {}").format(Identifier(self.schema_name))
            )
            
            # If action is 'replace' or 'replace_selected', check if there's an existing import record with same schema and table
            if action in ['replace', 'replace_selected']:
                cursor.execute("""
                    SELECT id FROM project_data_imports 
                    WHERE schema_name = %s AND table_name = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                """, [schema_name, table_name])
                
                existing_record = cursor.fetchone()
                if existing_record:
                    # Update the existing record instead of creating a new one
                    import_id = existing_record[0]
                    cursor.execute("""
                        UPDATE project_data_imports 
                        SET action = %s, description = %s, status = %s, 
                            created_at = %s, started_at = NULL, completed_at = NULL,
                            imported_rows = 0, total_rows = 0, error_message = NULL
                        WHERE id = %s
                    """, [action, description, 'pending', timezone.now(), import_id])
                    
                    return import_id
            
            # Create new import record
            cursor.execute("""
                INSERT INTO project_data_imports 
                (schema_name, table_name, action, description, status, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
            """, [schema_name, table_name, action, description, 'pending', timezone.now()])
            
            import_id = cursor.fetchone()[0]
            return import_id
    
    def get_import_by_id(self, import_id):
        """Get import record by ID"""
        from django.db import connection
        
        with connection.cursor() as cursor:
            cursor.execute(
                SQL("SET search_path TO {}").format(Identifier(self.schema_name))
            )
            
            cursor.execute("""
                SELECT id, schema_name, table_name, action, status, imported_rows, 
                       total_rows, description, error_message, created_at, started_at, completed_at
                FROM project_data_imports 
                WHERE id = %s
            """, [import_id])
            
            row = cursor.fetchone()
            if row:
                return self._row_to_dict(cursor.description, row)
            return None
    
    def list_imports(self):
        """List all import records for this project"""
        from django.db import connection
        
        with connection.cursor() as cursor:
            cursor.execute(
                SQL("SET search_path TO {}").format(Identifier(self.schema_name))
            )
            
            cursor.execute("""
                SELECT id, schema_name, table_name, action, status, imported_rows, 
                       total_rows, description, error_message, created_at, started_at, completed_at
                FROM project_data_imports 
                ORDER BY created_at DESC
            """)
            
            return [self._row_to_dict(cursor.description, row) for row in cursor.fetchall()]
    
    def update_import_status(self, import_id, status, **kwargs):
        """Update import status and other fields"""
        from django.db import connection
        from django.utils import timezone
        
        with connection.cursor() as cursor:
            cursor.execute(
                SQL("SET search_path TO {}").format(Identifier(self.schema_name))
            )
            
            # Build dynamic update query
            update_fields = ['status = %s']
            values = [status]
            
            if status == 'processing':
                update_fields.append('started_at = %s')
                values.append(timezone.now())
            elif status in ['completed', 'failed']:
                update_fields.append('completed_at = %s')
                values.append(timezone.now())
                
                if 'imported_rows' in kwargs:
                    update_fields.append('imported_rows = %s')
                    values.append(kwargs['imported_rows'])
                
                if 'error_message' in kwargs:
                    update_fields.append('error_message = %s')
                    values.append(kwargs['error_message'])
                
                if 'total_rows' in kwargs:
                    update_fields.append('total_rows = %s')
                    values.append(kwargs['total_rows'])
            
            values.append(import_id)
            
            cursor.execute(f"""
                UPDATE project_data_imports 
                SET {', '.join(update_fields)}
                WHERE id = %s
            """, values)
    
    def delete_import(self, import_id):
        """Delete an import record"""
        from django.db import connection
        
        with connection.cursor() as cursor:
            cursor.execute(
                SQL("SET search_path TO {}").format(Identifier(self.schema_name))
            )
            
            cursor.execute("DELETE FROM project_data_imports WHERE id = %s", [import_id])
            return cursor.rowcount > 0
    
    def _row_to_dict(self, description, row):
        """Convert database row to dictionary"""
        from datetime import datetime
        
        result = {}
        for i, col_desc in enumerate(description):
            col_name = col_desc[0]
            value = row[i]
            
            # Convert timestamps to ISO format
            if isinstance(value, datetime):
                value = value.isoformat()
            
            result[col_name] = value
        
        # Calculate import duration if both timestamps exist
        if result.get('started_at') and result.get('completed_at'):
            try:
                from datetime import datetime
                started = datetime.fromisoformat(result['started_at'].replace('Z', '+00:00') if result['started_at'].endswith('Z') else result['started_at'])
                completed = datetime.fromisoformat(result['completed_at'].replace('Z', '+00:00') if result['completed_at'].endswith('Z') else result['completed_at'])
                result['import_duration'] = (completed - started).total_seconds()
            except:
                result['import_duration'] = None
        else:
            result['import_duration'] = None
        
        return result

class ForestSpecies(models.Model):
    code = models.IntegerField(unique=True)
    species_name = models.CharField(max_length=255)
    species = models.CharField(max_length=255)
    family = models.CharField(max_length=255)
    scientific_name = models.CharField(max_length=255)
    name = models.CharField(max_length=255)
    form = models.CharField(max_length=50)
    nepal = models.CharField(max_length=50)
    altitude = models.CharField(max_length=50)
    local_name = models.CharField(max_length=255)

    class Meta:
        db_table = 'forest_species'
        verbose_name = 'forest_species'
        verbose_name_plural = "forest_species"

    def __str__(self):
        return f"{self.code} - {self.species_name}"

class HDModel(models.Model):
    code = models.IntegerField(unique=True)
    name = models.CharField(max_length=100, unique=True)
    expression = models.TextField()
    description = models.TextField(blank=True, null=True)

    def evaluate_expression(self, diameter, params):
        try:
            d = symbols('d')
            bh, a, b, c = symbols('bh a b c')
            symbol_dict = {
                'd': d,
                'exp': exp,
                'log': log,
                'sqrt': sqrt,
                'bh': bh,
                'a': a,
                'b': b,
                'c': c
            }
            # parese expression
            expr = parse_expr(self.expression, local_dict=symbol_dict)
            # substitute actual values and evalute
            result = float(expr.subs({'d': diameter, **params}).evalf())
            return result
        except SympifyError as e:
            raise ValueError(f"Failed to parse expression: {e}")
        except Exception as e:
            raise ValueError(f"Error evaluating expression: {e}")
        
    def __str__(self):
        return f"{self.code} - {self.name}" 

    class Meta:
        db_table = 'hd_model'
        verbose_name = "hd_model"
        verbose_name_plural = "hd_model"

class SpeciesHDModelMap(models.Model):
    species = models.ForeignKey(
        ForestSpecies, 
        on_delete=models.CASCADE,
        to_field='code',
        db_column='species_code'
    )
    hd_model = models.ForeignKey(
        HDModel,
        on_delete=models.CASCADE,
        to_field='code',
        db_column='hd_model_code'
    )
    physiography = models.ForeignKey(
        Physiography,
        on_delete=models.CASCADE,
        to_field='code',
        db_column='physio_code'
    )
    hd_a = models.FloatField()
    hd_b = models.FloatField(null=True, blank=True)
    hd_c = models.FloatField(null=True, blank=True)

    class Meta:
        db_table = 'species_hd_model_map'
        verbose_name = "species_hd_model_map"
        verbose_name_plural = "species_hd_model_map"
        unique_together = (('species', 'physiography'),)  # One model per species per physiography

    def __str__(self):
        return f"{self.species} - {self.hd_model} ({self.physiography})"

class Allometric(models.Model):
    """Model for allometric equations used in biomass calculations"""
    species = models.ForeignKey(
        ForestSpecies,
        on_delete=models.CASCADE,
        to_field='code',
        db_column='species_code',
        help_text="Species code from forest_species table"
    )
    density = models.FloatField(
        help_text="Wood density (g/cm³)"
    )
    
    # Stem allometric parameters
    stem_a = models.FloatField(
        help_text="Stem allometric parameter 'a'"
    )
    stem_b = models.FloatField(
        help_text="Stem allometric parameter 'b'"
    )
    stem_c = models.FloatField(
        null=True, blank=True,
        help_text="Stem allometric parameter 'c' (optional)"
    )
    
    # Top 10% allometric parameters
    top_10_a = models.FloatField(
        help_text="Top 10% allometric parameter 'a'"
    )
    top_10_b = models.FloatField(
        help_text="Top 10% allometric parameter 'b'"
    )
    
    # Top 20% allometric parameters
    top_20_a = models.FloatField(
        help_text="Top 20% allometric parameter 'a'"
    )
    top_20_b = models.FloatField(
        help_text="Top 20% allometric parameter 'b'"
    )
    
    # Bark stem allometric parameters
    bark_stem_a = models.FloatField(
        help_text="Bark stem allometric parameter 'a'"
    )
    bark_stem_b = models.FloatField(
        help_text="Bark stem allometric parameter 'b'"
    )
    
    # Bark top 10% allometric parameters
    bark_top_10_a = models.FloatField(
        help_text="Bark top 10% allometric parameter 'a'"
    )
    bark_top_10_b = models.FloatField(
        help_text="Bark top 10% allometric parameter 'b'"
    )
    
    # Bark top 20% allometric parameters
    bark_top_20_a = models.FloatField(
        help_text="Bark top 20% allometric parameter 'a'"
    )
    bark_top_20_b = models.FloatField(
        help_text="Bark top 20% allometric parameter 'b'"
    )
    
    # Branch allometric parameters (small, medium, large)
    branch_s = models.FloatField(
        help_text="Small branch allometric parameter"
    )
    branch_m = models.FloatField(
        help_text="Medium branch allometric parameter"
    )
    branch_l = models.FloatField(
        help_text="Large branch allometric parameter"
    )
    
    # Foliage allometric parameters (small, medium, large)
    foliage_s = models.FloatField(
        help_text="Small foliage allometric parameter"
    )
    foliage_m = models.FloatField(
        help_text="Medium foliage allometric parameter"
    )
    foliage_l = models.FloatField(
        help_text="Large foliage allometric parameter"
    )

    class Meta:
        db_table = 'allometric'
        verbose_name = 'Allometric Equation'
        verbose_name_plural = 'Allometric Equations'
        unique_together = (('species',),)  # One allometric model per species

    def __str__(self):
        return f"Allometric - {self.species.species_name} ({self.species.code})"
    