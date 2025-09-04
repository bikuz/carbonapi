from mrv.models import HeightDiameterModel, TreeData

import numpy as np
import pandas as pd
from django.conf import settings
import os

class HeightDiameterService:
    """Service for height-diameter modeling in Django"""
    
    def __init__(self):
        self.hd_model = HeightDiameterModel()
    
    def fit_species_models(self, force_refit=False):
        """
        Fit height-diameter models for all species in the database
        
        Args:
            force_refit: Whether to refit even if models already exist
            
        Returns:
            Dictionary with fitting results
        """
        results = {}
        
        # Get all species with height measurements
        species_data = TreeData.objects.filter(
            crown_class__lt=6,
            height_m__gt=0,
            sample_tree_type__in=[1, 2, 4, 5]
        ).exclude(species_model_name__isnull=True)
        
        # Group by species
        species_groups = {}
        for tree in species_data:
            species_name = tree.species_model_name
            if species_name not in species_groups:
                species_groups[species_name] = []
            species_groups[species_name].append(tree)
        
        print(f"Found {len(species_groups)} species groups to process")
        
        for species_name, trees in species_groups.items():
            print(f"\nProcessing {species_name} ({len(trees)} trees)...")
            
            # Check if model already exists
            existing_model = HeightDiameterModel.objects.filter(species_name=species_name).first()
            if existing_model and not force_refit:
                print(f"Model for {species_name} already exists, skipping (use force_refit=True to override)")
                results[species_name] = {'status': 'skipped', 'model': existing_model}
                continue
            
            # Prepare data
            d_values = np.array([tree.d for tree in trees])
            h_values = np.array([tree.height_m for tree in trees])
            
            # Get model type for this species
            model_type = self.hd_model.species_models.get(species_name, 'curtis')
            
            # Determine cluster usage
            use_cluster = species_name not in self.hd_model.species_no_cluster
            cluster_values = None
            if use_cluster:
                cluster_values = np.array([tree.col * 1000 + tree.row for tree in trees])
            
            # Fit model
            model_info = self.hd_model.fit_model(d_values, h_values, model_type, cluster_values)
            
            # Save or update model
            if existing_model:
                hd_model_obj = existing_model
            else:
                hd_model_obj = HeightDiameterModel(species_name=species_name)
            
            hd_model_obj.model_type = model_type
            hd_model_obj.parameters = model_info['params']
            hd_model_obj.n_observations = model_info['n_obs']
            hd_model_obj.rmse = model_info['rmse'] if not np.isnan(model_info['rmse']) else 0
            hd_model_obj.r_squared = model_info['r2'] if not np.isnan(model_info['r2']) else 0
            hd_model_obj.fitted_successfully = model_info['fitted']
            hd_model_obj.save()
            
            results[species_name] = {
                'status': 'fitted' if model_info['fitted'] else 'failed',
                'model': hd_model_obj,
                'model_info': model_info
            }
            
            print(f"Model fitted: {model_info['fitted']}, RMSE: {model_info['rmse']:.3f}")
        
        return results
    
    def predict_heights_for_all_trees(self):
        """
        Predict heights for all trees in the database using fitted models
        
        Returns:
            Number of trees updated
        """
        updated_count = 0
        
        # Get all fitted models
        fitted_models = HeightDiameterModel.objects.filter(fitted_successfully=True)
        model_dict = {model.species_name: model for model in fitted_models}
        
        # Process trees by species
        for species_name, model in model_dict.items():
            trees = TreeData.objects.filter(species_model_name=species_name)
            
            if not trees.exists():
                continue
            
            print(f"Predicting heights for {species_name} ({trees.count()} trees)...")
            
            # Get model parameters
            model_info = {
                'model_type': model.model_type,
                'params': model.get_parameters(),
                'fitted': True
            }
            
            # Predict heights for all trees of this species
            for tree in trees:
                if tree.d and tree.d > 0:
                    h_pred = self.hd_model.predict_heights(np.array([tree.d]), model_info)[0]
                    
                    # Use measured height if available and tree meets criteria
                    if (tree.height_m and tree.height_m > 0 and 
                        tree.crown_class < 6 and tree.sample_tree_type in [1, 2, 4, 5]):
                        tree.height_predicted = tree.height_m
                    else:
                        tree.height_predicted = h_pred
                    
                    tree.save()
                    updated_count += 1
        
        return updated_count
    
    def export_height_diameter_models(self):
        """
        Export fitted models to DataFrame for analysis
        
        Returns:
            DataFrame with model information
        """
        models = HeightDiameterModel.objects.all()
        
        data = []
        for model in models:
            data.append({
                'species_name': model.species_name,
                'model_type': model.model_type,
                'parameters': model.get_parameters(),
                'n_observations': model.n_observations,
                'rmse': model.rmse,
                'r_squared': model.r_squared,
                'fitted_successfully': model.fitted_successfully,
                'created_at': model.created_at,
                'updated_at': model.updated_at
            })
        
        return pd.DataFrame(data)
    
    def get_species_statistics(self):
        """
        Get statistics for each species group
        
        Returns:
            DataFrame with species statistics
        """
        stats = []
        
        for species_name in self.hd_model.species_models.keys():
            trees = TreeData.objects.filter(species_model_name=species_name)
            trees_with_height = trees.filter(height_m__gt=0)
            
            model = HeightDiameterModel.objects.filter(species_name=species_name).first()
            
            stats.append({
                'species_name': species_name,
                'total_trees': trees.count(),
                'trees_with_measured_height': trees_with_height.count(),
                'model_type': self.hd_model.species_models[species_name],
                'model_fitted': model.fitted_successfully if model else False,
                'model_rmse': model.rmse if model else None,
                'model_r2': model.r_squared if model else None
            })
        
        return pd.DataFrame(stats)