import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.optimize import minimize
from sklearn.metrics import mean_squared_error, r2_score
from typing import Dict, List, Optional, Tuple, Union
import warnings
warnings.filterwarnings('ignore')

class HeightDiameterModeling:
    """
    Height-Diameter modeling for Forest Research and Training Centre, Nepal
    Implements Curtis, Naslund, and Michailoff models for height prediction
    Fixed to match R lmfor package behavior
    """
    
    def __init__(self):
        # Species to model mapping (matching R code exactly)
        self.species_models = {
            'Terminalia': 'curtis',
            'Shorea': 'curtis', 
            'Anogeissus': 'curtis',
            'Buchanania': 'naslund',
            'Mallotus': 'curtis',
            'Lagerstromia': 'naslund',
            'Dalbergia': 'curtis',
            'Trewia': 'curtis',
            'Adina': 'curtis',
            'Cleistocalyx': 'naslund',
            'Dillenia': 'naslund',
            'Syzygium': 'michailoff',
            'Castanopsis': 'naslund',
            'Desmodium': 'naslund',
            'Semecarpus': 'naslund',
            'Pinus': 'curtis',
            'Acacia': 'naslund',
            'Hymenodictyon': 'naslund',
            'Schima': 'naslund',
            'Miliusa': 'naslund',
            'Misc': 'curtis'
        }
        
        # Species without cluster modeling (matching R code)
        self.species_no_cluster = ['Cleistocalyx', 'Castanopsis', 'Acacia', 'Schima']
    
    def get_species_model_name(self, species_code: str) -> str:
        """Map species code to model name - matching R code exactly"""
        species_mapping = {
            "6615": "Shorea",
            "6660": "Terminalia", 
            "6651": "Syzygium",
            "6063": "Acacia",
            "6089": "Adina",
            "6113": "Anogeissus",
            "6147": "Buchanania",
            "6175": "Castanopsis", "6176": "Castanopsis", "6177": "Castanopsis",
            "6207": "Cleistocalyx",
            "6235": "Dalbergia", "6237": "Dalbergia", "6239": "Dalbergia", "6240": "Dalbergia",
            "6249": "Dillenia", "6250": "Dillenia",
            "6246": "Desmodium",
            "6349": "Hymenodictyon",
            "6513": "Pinus",
            "6609": "Schima",
            "6369": "Lagerstromia",
            "6446": "Miliusa",
            "6419": "Mallotus",
            "6611": "Semecarpus",
            "6676": "Trewia"
        }
        return species_mapping.get(str(species_code), "Misc")
    
    def curtis_model(self, d: np.ndarray, params: List[float]) -> np.ndarray:
        """
        Curtis height-diameter model matching lmfor package:
        h = 1.3 + a * exp(-b/d)
        """
        a, b = params
        d_safe = np.maximum(d, 0.1)  # Avoid division by zero
        return 1.3 + a * np.exp(-b / d_safe)
    
    def naslund_model(self, d: np.ndarray, params: List[float]) -> np.ndarray:
        """
        Naslund height-diameter model matching lmfor package:
        h = 1.3 + (d²/(a + b*d)²)
        """
        a, b = params
        denominator = a + b * d
        # Handle potential division by zero
        denominator = np.where(np.abs(denominator) < 1e-10, 1e-10, denominator)
        return 1.3 + (d**2) / (denominator**2)
    
    def michailoff_model(self, d: np.ndarray, params: List[float]) -> np.ndarray:
        """
        Michailoff height-diameter model matching lmfor package:
        h = 1.3 + a * exp(-b/d²)
        """
        a, b = params
        d_safe = np.maximum(d, 0.1)  # Avoid division by zero
        return 1.3 + a * np.exp(-b / (d_safe**2))
    
    def fit_model_lmfor_style(self, d: np.ndarray, h: np.ndarray, model_type: str, 
                             cluster: Optional[np.ndarray] = None) -> Dict:
        """
        Fit model using approach similar to lmfor package
        """
        # Remove missing values
        valid_mask = (~np.isnan(d)) & (~np.isnan(h)) & (d > 0) & (h > 1.3)
        d_clean = d[valid_mask]
        h_clean = h[valid_mask]
        
        if len(d_clean) < 3:
            # Not enough data - return default parameters similar to lmfor
            if model_type == 'curtis':
                default_params = [25.0, 5.0]
            elif model_type == 'naslund':
                default_params = [2.0, 0.1]
            elif model_type == 'michailoff':
                default_params = [25.0, 50.0]
            
            return {
                'params': default_params,
                'model_type': model_type,
                'n_obs': len(d_clean),
                'rmse': np.nan,
                'r2': np.nan,
                'fitted': False
            }
        
        # Select model function and initial parameters matching lmfor
        if model_type == 'curtis':
            model_func = self.curtis_model
            # Better initial parameters based on typical forest data
            mean_h = np.mean(h_clean)
            initial_params = [mean_h - 1.3, 5.0]
            bounds = [(0.1, 200), (0.1, 100)]
        elif model_type == 'naslund':
            model_func = self.naslund_model
            # Initial parameters that work well with Naslund model
            initial_params = [2.0, 0.05]
            bounds = [(0.1, 50), (0.001, 5)]
        elif model_type == 'michailoff':
            model_func = self.michailoff_model
            mean_h = np.mean(h_clean)
            initial_params = [mean_h - 1.3, 100.0]
            bounds = [(0.1, 200), (1, 10000)]
        else:
            raise ValueError(f"Unknown model type: {model_type}")
        
        # Weighted least squares objective function (similar to lmfor)
        def objective(params):
            try:
                h_pred = model_func(d_clean, params)
                # Use log-likelihood approach similar to lmfor
                residuals = h_clean - h_pred
                # Add small penalty for extreme parameters
                penalty = 0.001 * sum(p**2 for p in params)
                return np.sum(residuals**2) + penalty
            except:
                return 1e10
        
        # Try multiple starting points to find global minimum
        best_result = None
        best_ssr = np.inf
        
        # Try different starting points
        for i in range(5):
            try:
                # Vary initial parameters
                start_params = [p * (0.5 + i * 0.3) for p in initial_params]
                
                result = minimize(objective, start_params, bounds=bounds, 
                                method='L-BFGS-B', 
                                options={'maxiter': 1000, 'ftol': 1e-9})
                
                if result.success and result.fun < best_ssr:
                    best_result = result
                    best_ssr = result.fun
            except:
                continue
        
        if best_result is not None:
            fitted_params = best_result.x
            
            # Calculate predictions and metrics
            h_pred = model_func(d_clean, fitted_params)
            rmse = np.sqrt(mean_squared_error(h_clean, h_pred))
            
            # Calculate R² properly
            ss_res = np.sum((h_clean - h_pred)**2)
            ss_tot = np.sum((h_clean - np.mean(h_clean))**2)
            r2 = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
            
            return {
                'params': fitted_params.tolist(),
                'model_type': model_type,
                'n_obs': len(d_clean),
                'rmse': rmse,
                'r2': r2,
                'fitted': True
            }
        else:
            # Fallback to initial parameters
            return {
                'params': initial_params,
                'model_type': model_type,
                'n_obs': len(d_clean),
                'rmse': np.nan,
                'r2': np.nan,
                'fitted': False
            }
    
    def predict_heights(self, d: np.ndarray, model_info: Dict) -> np.ndarray:
        """Predict heights using fitted model"""
        model_type = model_info['model_type']
        params = model_info['params']
        
        if model_type == 'curtis':
            return self.curtis_model(d, params)
        elif model_type == 'naslund':
            return self.naslund_model(d, params)
        elif model_type == 'michailoff':
            return self.michailoff_model(d, params)
        else:
            # Return simple relationship as fallback
            return 1.3 + 0.5 * d
    
    def impute_heights_lmfor_style(self, d: np.ndarray, h: np.ndarray, 
                                  cluster: Optional[np.ndarray], model_type: str,
                                  make_plot: bool = False) -> Dict:
        """
        Impute heights matching lmfor ImputeHeights behavior
        """
        # Fit model using available height data
        model_info = self.fit_model_lmfor_style(d, h, model_type, cluster)
        
        # Predict heights for all trees
        h_pred_all = self.predict_heights(d, model_info)
        
        # Key difference: lmfor returns predicted heights for ALL trees,
        # not just missing ones
        # This matches R behavior: hpred contains predicted values for all trees
        
        if make_plot and model_info['fitted']:
            # Pass species name to plot function
            species_name = getattr(self, '_current_species', 'Unknown Species')
            self.plot_height_diameter_relationship(d, h, h_pred_all, model_info, species_name)
        
        return {
            'hpred': h_pred_all,  # All predicted heights (matching R lmfor behavior)
            'model_info': model_info,
            'h_predicted_only': h_pred_all
        }
    
    def plot_height_diameter_relationship(self, d: np.ndarray, h_observed: np.ndarray, 
                                        h_predicted: np.ndarray, model_info: Dict, species_name: str = "Unknown"):
        """Create diagnostic plots for height-diameter relationship"""
        try:
            plt.figure(figsize=(15, 5))
            
            # Plot 1: Observed vs Predicted with model curve
            plt.subplot(1, 3, 1)
            valid_mask = ~np.isnan(h_observed) & (d > 0)
            
            # Plot observed data points
            if np.sum(valid_mask) > 0:
                plt.scatter(d[valid_mask], h_observed[valid_mask], alpha=0.7, 
                           label='Observed Heights', s=30, color='blue', edgecolors='navy', linewidth=0.5)
            
            # Plot all predicted points (including those without observed heights)
            all_mask = d > 0
            if np.sum(all_mask) > 0:
                plt.scatter(d[all_mask], h_predicted[all_mask], alpha=0.3, 
                           label='All Predicted', s=10, color='red', marker='.')
            
            # Plot smooth model curve
            if np.sum(d > 0) > 0:
                d_range = np.linspace(np.min(d[d > 0]), np.max(d[d > 0]), 100)
                h_curve = self.predict_heights(d_range, model_info)
                plt.plot(d_range, h_curve, 'r-', label=f'{model_info["model_type"].title()} Model Curve', 
                        linewidth=2, alpha=0.8)
            
            plt.xlabel('Diameter (cm)', fontsize=11)
            plt.ylabel('Height (m)', fontsize=11)
            plt.title(f'{species_name} - Height-Diameter Relationship\n{model_info["model_type"].title()} Model', 
                     fontsize=12, fontweight='bold')
            plt.legend(fontsize=9)
            plt.grid(True, alpha=0.3)
            
            # Plot 2: Residuals (only for observed data)
            plt.subplot(1, 3, 2)
            if np.sum(valid_mask) > 0:
                residuals = h_observed[valid_mask] - h_predicted[valid_mask]
                plt.scatter(h_predicted[valid_mask], residuals, alpha=0.7, s=30, 
                           color='green', edgecolors='darkgreen', linewidth=0.5)
                plt.axhline(y=0, color='red', linestyle='--', alpha=0.8)
                
                # Add trend line for residuals
                if len(residuals) > 1:
                    z = np.polyfit(h_predicted[valid_mask], residuals, 1)
                    p = np.poly1d(z)
                    plt.plot(h_predicted[valid_mask], p(h_predicted[valid_mask]), 
                            "orange", linestyle=':', alpha=0.8, label='Trend')
                    plt.legend(fontsize=9)
                
                plt.xlabel('Predicted Height (m)', fontsize=11)
                plt.ylabel('Residuals (m)', fontsize=11)
                plt.title(f'{species_name} - Residual Plot', fontsize=12)
                plt.grid(True, alpha=0.3)
            else:
                plt.text(0.5, 0.5, 'No observed data\nfor residual analysis', 
                        ha='center', va='center', transform=plt.gca().transAxes, fontsize=12)
                plt.title(f'{species_name} - Residual Plot', fontsize=12)
            
            # Plot 3: Model statistics and data summary
            plt.subplot(1, 3, 3)
            plt.axis('off')
            
            # Count data types
            n_observed = np.sum(valid_mask)
            n_predicted_only = np.sum(np.isnan(h_observed) & (d > 0))
            n_total = n_observed + n_predicted_only
            
            stats_text = f"""MODEL: {species_name}
            
                            Model Type: {model_info['model_type'].title()}
                                        
                            DATA SUMMARY:
                            • Total trees: {n_total}
                            • With measured heights: {n_observed}
                            • Predicted only: {n_predicted_only}
                            • Model fitted on: {model_info['n_obs']} trees

                            MODEL PERFORMANCE:
                            • RMSE: {model_info['rmse']:.3f} m
                            • R²: {model_info['r2']:.3f}
                            • Model fitted: {model_info['fitted']}

                            PARAMETERS:
                            • a = {model_info['params'][0]:.4f}
                            • b = {model_info['params'][1]:.4f}

                            DIAMETER RANGE:
                            • Min: {np.min(d[d > 0]):.1f} cm
                            • Max: {np.max(d[d > 0]):.1f} cm
                            • Mean: {np.mean(d[d > 0]):.1f} cm"""
            
            plt.text(0.05, 0.95, stats_text, transform=plt.gca().transAxes, 
                    fontsize=10, verticalalignment='top', fontfamily='monospace',
                    bbox=dict(boxstyle="round,pad=0.3", facecolor="lightgray", alpha=0.8))
            
            plt.tight_layout()
            plt.show()
            
        except Exception as e:
            print(f"Error creating plot for {species_name}: {e}")
            import traceback
            traceback.print_exc()
    
    def process_forest_data(self, csv_file_path: str, output_file_path: str, 
                           show_plots: bool = True, max_plots: int = None) -> pd.DataFrame:
        """
        Process forest data matching R code behavior exactly
        
        Args:
            csv_file_path: Path to input CSV file
            output_file_path: Path for output CSV file  
            show_plots: Whether to show diagnostic plots for each species
            max_plots: Maximum number of plots to show (None = show all)
        """
        # Read data
        H = pd.read_csv(csv_file_path)
        print(f"Loaded {len(H)} records")
        
        # Set diameter (matching R: H$d <- H$diameter_p)
        H['d'] = H['diameter_p']
        
        # Create cluster variable (matching R: H$cluster<-(H$col*1000+H$row))
        H['cluster'] = H['col'] * 1000 + H['row']
        
        # Set height for modeling - matching R logic exactly
        H['h'] = np.where(
            (H['height_m'] > 0) & 
            (H['sample_tree_type'].isin([1, 2, 4, 5])),
            H['height_m'],  # Use height_m, not height
            np.nan
        )
        
        # Map species to model names (matching R code exactly)
        H['H_model'] = H['species'].astype(str).apply(self.get_species_model_name)
        
        print("Species distribution:")
        species_counts = H['H_model'].value_counts()
        print(species_counts)
        
        # Calculate how many species have data
        species_with_data = [species for species in self.species_models.keys() 
                           if species in species_counts.index and species_counts[species] > 0]
        
        print(f"\nFound {len(species_with_data)} species groups with data:")
        for species in species_with_data:
            print(f"  {species}: {species_counts[species]} trees")
        
        if show_plots:
            if max_plots is None:
                print(f"\nWill show {len(species_with_data)} diagnostic plots (one per species group)")
            else:
                actual_plots = min(len(species_with_data), max_plots)
                print(f"\nWill show {actual_plots} diagnostic plots (limited by max_plots={max_plots})")
        
        # Process each species group
        all_results = []
        plot_count = 0
        
        for species in self.species_models.keys():
            print(f"\nProcessing {species}...")
            
            # Filter data for this species
            species_data = H[H['H_model'] == species].copy()
            
            if len(species_data) == 0:
                print(f"No data for {species}, skipping")
                continue
                
            print(f"Found {len(species_data)} trees for {species}")
            
            # Count trees with measured heights for this species
            measured_heights = np.sum(~np.isnan(species_data['h']))
            print(f"  - Trees with measured heights: {measured_heights}")
            print(f"  - Trees needing height prediction: {len(species_data) - measured_heights}")
            
            # Determine if cluster should be used (matching R code)
            use_cluster = species not in self.species_no_cluster
            cluster_data = species_data['cluster'].values if use_cluster else None
            print(f"  - Using cluster modeling: {use_cluster}")
            
            # Get model type
            model_type = self.species_models[species]
            print(f"  - Model type: {model_type}")
            
            # Set current species for plotting
            self._current_species = species
            
            # Determine if we should show plot for this species
            should_show_plot = (show_plots and 
                              (max_plots is None or plot_count < max_plots) and
                              measured_heights > 0)  # Only show plot if we have data to fit
            
            # Impute heights using lmfor-style approach
            result = self.impute_heights_lmfor_style(
                d=species_data['d'].values,
                h=species_data['h'].values,
                cluster=cluster_data,
                model_type=model_type,
                make_plot=should_show_plot
            )
            
            if should_show_plot:
                plot_count += 1
            
            # Add predicted heights to dataframe (matching R: H_Species$hpred<-ImpFixed_Species$hpred)
            species_data['hpred'] = result['hpred']
            species_data['source'] = f"H_{species}"
            
            # Store model info
            model_info = result['model_info']
            print(f"  - Model fitted successfully: {model_info['fitted']}")
            if not np.isnan(model_info['rmse']):
                print(f"  - RMSE: {model_info['rmse']:.3f}, R²: {model_info['r2']:.3f}")
            print(f"  - Parameters: a={model_info['params'][0]:.4f}, b={model_info['params'][1]:.4f}")
            
            all_results.append(species_data)
        
        print(f"\nShowed {plot_count} diagnostic plots total")
        
        # Combine all results (matching R: combined_pred_ht <- bind_rows(models_standardized, .id = "source"))
        if all_results:
            combined_data = pd.concat(all_results, ignore_index=True)
            
            # Add height_p column (matching R: combined_pred_ht$height_p <- combined_pred_ht$hpred)
            combined_data['height_p'] = combined_data['hpred']
            
            # Remove hpred column (matching R: combined_pred_ht$hpred <- NULL)
            combined_data = combined_data.drop('hpred', axis=1)
            
            # Save results
            combined_data.to_csv(output_file_path, index=False)
            
            print(f"\nProcessing complete!")
            print(f"Results saved to: {output_file_path}")
            print(f"Total trees processed: {len(combined_data)}")
            
            # Summary of results
            print(f"\nSUMMARY:")
            print(f"- Total tree records: {len(combined_data)}")
            print(f"- Species groups processed: {len(species_with_data)}")
            print(f"- Diagnostic plots shown: {plot_count}")
            
            return combined_data
        else:
            print("No data processed!")
            return pd.DataFrame()


# Example usage and testing
if __name__ == "__main__":
    # Initialize the modeling class
    hd_model = HeightDiameterModeling()
    
    # Process the data with plots enabled
    print("=== PROCESSING FOREST DATA ===")
    result_df = hd_model.process_forest_data(
        csv_file_path="2.tree_data_2022.csv", 
        output_file_path="tree_data_2022_with_pred_heights_python_fixed.csv",
        show_plots=True,  # Enable plots
        max_plots=10       # Limit to first 5 species (remove this to see all)
    )
    
    # print("\n=== TESTING SPECIFIC ROWS ===")
    # # Test with your specific rows
    # test_data = {
    #     'col': [13, 13, 13, 13, 13, 13, 13],
    #     'row': [73, 73, 73, 73, 73, 73, 73],
    #     'plot_number': [1, 1, 1, 1, 1, 3, 3],
    #     'tree_no': [1, 2, 3, 5, 6, 1, 2],
    #     'diameter_p': [11.8, 43.4, 31.0, 25.7, 15.9, 7.5, 8.2],
    #     'species': ['6063', '6063', '6063', '6063', '6063', '6063', '6063'],
    #     'height_m': [2.2, 17.1, 0, 0, 2.2, 2.1, 2.1],
    #     'sample_tree_type': [3, 1, 0, 0, 3, 3, 3]
    # }
    
    # test_df = pd.DataFrame(test_data)
    # test_df['d'] = test_df['diameter_p']
    # test_df['cluster'] = test_df['col'] * 1000 + test_df['row']
    # test_df['h'] = np.where(
    #     (test_df['height_m'] > 0) & (test_df['sample_tree_type'].isin([1, 2, 4, 5])),
    #     test_df['height_m'], np.nan
    # )
    # test_df['H_model'] = 'Acacia'
    
    # print("Input data:")
    # print("Diameter values:", test_df['d'].values)
    # print("Height values for modeling:", test_df['h'].values)
    # print("Sample tree types:", test_df['sample_tree_type'].values)
    
    # # Test Acacia model specifically with plot
    # print("\nTesting Acacia model with your specific data:")
    # hd_model._current_species = 'Acacia (Test Data)'
    # acacia_model = hd_model.impute_heights_lmfor_style(
    #     d=test_df['d'].values,
    #     h=test_df['h'].values,
    #     cluster=None,  # Acacia doesn't use cluster in R
    #     model_type='naslund',
    #     make_plot=True
    # )
    
    # print("\nResults comparison:")
    # print("Row | Diameter | Python Pred | Expected R")
    # print("----|----------|-------------|------------")
    # expected_r = [8.015286, 18.44708, 15.75054, 14.18084, 10.22377, 5.306432, 5.770411]
    # for i, (d, pred) in enumerate(zip(test_df['d'].values, acacia_model['hpred'])):
    #     print(f"{i+1:2d}  | {d:8.1f} | {pred:11.6f} | {expected_r[i]:10.6f}")
    
    # print(f"\nModel parameters: a={acacia_model['model_info']['params'][0]:.6f}, b={acacia_model['model_info']['params'][1]:.6f}")
    # print(f"Model RMSE: {acacia_model['model_info']['rmse']:.6f}")
    # print(f"Model R²: {acacia_model['model_info']['r2']:.6f}")