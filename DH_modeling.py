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
    """
    
    def __init__(self):
        # Species to model mapping
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
        
        # Species without cluster modeling
        self.species_no_cluster = ['Cleistocalyx', 'Castanopsis', 'Acacia', 'Schima']
    
    def get_species_model_name(self, species_code: str) -> str:
        """Map species code to model name"""
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
        Curtis height-diameter model: h = 1.3 + a * exp(-b/d)
        
        Args:
            d: Diameter values
            params: [a, b] parameters
            
        Returns:
            Predicted heights
        """
        a, b = params
        return 1.3 + a * np.exp(-b / np.maximum(d, 0.1))  # Avoid division by zero
    
    def naslund_model(self, d: np.ndarray, params: List[float]) -> np.ndarray:
        """
        Naslund height-diameter model: h = 1.3 + (d²/(a + b*d)²)
        
        Args:
            d: Diameter values
            params: [a, b] parameters
            
        Returns:
            Predicted heights
        """
        a, b = params
        denominator = np.maximum(a + b * d, 0.1)  # Avoid division by zero
        return 1.3 + (d**2) / (denominator**2)
    
    def michailoff_model(self, d: np.ndarray, params: List[float]) -> np.ndarray:
        """
        Michailoff height-diameter model: h = 1.3 + a * exp(-b/d²)
        
        Args:
            d: Diameter values
            params: [a, b] parameters
            
        Returns:
            Predicted heights
        """
        a, b = params
        return 1.3 + a * np.exp(-b / np.maximum(d**2, 0.01))  # Avoid division by zero
    
    def fit_model(self, d: np.ndarray, h: np.ndarray, model_type: str, 
                  cluster: Optional[np.ndarray] = None) -> Dict:
        """
        Fit height-diameter model to data
        
        Args:
            d: Diameter data
            h: Height data
            model_type: 'curtis', 'naslund', or 'michailoff'
            cluster: Cluster information (optional)
            
        Returns:
            Dictionary with fitted parameters and model info
        """
        # Remove missing values
        valid_mask = (~np.isnan(d)) & (~np.isnan(h)) & (d > 0) & (h > 1.3)
        d_clean = d[valid_mask]
        h_clean = h[valid_mask]
        
        if len(d_clean) < 3:
            # Not enough data for fitting
            return {
                'params': [20.0, 5.0],  # Default parameters
                'model_type': model_type,
                'n_obs': len(d_clean),
                'rmse': np.nan,
                'r2': np.nan,
                'fitted': False
            }
        
        # Select model function
        if model_type == 'curtis':
            model_func = self.curtis_model
            initial_params = [20.0, 5.0]
            bounds = [(0.1, 100), (0.1, 50)]
        elif model_type == 'naslund':
            model_func = self.naslund_model
            initial_params = [2.0, 0.1]
            bounds = [(0.1, 20), (0.01, 2)]
        elif model_type == 'michailoff':
            model_func = self.michailoff_model
            initial_params = [20.0, 50.0]
            bounds = [(0.1, 100), (0.1, 1000)]
        else:
            raise ValueError(f"Unknown model type: {model_type}")
        
        # Objective function
        def objective(params):
            try:
                h_pred = model_func(d_clean, params)
                return np.sum((h_clean - h_pred)**2)
            except:
                return 1e6
        
        # Fit model
        try:
            result = minimize(objective, initial_params, bounds=bounds, method='L-BFGS-B')
            fitted_params = result.x
            
            # Calculate predictions and metrics
            h_pred = model_func(d_clean, fitted_params)
            rmse = np.sqrt(mean_squared_error(h_clean, h_pred))
            r2 = r2_score(h_clean, h_pred)
            
            return {
                'params': fitted_params.tolist(),
                'model_type': model_type,
                'n_obs': len(d_clean),
                'rmse': rmse,
                'r2': r2,
                'fitted': True
            }
            
        except Exception as e:
            print(f"Error fitting {model_type} model: {e}")
            return {
                'params': initial_params,
                'model_type': model_type,
                'n_obs': len(d_clean),
                'rmse': np.nan,
                'r2': np.nan,
                'fitted': False
            }
    
    def predict_heights(self, d: np.ndarray, model_info: Dict) -> np.ndarray:
        """
        Predict heights using fitted model
        
        Args:
            d: Diameter values
            model_info: Model information from fit_model
            
        Returns:
            Predicted heights
        """
        model_type = model_info['model_type']
        params = model_info['params']
        
        if model_type == 'curtis':
            return self.curtis_model(d, params)
        elif model_type == 'naslund':
            return self.naslund_model(d, params)
        elif model_type == 'michailoff':
            return self.michailoff_model(d, params)
        else:
            # Return simple linear relationship as fallback
            return 1.3 + 0.5 * d
    
    def impute_heights(self, d: np.ndarray, h: np.ndarray, 
                      cluster: Optional[np.ndarray], model_type: str,
                      make_plot: bool = False) -> Dict:
        """
        Impute missing heights using height-diameter models
        
        Args:
            d: Diameter data
            h: Height data (with missing values)
            cluster: Cluster information
            model_type: Model type to use
            make_plot: Whether to create diagnostic plots
            
        Returns:
            Dictionary with predicted heights and model info
        """
        # Fit model using available height data
        model_info = self.fit_model(d, h, model_type, cluster)
        
        # Predict heights for all trees
        h_pred = self.predict_heights(d, model_info)
        
        # Use measured heights where available, predicted otherwise
        h_final = np.where(np.isnan(h), h_pred, h)
        
        if make_plot and model_info['fitted']:
            self.plot_height_diameter_relationship(d, h, h_pred, model_info)
        
        return {
            'hpred': h_final,
            'model_info': model_info,
            'h_predicted_only': h_pred
        }
    
    def plot_height_diameter_relationship(self, d: np.ndarray, h_observed: np.ndarray, 
                                        h_predicted: np.ndarray, model_info: Dict):
        """
        Create diagnostic plots for height-diameter relationship
        
        Args:
            d: Diameter data  
            h_observed: Observed heights
            h_predicted: Predicted heights
            model_info: Model information
        """
        plt.figure(figsize=(12, 4))
        
        # Plot 1: Observed vs Predicted
        plt.subplot(1, 3, 1)
        valid_mask = ~np.isnan(h_observed)
        if np.sum(valid_mask) > 0:
            plt.scatter(d[valid_mask], h_observed[valid_mask], alpha=0.6, label='Observed', s=20)
        
        # Plot model curve
        d_range = np.linspace(np.nanmin(d), np.nanmax(d), 100)
        h_curve = self.predict_heights(d_range, model_info)
        plt.plot(d_range, h_curve, 'r-', label=f'{model_info["model_type"].title()} model', linewidth=2)
        
        plt.xlabel('Diameter (cm)')
        plt.ylabel('Height (m)')
        plt.title(f'Height-Diameter Relationship\n{model_info["model_type"].title()} Model')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # Plot 2: Residuals
        plt.subplot(1, 3, 2)
        if np.sum(valid_mask) > 0:
            residuals = h_observed[valid_mask] - h_predicted[valid_mask]
            plt.scatter(h_predicted[valid_mask], residuals, alpha=0.6, s=20)
            plt.axhline(y=0, color='r', linestyle='--')
            plt.xlabel('Predicted Height (m)')
            plt.ylabel('Residuals (m)')
            plt.title('Residual Plot')
            plt.grid(True, alpha=0.3)
        
        # Plot 3: Model statistics
        plt.subplot(1, 3, 3)
        plt.axis('off')
        stats_text = f"""Model Statistics:
        
Type: {model_info['model_type'].title()}
N observations: {model_info['n_obs']}
RMSE: {model_info['rmse']:.2f} m
R²: {model_info['r2']:.3f}
Parameters: {[f'{p:.3f}' for p in model_info['params']]}
Fitted: {model_info['fitted']}"""
        
        plt.text(0.1, 0.9, stats_text, transform=plt.gca().transAxes, 
                fontsize=10, verticalalignment='top', fontfamily='monospace')
        
        plt.tight_layout()
        plt.show()
    
    def process_forest_data(self, csv_file_path: str, output_file_path: str) -> pd.DataFrame:
        """
        Process forest data and predict heights for all species
        
        Args:
            csv_file_path: Path to input CSV file
            output_file_path: Path for output CSV file
            
        Returns:
            Processed DataFrame with predicted heights
        """
        # Read data
        H = pd.read_csv(csv_file_path)
        print(f"Loaded {len(H)} records")
        
        # Filter out crown_class 10
        H = H[H['crown_class'] != 10].copy()
        print(f"After filtering crown_class != 10: {len(H)} records")
        
        # Set diameter
        H['d'] = H['diameter_p']
        
        # Create cluster variable
        H['cluster'] = H['col'] * 1000 + H['row']
        
        # Set height for modeling (only measured heights for specific sample types)
        H['h'] = np.where(
            (H['height_m'] > 0) & (H['sample_tree_type'].isin([1, 2, 4, 5])),
            H['height_m'], 
            np.nan
        )
        
        # Map species to model names
        H['H_model'] = H['species'].astype(str).apply(self.get_species_model_name)
        
        print("Species distribution:")
        print(H['H_model'].value_counts())
        
        # Process each species group
        all_results = []
        
        for species in self.species_models.keys():
            print(f"\nProcessing {species}...")
            
            # Filter data for this species
            species_data = H[H['H_model'] == species].copy()
            
            if len(species_data) == 0:
                print(f"No data for {species}, skipping")
                continue
                
            print(f"Found {len(species_data)} trees for {species}")
            
            # Determine if cluster should be used
            use_cluster = species not in self.species_no_cluster
            cluster_data = species_data['cluster'].values if use_cluster else None
            
            # Get model type
            model_type = self.species_models[species]
            
            # Impute heights
            result = self.impute_heights(
                d=species_data['d'].values,
                h=species_data['h'].values,
                cluster=cluster_data,
                model_type=model_type,
                make_plot=True
            )
            
            # Add predicted heights to dataframe
            species_data['hpred'] = result['hpred']
            species_data['source'] = f"H_{species}"
            
            # Store model info
            model_info = result['model_info']
            print(f"Model fitted: {model_info['fitted']}")
            print(f"RMSE: {model_info['rmse']:.3f}, R²: {model_info['r2']:.3f}")
            print(f"Parameters: {model_info['params']}")
            
            all_results.append(species_data)
        
        # Combine all results
        if all_results:
            combined_data = pd.concat(all_results, ignore_index=True)
            
            # Add height_p column
            combined_data['height_p'] = combined_data['hpred']
            
            # Drop temporary hpred column
            combined_data = combined_data.drop('hpred', axis=1)
            
            # Save results
            combined_data.to_csv(output_file_path, index=False)
            
            print(f"\nProcessing complete!")
            print(f"Results saved to: {output_file_path}")
            print(f"Total trees processed: {len(combined_data)}")
            
            return combined_data
        else:
            print("No data processed!")
            return pd.DataFrame()


# Example usage and testing
if __name__ == "__main__":
    # Initialize the modeling class
    hd_model = HeightDiameterModeling()
    
    # # Test individual models with sample data
    # print("Testing height-diameter models...")
    
    # # Generate sample data
    # np.random.seed(42)
    # d_test = np.random.uniform(5, 50, 100)
    
    # # Test Curtis model
    # h_curtis = hd_model.curtis_model(d_test, [20.0, 5.0])
    # h_curtis_noisy = h_curtis + np.random.normal(0, 2, len(h_curtis))
    
    # print("\n--- Testing Curtis Model ---")
    # curtis_result = hd_model.fit_model(d_test, h_curtis_noisy, 'curtis')
    # print(f"Curtis model fitted: {curtis_result['fitted']}")
    # print(f"Parameters: {curtis_result['params']}")
    # print(f"RMSE: {curtis_result['rmse']:.3f}")
    # print(f"R²: {curtis_result['r2']:.3f}")
    
    # # Test Naslund model
    # h_naslund = hd_model.naslund_model(d_test, [2.0, 0.1])
    # h_naslund_noisy = h_naslund + np.random.normal(0, 2, len(h_naslund))
    
    # print("\n--- Testing Naslund Model ---")
    # naslund_result = hd_model.fit_model(d_test, h_naslund_noisy, 'naslund')
    # print(f"Naslund model fitted: {naslund_result['fitted']}")
    # print(f"Parameters: {naslund_result['params']}")
    # print(f"RMSE: {naslund_result['rmse']:.3f}")
    # print(f"R²: {naslund_result['r2']:.3f}")
    
    # # Test Michailoff model
    # h_michailoff = hd_model.michailoff_model(d_test, [20.0, 50.0])
    # h_michailoff_noisy = h_michailoff + np.random.normal(0, 2, len(h_michailoff))
    
    # print("\n--- Testing Michailoff Model ---")
    # michailoff_result = hd_model.fit_model(d_test, h_michailoff_noisy, 'michailoff')
    # print(f"Michailoff model fitted: {michailoff_result['fitted']}")
    # print(f"Parameters: {michailoff_result['params']}")
    # print(f"RMSE: {michailoff_result['rmse']:.3f}")
    # print(f"R²: {michailoff_result['r2']:.3f}")
    
    # # Test species mapping
    # print("\n--- Testing Species Mapping ---")
    # test_species = ["6615", "6660", "6651", "6513", "unknown"]
    # for species in test_species:
    #     model_name = hd_model.get_species_model_name(species)
    #     model_type = hd_model.species_models.get(model_name, 'unknown')
    #     print(f"Species {species} -> {model_name} -> {model_type}")
    
    # print("\n--- Ready to process real data ---")
    # print("Use: hd_model.process_forest_data('input.csv', 'output.csv')")
    
    # Uncomment to process actual data:
    result_df = hd_model.process_forest_data("2.tree_data_2022.csv", 
                                            "tree_data_2022_with_pred_heights_python.csv")