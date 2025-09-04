import pandas as pd
import numpy as np
import math

class ForestInventoryAnalyzer:
    """
    Forest Inventory Analysis - Pure Python Implementation
    Converts R forest inventory analysis to Python for biomass and carbon calculations
    """
    
    def __init__(self, tree_data_file="MRV_tree_data_with_V_ratio.csv", equations_file="Equations.csv"):
        self.tree_data_file = tree_data_file
        self.equations_file = equations_file
        self.data_equation = None
        self.final_table = None
        
    def load_data(self):
        """Load tree data and equations from CSV files"""
        try:
            # Read the main tree data
            nepaltree = pd.read_csv(self.tree_data_file)
            print(f"Loaded tree data with {len(nepaltree)} rows and columns: {list(nepaltree.columns)}")
            
            # Read equations data
            equations = pd.read_csv(self.equations_file)
            print(f"Loaded equations data with {len(equations)} rows and columns: {list(equations.columns)}")
            
            return nepaltree, equations
            
        except FileNotFoundError as e:
            print(f"Error loading data: {e}")
            return None, None
            
    def preprocess_data(self, nepaltree, equations):
        """Clean and preprocess the raw data"""
        # Create working dataframe
        data_all = nepaltree.copy()
        
        # Remove crown class 11 and 12
        data_all = data_all[~data_all['sample_tree_type'].isin([11, 12])]
        print(f"After removing crown classes 11 & 12: {len(data_all)} rows")
        
        # Calculate predicted DBH
        data_all['predbh'] = np.where(
            data_all['crown_class'] == 9,
            np.exp(-0.200345 + 1.002703 * np.log(data_all['dbh'])),
            data_all['dbh']
        )
        
        # Join with equations data
        data_equation = pd.merge(data_all, equations, on='Species_Name', how='left')
        print(f"After joining with equations: {len(data_equation)} rows")
        
        # Remove dead trees and stumps (crown_class < 7)
        data_equation = data_equation[data_equation['crown_class'] < 7]
        print(f"After removing dead trees: {len(data_equation)} rows")
        
        return data_equation
        
    def calculate_expansion_factors(self, data):
        """Calculate expansion factors for per hectare conversion"""
        conditions = [
            data['predbh'] < 10,
            data['predbh'] < 20,
            data['predbh'] < 30,
            data['predbh'] >= 30
        ]
        choices = [198.94, 49.74, 14.15, 7.96]
        data['exp_fa'] = np.select(conditions, choices, default=0)
        return data
        
    def process_height_data(self, data):
        """Clean and process height measurements"""
        # Clean height data
        data['height'] = data['height'].replace('NA', np.nan)
        data['height'] = pd.to_numeric(data['height'], errors='coerce')
        data.loc[data['height'] < 1.3, 'height'] = np.nan
        
        # Calculate USED_HT1 (handling top cut trees)
        data['USED_HT1'] = np.where(
            data['crown_class'] == 6,
            np.where(
                data['Pre_ht'] < data['height'],
                data['height'] * 1.1,
                data['height']
            ),
            np.where(
                data['height'].isna(),
                data['Pre_ht'],
                data['height']
            )
        )
        
        # Use predicted height when measured height is not available
        data['USED_HT'] = np.where(
            data['USED_HT1'].isna(),
            data['Pre_ht'],
            data['USED_HT1']
        )
        
        return data
        
    def calculate_basal_area(self, data):
        """Calculate basal area metrics"""
        # Convert density to per hectare
        data['No_trees_ha'] = data['exp_fa']
        
        # Calculate basal area of individual trees (in square meters)
        data['BA_tree_sqm'] = (math.pi * data['predbh']**2) / 40000
        
        # Calculate basal area per hectare
        data['BA_sqm_ha'] = data['BA_tree_sqm'] * data['No_trees_ha']
        
        return data
        
    def calculate_volume(self, data):
        """Calculate tree and stand volume using allometric equations"""
        # Calculate volume using Sharma and Pukala 1990 equation
        data['volume_cum_tree'] = (
            np.exp(
                data['Stem_a'] + 
                data['stem_b'] * np.log(data['predbh']) + 
                data['Stem_c'] * np.log(data['USED_HT'])
            ) / 1000
        )
        
        # Calculate maximum volume with form factor 0.7
        data['volume_BA_tree'] = (
            data['BA_tree_sqm'] * data['USED_HT'] * 0.7
        )
        
        # Final volume calculation with ratio adjustment
        data['Volume_final_cum_tree'] = np.where(
            data['volume_BA_tree'] > data['volume_cum_tree'],
            data['volume_cum_tree'] * data['volume_ratio'],
            data['volume_BA_tree'] * data['volume_ratio']
        )
        
        # Calculate per hectare volume
        data['Volume_final_cum_ha'] = (
            data['volume_cum_tree'] * data['No_trees_ha']
        )
        
        return data
        
    def calculate_biomass_ratios(self, data):
        """Calculate branch and foliage ratios based on DBH"""
        # Branch ratio calculation
        branch_conditions = [
            data['predbh'] < 10,
            data['predbh'] < 40,
            data['predbh'] < 70
        ]
        branch_choices = [
            data['branch_s'],
            ((data['predbh'] - 10) * data['branch_m'] + 
             (40 - data['predbh']) * data['branch_s']) / 30,
            ((data['predbh'] - 40) * data['branch_l'] + 
             (70 - data['predbh']) * data['branch_m']) / 30
        ]
        data['b_ratio'] = np.select(
            branch_conditions, 
            branch_choices, 
            default=data['branch_l']
        )
        
        # Adjust branch ratio for dead trees (not applicable for MRV since dead trees filtered)
        data['b_ratio_final'] = np.where(
            data['crown_class'] == 7,
            data['b_ratio'] * 0.75,
            np.where(
                data['crown_class'] == 8,
                0,
                data['b_ratio']
            )
        )
        
        # Foliage ratio calculation
        foliage_conditions = [
            data['predbh'] < 10,
            data['predbh'] < 40,
            data['predbh'] < 70
        ]
        foliage_choices = [
            data['foliage_s'],
            ((data['predbh'] - 10) * data['foliage_m'] + 
             (40 - data['predbh']) * data['foliage_s']) / 30,
            ((data['predbh'] - 40) * data['foliage_l'] + 
             (70 - data['predbh']) * data['foliage_m']) / 30
        ]
        data['f_ratio'] = np.select(
            foliage_conditions, 
            foliage_choices, 
            default=data['foliage_l']
        )
        
        # Adjust foliage ratio for dead trees
        data['f_ratio_final'] = np.where(
            data['crown_class'].isin([7, 8]),
            0,
            data['f_ratio']
        )
        
        return data
        
    def calculate_biomass(self, data):
        """Calculate stem, branch, and foliage biomass"""
        # Calculate stem biomass (kg per tree)
        data['stem_kg_tree'] = data['volume_cum_tree'] * data['density']
        
        # Convert to tons per hectare
        data['stem_ton_ha'] = (
            data['stem_kg_tree'] * data['No_trees_ha'] / 1000
        )
        
        # Calculate branch and foliage biomass
        data['branch_kg_tree'] = (
            data['stem_kg_tree'] * data['b_ratio_final']
        )
        data['branch_ton_ha'] = (
            data['branch_kg_tree'] * data['No_trees_ha'] / 1000
        )
        
        data['foliage_kg_tree'] = (
            data['stem_kg_tree'] * data['f_ratio_final']
        )
        data['foliage_ton_ha'] = (
            data['foliage_kg_tree'] * data['No_trees_ha'] / 1000
        )
        
        # Total biomass (air dry)
        data['Total_biom_ad_ton_ha'] = (
            data['stem_ton_ha'] + 
            data['branch_ton_ha'] + 
            data['foliage_ton_ha']
        )
        
        data['total_bio_ad'] = (
            (data['stem_kg_tree'] + 
             data['branch_kg_tree'] + 
             data['foliage_kg_tree']) * 
            data['No_trees_ha'] / 1000
        )
        
        # Convert air dry to oven dry biomass
        data['Total_biom_od_ton_ha'] = data['Total_biom_ad_ton_ha'] / 1.1
        
        # Convert biomass to carbon (47% carbon content)
        data['carbon_ton_ha'] = data['Total_biom_od_ton_ha'] * 0.47
        
        return data
        
    def create_summary_tables(self, data):
        """Create plot-wise summary tables"""
        # Variables of interest for plot-wise summarization
        var_interest = [
            'No_trees_ha', 'BA_sqm_ha', 'Volume_final_cum_ha', 'stem_ton_ha',
            'Total_biom_ad_ton_ha', 'Total_biom_od_ton_ha', 'carbon_ton_ha'
        ]
        
        # Plot-wise summary of final analysis
        new_table_mrv = data.groupby('Plot_id')[var_interest].sum().reset_index()
        
        # Diameter distribution analysis
        var_interest1 = ['dbh', 'USED_HT']
        new_table_mrv1 = data.groupby('Plot_id')[var_interest1].mean().reset_index()
        new_table_mrv1.rename(columns={
            'dbh': 'Mean_Diameter2022',
            'USED_HT': 'Mean_HT_2022'
        }, inplace=True)
        
        # Final summary table
        final_table = pd.merge(new_table_mrv, new_table_mrv1, on='Plot_id', how='left')
        
        return final_table, data
        
    def analyze(self, save_results=True):
        """
        Main analysis function that runs the complete forest inventory analysis
        """
        print("Starting Forest Inventory Analysis...")
        
        # Load data
        nepaltree, equations = self.load_data()
        if nepaltree is None or equations is None:
            return None, None
            
        # Preprocess data
        data = self.preprocess_data(nepaltree, equations)
        
        # Calculate expansion factors
        data = self.calculate_expansion_factors(data)
        
        # Process height data
        data = self.process_height_data(data)
        
        # Calculate basal area
        data = self.calculate_basal_area(data)
        
        # Calculate volume
        data = self.calculate_volume(data)
        
        # Calculate biomass ratios
        data = self.calculate_biomass_ratios(data)
        
        # Calculate biomass
        data = self.calculate_biomass(data)
        
        # Create summary tables
        final_table, detailed_data = self.create_summary_tables(data)
        
        # Store results
        self.data_equation = detailed_data
        self.final_table = final_table
        
        # Save results if requested
        if save_results:
            final_table.to_csv('final_mrv_python.csv', index=False)
            detailed_data.to_csv('mrv_analysis_2022_python.csv', index=False)
            print("Results saved to 'final_mrv_python.csv' and 'mrv_analysis_2022_python.csv'")
        
        print("Analysis completed successfully!")
        print(f"Final table shape: {final_table.shape}")
        print(f"Detailed data shape: {detailed_data.shape}")
        
        return final_table, detailed_data
        
    def get_summary_statistics(self):
        """Get summary statistics from the analysis"""
        if self.final_table is None:
            print("No analysis results available. Run analyze() first.")
            return None
            
        summary = {
            'total_plots': len(self.final_table),
            'avg_carbon_per_ha': self.final_table['carbon_ton_ha'].mean(),
            'total_carbon': self.final_table['carbon_ton_ha'].sum(),
            'max_carbon_per_ha': self.final_table['carbon_ton_ha'].max(),
            'min_carbon_per_ha': self.final_table['carbon_ton_ha'].min(),
            'avg_biomass_per_ha': self.final_table['Total_biom_od_ton_ha'].mean(),
            'total_biomass': self.final_table['Total_biom_od_ton_ha'].sum(),
            'avg_diameter': self.final_table['Mean_Diameter2022'].mean(),
            'avg_height': self.final_table['Mean_HT_2022'].mean(),
            'avg_trees_per_ha': self.final_table['No_trees_ha'].mean(),
            'avg_basal_area_per_ha': self.final_table['BA_sqm_ha'].mean(),
            'avg_volume_per_ha': self.final_table['Volume_final_cum_ha'].mean()
        }
        
        return summary


# Standalone functions for direct use
def analyze_forest_inventory(tree_data_file="MRV_tree_data_with_V_ratio.csv", 
                           equations_file="Equations.csv", 
                           save_results=True):
    """
    Standalone function to run forest inventory analysis
    
    Args:
        tree_data_file (str): Path to tree data CSV file
        equations_file (str): Path to equations CSV file
        save_results (bool): Whether to save results to CSV files
        
    Returns:
        tuple: (final_table, detailed_data) pandas DataFrames
    """
    analyzer = ForestInventoryAnalyzer(tree_data_file, equations_file)
    return analyzer.analyze(save_results=save_results)


def get_forest_summary_stats(final_table):
    """
    Get summary statistics from forest analysis results
    
    Args:
        final_table (pd.DataFrame): Results from forest analysis
        
    Returns:
        dict: Summary statistics
    """
    if final_table is None or final_table.empty:
        return None
        
    summary = {
        'total_plots': len(final_table),
        'avg_carbon_per_ha': final_table['carbon_ton_ha'].mean(),
        'total_carbon': final_table['carbon_ton_ha'].sum(),
        'max_carbon_per_ha': final_table['carbon_ton_ha'].max(),
        'min_carbon_per_ha': final_table['carbon_ton_ha'].min(),
        'avg_biomass_per_ha': final_table['Total_biom_od_ton_ha'].mean(),
        'total_biomass': final_table['Total_biom_od_ton_ha'].sum(),
        'avg_diameter': final_table['Mean_Diameter2022'].mean(),
        'avg_height': final_table['Mean_HT_2022'].mean(),
        'avg_trees_per_ha': final_table['No_trees_ha'].mean(),
        'avg_basal_area_per_ha': final_table['BA_sqm_ha'].mean(),
        'avg_volume_per_ha': final_table['Volume_final_cum_ha'].mean()
    }
    
    return summary


if __name__ == "__main__":
    # Example usage
    print("Running Forest Inventory Analysis...")
    
    # # Method 1: Using the class
    # analyzer = ForestInventoryAnalyzer()
    # final_results, detailed_results = analyzer.analyze()
    
    # if final_results is not None:
    #     summary_stats = analyzer.get_summary_statistics()
    #     print("\nSummary Statistics:")
    #     for key, value in summary_stats.items():
    #         if isinstance(value, float):
    #             print(f"{key}: {value:.2f}")
    #         else:
    #             print(f"{key}: {value}")
    
    # Method 2: Using standalone function
    final_results, detailed_results = analyze_forest_inventory()
    summary_stats = get_forest_summary_stats(final_results)