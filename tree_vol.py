import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from typing import List, Tuple, Optional
import math

class ForestBiometrics:
    """
    Forest Biometrics class for handling FRA Nepal data
    Implements Polynomial taper curve by Heinonen et al. for plantation tree species
    """
    
    def __init__(self):
        # Parameters for the Fibonacci curve
        # pre-calibrated coefficients for the Heinonen taper curve model.
        self.a_par = [0, 0, 0]
        self.b_par = [2.05502, -0.89331, -1.50615, 3.47354, -3.10063, 1.50246, -0.05514, 0.00070]
    
    def fibonacci(self, x_m: float, a_par: List[float], b_par: List[float]) -> float:
        """
        Fibonacci function for taper curve calculation

        This method calculates the value of the Fibonacci polynomial at a given relative height position x_m.
        It's a core component of the taper curve model. Notice how the exponents of x_m follow the 
        Fibonacci sequence (1, 2, 3, 5, 8, 13, 21, 34), which is a key feature of this specific model.

        Args:
            x_m: Relative height position (1 - 1.3/height)
            a_par: Correction polynomial parameters (a1-a3)
            b_par: Relative taper curve parameters (b1-b8)
            
        Returns:
            Fibonacci curve value
        """
        pb = ((a_par[0] + b_par[0]) * x_m +
              (a_par[1] + b_par[1]) * x_m**2 +
              (a_par[2] + b_par[2]) * x_m**3 +
              b_par[3] * x_m**5 +
              b_par[4] * x_m**8 +
              b_par[5] * x_m**13 +
              b_par[6] * x_m**21 +
              b_par[7] * x_m**34)
        
        return pb
    
    def d_m_taper(self, d13: float, x_m: float, ht: float, 
                  a_par: List[float], b_par: List[float]) -> float:
        """
        Taper curve function to calculate diameter at any height
        
        It first determines a scaling factor (d_0_2h) using the DBH and the Fibonacci function. 
        This factor normalizes the curve to the DBH, allowing the model to be applied to trees of different sizes. 
        It then applies this scaling factor to the Fibonacci function for the desired height.

        Args:
            d13: Diameter at breast height (1.3m)
            x_m: Relative height position
            ht: Total height
            a_par: Correction polynomial parameters
            b_par: Relative taper curve parameters
            
        Returns:
            Diameter at specified height
        """
        d_0_2h = d13 / self.fibonacci(1 - 1.3/ht, a_par, b_par)
        value = d_0_2h * self.fibonacci(x_m, a_par, b_par)
        return value
    
    def plot_taper_curve(self, d13: float, ht: float) -> None:
        """
        Plot taper curve for a given tree
        
        This method creates a visual representation of the taper curve for a specific tree.
        It calculate the diameter at many small height increments (hl) and then plots these points to show the tree's shape.
        This is useful for visualizing the model's output.

        Args:
            d13: Diameter at breast height
            ht: Total height
        """
        hl = np.arange(0.15, ht, 0.01)
        hl = hl[1:] - 0.01/2
        
        dl = [self.d_m_taper(d13, 1-(h/ht), ht, self.a_par, self.b_par) for h in hl]
        
        plt.figure(figsize=(10, 6))
        plt.scatter(hl, dl, s=1)
        plt.axhline(y=d13, color='red', linestyle='--', label=f'DBH = {d13}')
        plt.axvline(x=1.3, color='red', linestyle='--', label='Breast height = 1.3m')
        plt.xlabel('Height (m)')
        plt.ylabel('Diameter (cm)')
        plt.title('Tree Taper Curve')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.show()
    
    def v_taper(self, d13: float, ht: float, ht_x: float, 
                a_par: List[float], b_par: List[float]) -> float:
        """
        Calculate volume using taper curve integration
        
        This method calculates the volume of a tree up to a specified height (ht_x). 

        It does this by using a numerical integration approach. 

        It calculates the diameter at very small height intervals, treats each interval
          as a small cylinder,  calculates the volume of that cylinder (Volume=Area x height), and 
          then sums up all these small volumes. This is an approximation of the integral of the squared taper curve. 

        The calculation uses the formula for the area of a circle, πr2, where r= d/2.

        Args:
            d13: Diameter at breast height
            ht: Total height
            ht_x: Height to calculate volume up to
            a_par: Correction polynomial parameters
            b_par: Relative taper curve parameters
            
        Returns:
            Volume in cubic meters
        """
        if ht_x == ht:
            hl = np.arange(0.15, ht, 0.01)
            hl = hl[1:] - 0.01/2
            dl = [self.d_m_taper(d13, 1-(h/ht), ht, a_par, b_par) for h in hl]
            # Convert from cm² to m² and calculate volume
            volume = sum([(math.pi * (d/100)**2 / 4) * 0.01 for d in dl])
        elif ht_x < ht:
            hl_x = np.arange(0.15, ht_x, 0.01)
            hl_x = hl_x[1:] - 0.01/2
            dl_x = [self.d_m_taper(d13, 1-(h/ht), ht, a_par, b_par) for h in hl_x]
            # Convert from cm² to m² and calculate volume
            volume = sum([(math.pi * (d/100)**2 / 4) * 0.01 for d in dl_x])
        else:
            volume = 0
            
        return volume
    
    def v_ratio_broken_top_trees(self, d13: float, ht: float, ht_x: float, 
                                crown_class: int, a_par: List[float], 
                                b_par: List[float]) -> float:
        """
        Calculate volume ratio for broken top trees
        
        This method is specifically designed for trees with a broken top (indicated by crown_class=6).
        It calculates the ratio of the volume of the remaining trunk (v_t_actual_height) to the tree's 
            predicted total volume (v_t_height_p). This ratio is a correction factor used to adjust 
            volume estimates for damaged trees.

        Args:
            d13: Diameter at breast height
            ht: Total height (predicted)
            ht_x: Actual height (to broken top)
            crown_class: Crown class
            a_par: Correction polynomial parameters
            b_par: Relative taper curve parameters
            
        Returns:
            Volume ratio
        """
        # Volume up to predicted height
        v_t_height_p = self.v_taper(d13, ht, ht, a_par, b_par)
        # Volume to broken top height
        v_t_actual_height = self.v_taper(d13, ht, ht_x, a_par, b_par)
        
        if v_t_height_p > 0:
            ratio = v_t_actual_height / v_t_height_p
        else:
            ratio = 1
            
        return ratio
    
    def process_forest_data(self, csv_file_path: str, output_file_path: str) -> pd.DataFrame:
        """
        Process forest data CSV file and calculate volume ratios
        
        Args:
            csv_file_path: Path to input CSV file
            output_file_path: Path for output CSV file
            
        Returns:
            Processed DataFrame with volume ratios
        """
        # Read data
        H = pd.read_csv(csv_file_path)
        
        # Create diameter column
        H['d'] = H['diameter_p']
        
        # Create height column with conditional logic
        H['h'] = np.where((H['height_m'] > 0) & (H['crown_class'] < 6), 
                         H['height_m'], H['height_p'])
        
        # Filter broken trees (crown_class == 6)
        H_broken = H[H['crown_class'] == 6].copy()
        
        # Initialize volume_ratio column
        H_broken['volume_ratio'] = np.nan
        
        # Calculate volume ratio for each broken tree
        for i in range(len(H_broken)):
            row = H_broken.iloc[i]
            d13 = row['d']
            h = row['h']
            height_p = row['height_p']
            crown_class = row['crown_class']
            
            # Case 1: measured height is less than predicted height.
            if h < height_p and h > 0:
                H_broken.iloc[i, H_broken.columns.get_loc('volume_ratio')] = \
                    self.v_ratio_broken_top_trees(d13, height_p, h, crown_class, 
                                                 self.a_par, self.b_par)
            
            # Case 2: measured height >= predicted height
            # this is an unusual case for broken tree, so the code assumes the broken top 
            # is acutally slightly above the predicted height and calculates the ratio accordingly.
            # it uses a small scaling factor (h * 1.1) for the predicted height.
            elif h >= height_p:
                H_broken.iloc[i, H_broken.columns.get_loc('volume_ratio')] = \
                    self.v_ratio_broken_top_trees(d13, h * 1.1, h, crown_class, 
                                                 self.a_par, self.b_par)
            
            # Case 3: no measured height
            # It assumes a default broken top height of 90% of the predicted height (height_p * 0.9)
            elif h == 0:
                H_broken.iloc[i, H_broken.columns.get_loc('volume_ratio')] = \
                    self.v_ratio_broken_top_trees(d13, height_p, height_p * 0.9, 
                                                 crown_class, self.a_par, self.b_par)
        
        # Merge data frames
        merge_cols = ['col', 'row', 'plot_number', 'tree_no', 'volume_ratio']
        H_vol_ratio_join = H.merge(H_broken[merge_cols], 
                                  on=['col', 'row', 'plot_number', 'tree_no'], 
                                  how='left')
        
        # Set volume_ratio = 1 for non-broken trees
        H_vol_ratio_join['volume_ratio'] = H_vol_ratio_join['volume_ratio'].fillna(1)
        
        # Save results
        H_vol_ratio_join.to_csv(output_file_path, index=False)
        
        print(f"Processing complete. Results saved to {output_file_path}")
        print(f"Number of trees with volume_ratio = 1: {(H_vol_ratio_join['volume_ratio'] == 1).sum()}")
        print(f"Number of broken trees processed: {len(H_broken)}")
        
        return H_vol_ratio_join


# Example usage and testing
if __name__ == "__main__":
    # Initialize the forest biometrics class
    fb = ForestBiometrics()
    
    # # Test with sample tree data
    # print("Testing Fibonacci taper curve functions...")
    
    # # Test parameters
    # ht = 20  # height in meters
    # d13 = 25.4  # diameter at breast height in cm
    # x_m = 1 - 1.3/ht
    
    # # Test Fibonacci function
    # fib_value = fb.fibonacci(x_m, fb.a_par, fb.b_par)
    # print(f"Fibonacci value: {fib_value}")
    
    # # Test taper function
    # diameter_at_height = fb.d_m_taper(d13, x_m, ht, fb.a_par, fb.b_par)
    # print(f"Diameter at relative height {x_m}: {diameter_at_height:.2f} cm")
    
    # # Test volume calculation
    # volume = fb.v_taper(d13, ht, ht, fb.a_par, fb.b_par)
    # print(f"Total volume: {volume:.4f} m³")
    
    # # Test volume ratio for broken tree
    # ht_broken = 15  # broken at 15m
    # volume_ratio = fb.v_ratio_broken_top_trees(d13, ht, ht_broken, 6, fb.a_par, fb.b_par)
    # print(f"Volume ratio for broken tree: {volume_ratio:.4f}")
    
    # # Uncomment to plot taper curve
    # fb.plot_taper_curve(d13, ht)
    
    # To process actual data file:
    df_result = fb.process_forest_data("3.tree_data_2022.csv", "3.tree_data_2022_with_vol_ratio.csv")