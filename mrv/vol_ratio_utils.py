"""
Volume Ratio Calculation Utilities

This module provides functions for calculating volume ratios for broken trees
using Fibonacci taper functions. It's used in the forest biometric analysis
system for determining the volume ratio of trees with broken tops.
"""

import math

# Parameters from the R script for Fibonacci taper function
a_par = [0, 0, 0]
b_par = [2.05502, -0.89331, -1.50615, 3.47354, -3.10063, 1.50246, -0.05514, 0.00070]


def fibonacci(x_m, a_par, b_par):
    """
    Calculates the relative diameter at a given relative height using the Fibonacci taper function.
    
    Parameters:
    x_m (float): Relative height (1 - height_above_ground/total_height)
    a_par (list): Correction parameters [a1, a2, a3]
    b_par (list): Fibonacci parameters [b1, b2, b3, b4, b5, b6, b7, b8]
    
    Returns:
    float: Relative diameter at the given height
    """
    Pb = ((a_par[0] + b_par[0]) * x_m +
          (a_par[1] + b_par[1]) * (x_m ** 2) +
          (a_par[2] + b_par[2]) * (x_m ** 3) +
          b_par[3] * (x_m ** 5) +
          b_par[4] * (x_m ** 8) +
          b_par[5] * (x_m ** 13) +
          b_par[6] * (x_m ** 21) +
          b_par[7] * (x_m ** 34))
    
    return Pb


def d_m_taper(d13, x_m, ht, a_par, b_par):
    """
    Calculates the diameter at a specific height using the taper function.
    
    Parameters:
    d13 (float): Diameter at breast height (1.3m) in cm
    x_m (float): Relative height (1 - height_above_ground/total_height)
    ht (float): Total tree height in meters
    a_par (list): Correction parameters
    b_par (list): Fibonacci parameters
    
    Returns:
    float: Diameter at the specified height in cm
    """
    # Calculate diameter at 20% of height (d.0.2h)
    d_0_2h = d13 / fibonacci(1 - 1.3/ht, a_par, b_par)
    
    # Calculate diameter at the specified relative height
    diameter = d_0_2h * fibonacci(x_m, a_par, b_par)
    
    return diameter


def v_taper(d13, ht, ht_x, a_par, b_par, step=0.01):
    """
    Calculates stem volume from stump height (0.15m) to a specified height using numerical integration.
    
    Parameters:
    d13 (float): Diameter at breast height (1.3m) in cm
    ht (float): Total tree height in meters (used for taper curve)
    ht_x (float): Height to which volume should be calculated (cut height)
    a_par (list): Correction parameters
    b_par (list): Fibonacci parameters
    step (float): Integration step size in meters (default: 0.01m = 1cm)
    
    Returns:
    float: Volume in cubic meters (m³)
    """
    volume = 0.0
    
    # Create height intervals from 0.15m to ht_x
    heights = []
    current_height = 0.15
    
    while current_height < ht_x:
        heights.append(current_height)
        current_height += step
    
    # Add the final point if needed
    if len(heights) == 0 or heights[-1] < ht_x:
        heights.append(ht_x)
    
    # Calculate volume for each segment
    for i in range(len(heights)):
        h = heights[i]
        
        # Calculate relative height
        x_m = 1 - h/ht
        
        # Calculate diameter at this height
        diameter_cm = d_m_taper(d13, x_m, ht, a_par, b_par)
        diameter_m = diameter_cm / 100  # Convert cm to meters
        
        # Calculate cross-sectional area in m²
        area = math.pi * (diameter_m ** 2) / 4
        
        # Calculate segment length (for all but last segment, use step size)
        if i < len(heights) - 1:
            segment_length = step
        else:
            segment_length = ht_x - heights[i-1] if i > 0 else ht_x - 0.15
        
        # Add segment volume (m³)
        volume += area * segment_length
    
    return volume


def v_ratio_broken_top_trees(d13, ht, ht_x, crown_class, a_par, b_par):
    """
    Calculates volume ratio for broken top trees.
    
    This function implements the volume ratio calculation logic for broken trees:
    - Case 1: height_m < height_p (normal broken tree)
    - Case 2: height_m >= height_p (unusual case with error handling)
    - Case 3: height_m == 0 (no measured height, assumes 90% break point)
    - Non-broken trees: volume ratio = 1
    
    Parameters:
    d13 (float): Diameter at breast height (1.3m) in cm
    ht (float): Predicted total height in meters
    ht_x (float): Actual measured height in meters
    crown_class (int): Crown class code (6 = broken tree)
    a_par (list): Correction parameters
    b_par (list): Fibonacci parameters
    
    Returns:
    float: Volume ratio (0-1)
    """
    # For non-broken trees, return 1
    if crown_class != 6:
        return 1.0
    
    # Handle the three cases for broken trees
    if ht_x is None or ht_x <= 0:
        # Case 3: No measured height (height_m == 0)
        # Use predicted height for full tree volume
        v_tot = v_taper(d13, ht, ht, a_par, b_par)
        # Assume tree broke at 90% of predicted height
        assumed_broken_height = ht * 0.9
        v_cut = v_taper(d13, ht, assumed_broken_height, a_par, b_par)
        
    elif ht_x < ht:
        # Case 1: measured height < predicted height (height_m < height_p)
        # Use predicted height for full tree volume
        v_tot = v_taper(d13, ht, ht, a_par, b_par)
        # Use measured height for broken tree volume
        v_cut = v_taper(d13, ht, ht_x, a_par, b_par)
        
    else:
        # Case 2: measured height >= predicted height (height_m >= height_p)
        # This is unusual - assume true unbroken height was 10% taller than measured
        assumed_full_height = ht_x * 1.1
        # Use assumed full height for full tree volume
        v_tot = v_taper(d13, assumed_full_height, assumed_full_height, a_par, b_par)
        # Use measured height for broken tree volume
        v_cut = v_taper(d13, assumed_full_height, ht_x, a_par, b_par)
    
    # Calculate ratio
    ratio = v_cut / v_tot if v_tot > 0 else 1.0
    
    return ratio
