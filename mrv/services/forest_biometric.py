import math
import numpy as np
from typing import List
 
from mrv.models import TreeData


class ForestBiometricsService:
    """Service class for forest biometrics calculations"""
    
    def __init__(self):
        # Fibonacci curve parameters
        self.a_par = [0, 0, 0]
        self.b_par = [2.05502, -0.89331, -1.50615, 3.47354, -3.10063, 1.50246, -0.05514, 0.00070]
    
    def fibonacci(self, x_m: float, a_par: List[float], b_par: List[float]) -> float:
        """Fibonacci function for taper curve calculation"""
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
        """Calculate diameter at any height using taper curve"""
        d_0_2h = d13 / self.fibonacci(1 - 1.3/ht, a_par, b_par)
        return d_0_2h * self.fibonacci(x_m, a_par, b_par)
    
    def v_taper(self, d13: float, ht: float, ht_x: float, 
                a_par: List[float], b_par: List[float]) -> float:
        """Calculate volume using taper curve integration"""
        if ht_x == ht:
            hl = np.arange(0.15, ht, 0.01)
            hl = hl[1:] - 0.01/2
            dl = [self.d_m_taper(d13, 1-(h/ht), ht, a_par, b_par) for h in hl]
            volume = sum([(math.pi * (d/100)**2 / 4) * 0.01 for d in dl])
        elif ht_x < ht:
            hl_x = np.arange(0.15, ht_x, 0.01)
            hl_x = hl_x[1:] - 0.01/2
            dl_x = [self.d_m_taper(d13, 1-(h/ht), ht, a_par, b_par) for h in hl_x]
            volume = sum([(math.pi * (d/100)**2 / 4) * 0.01 for d in dl_x])
        else:
            volume = 0
        return volume
    
    def calculate_volume_ratio(self, tree: TreeData) -> float:
        """Calculate volume ratio for a single tree"""
        if tree.crown_class != 6:
            return 1.0
        
        d13 = tree.d
        h = tree.h
        height_p = tree.height_p
        
        # Case 1: measured height below modeled
        if h < height_p and h > 0:
            v_t_height_p = self.v_taper(d13, height_p, height_p, self.a_par, self.b_par)
            v_t_actual = self.v_taper(d13, height_p, h, self.a_par, self.b_par)
        
        # Case 2: measured height >= predicted height
        elif h >= height_p:
            adjusted_height = h * 1.1
            v_t_height_p = self.v_taper(d13, adjusted_height, adjusted_height, self.a_par, self.b_par)
            v_t_actual = self.v_taper(d13, adjusted_height, h, self.a_par, self.b_par)
        
        # Case 3: no measured height
        elif h == 0:
            v_t_height_p = self.v_taper(d13, height_p, height_p, self.a_par, self.b_par)
            v_t_actual = self.v_taper(d13, height_p, height_p * 0.9, self.a_par, self.b_par)
        
        else:
            return 1.0
        
        if v_t_height_p > 0:
            return v_t_actual / v_t_height_p
        return 1.0
    
    def process_broken_trees(self) -> int:
        """Process all broken trees and update their volume ratios"""
        broken_trees = TreeData.objects.filter(crown_class=6)
        updated_count = 0
        
        for tree in broken_trees:
            volume_ratio = self.calculate_volume_ratio(tree)
            tree.volume_ratio = volume_ratio
            tree.save()
            updated_count += 1
        
        return updated_count