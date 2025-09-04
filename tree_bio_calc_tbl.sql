CREATE TABLE tree_biometric_calc (
    -- Primary Key and Identification
    calc_id BIGSERIAL PRIMARY KEY,
    
    -- Location and Plot Information
    plot_id BIGINT NOT NULL,
    plot_col BIGINT NOT NULL,
    plot_row BIGINT NOT NULL,
    plot_number BIGINT NOT NULL,
	plot_code character varying(255),
	plot_x REAL,
	plot_y REAL,
    phy_zone INTEGER,
    district_code INTEGER,
    
    -- Tree Identification
    tree_no INTEGER,
    
    -- Stand and Position Data
    forest_stand INTEGER,
    bearing REAL,
    distance REAL,
    tree_x REAL,
    tree_y REAL,

    
    -- Species Information
    species_code INTEGER,
    hd_model_code INTEGER,
    
    -- Tree Measurements
    dbh REAL,
    quality_class INTEGER,
    quality_class_code BIGINT,
    crown_class INTEGER,
    crown_class_code BIGINT,
    sample_tree_type INTEGER,
    sample_tree_type_code BIGINT,
    
    -- Height Measurements
    height REAL,
    crown_height REAL,
    base_tree_height REAL,
    base_crown_height REAL,
    base_slope REAL,
    
    -- Growth Data
    age INTEGER,
    radial_growth INTEGER,
    
    -- Height-Diameter Modeling
    heigth_calculated REAL,
    height_predicted REAL,
    
    -- Volume Ratio
    volume_ratio REAL,
    
    -- Expansion Factors
    exp_fa REAL,
    no_trees_per_ha REAL,
    
    -- Basal Area Calculations
    ba_per_sqm REAL,
    ba_per_ha REAL,
    
    -- Volume Calculations
    volume_cum_tree REAL,
    volume_ba_tree REAL,
    volume_final_cum_tree REAL,
    Volume_final_cum_ha REAL,
        
    -- Biomass Ratios
    branch_ratio REAL,
    branch_ratio_final REAL,
    foliage_ratio REAL,
    foliage_ratio_final REAL,
    
    -- Biomass Calculations (per tree)
    stem_kg_tree REAL,
    branch_kg_tree REAL,
    foliage_kg_tree REAL,
    
    -- Biomass Calculations (per hectare)
    stem_ton_ha REAL,
    branch_ton_ha REAL,
    foliage_ton_ha REAL,
    total_biomass_ad_tree REAL,
    total_biom_ad_ton_ha REAL,
    total_bio_ad REAL,
    total_biomass_od_tree REAL,
    total_biom_od_ton_ha REAL,
    
    -- Carbon Calculation
    carbon_kg_tree REAL,
    carbon_ton_ha REAL,
    
    -- Additional Fields
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);