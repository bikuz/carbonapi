CREATE TABLE "fra_high_mountain_high_himal"."sub_plot_code" (
	"sub_plot_code_id_" bigint NOT NULL PRIMARY KEY,
	"sub_plot" varchar(255) NOT NULL,
	"sub_plot_label" varchar(255),
	"sub_plot_label_en" varchar(255),
	"sub_plot_desc" varchar(511),
	"sub_plot_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."positioning_method_code" (
	"positioning_method_code_id_" bigint NOT NULL PRIMARY KEY,
	"positioning_method" varchar(255) NOT NULL,
	"positioning_method_label" varchar(255),
	"positioning_method_label_en" varchar(255),
	"positioning_method_desc" varchar(511),
	"positioning_method_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."fao_landuse_class_code" (
	"fao_landuse_class_code_id_" bigint NOT NULL PRIMARY KEY,
	"fao_landuse_class" varchar(255) NOT NULL,
	"fao_landuse_class_label" varchar(255),
	"fao_landuse_class_label_en" varchar(255),
	"fao_landuse_class_desc" varchar(511),
	"fao_landuse_class_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."tree_outside_forest_code" (
	"tree_outside_forest_code_id_" bigint NOT NULL PRIMARY KEY,
	"tree_outside_forest" varchar(255) NOT NULL,
	"tree_outside_forest_label" varchar(255),
	"tree_outside_forest_label_en" varchar(255),
	"tree_outside_forest_desc" varchar(511),
	"tree_outside_forest_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."rechability_code" (
	"rechability_code_id_" bigint NOT NULL PRIMARY KEY,
	"rechability" varchar(255) NOT NULL,
	"rechability_label" varchar(255),
	"rechability_label_en" varchar(255),
	"rechability_desc" varchar(511),
	"rechability_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."time_measurement_code" (
	"time_measurement_code_id_" bigint NOT NULL PRIMARY KEY,
	"time_measurement" varchar(255) NOT NULL,
	"time_measurement_label" varchar(255),
	"time_measurement_label_en" varchar(255),
	"time_measurement_desc" varchar(511),
	"time_measurement_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."phy_zones_code" (
	"phy_zones_code_id_" bigint NOT NULL PRIMARY KEY,
	"phy_zones" varchar(255) NOT NULL,
	"phy_zones_label" varchar(255),
	"phy_zones_label_en" varchar(255),
	"phy_zones_desc" varchar(511),
	"phy_zones_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."lrmp_landuse_code" (
	"lrmp_landuse_code_id_" bigint NOT NULL PRIMARY KEY,
	"lrmp_landuse" varchar(255) NOT NULL,
	"lrmp_landuse_label" varchar(255),
	"lrmp_landuse_label_en" varchar(255),
	"lrmp_landuse_desc" varchar(511),
	"lrmp_landuse_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."management_regime_code" (
	"management_regime_code_id_" bigint NOT NULL PRIMARY KEY,
	"management_regime" varchar(255) NOT NULL,
	"management_regime_label" varchar(255),
	"management_regime_label_en" varchar(255),
	"management_regime_desc" varchar(511),
	"management_regime_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."soil_depth_code" (
	"soil_depth_code_id_" bigint NOT NULL PRIMARY KEY,
	"soil_depth" varchar(255) NOT NULL,
	"soil_depth_label" varchar(255),
	"soil_depth_label_en" varchar(255),
	"soil_depth_desc" varchar(511),
	"soil_depth_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."organic_layer_code" (
	"organic_layer_code_id_" bigint NOT NULL PRIMARY KEY,
	"organic_layer" varchar(255) NOT NULL,
	"organic_layer_label" varchar(255),
	"organic_layer_label_en" varchar(255),
	"organic_layer_desc" varchar(511),
	"organic_layer_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."soil_texture_code" (
	"soil_texture_code_id_" bigint NOT NULL PRIMARY KEY,
	"soil_texture" varchar(255) NOT NULL,
	"soil_texture_label" varchar(255),
	"soil_texture_label_en" varchar(255),
	"soil_texture_desc" varchar(511),
	"soil_texture_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."main_site_type_code" (
	"main_site_type_code_id_" bigint NOT NULL PRIMARY KEY,
	"main_site_type" varchar(255) NOT NULL,
	"main_site_type_label" varchar(255),
	"main_site_type_label_en" varchar(255),
	"main_site_type_desc" varchar(511),
	"main_site_type_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."forest_type_code" (
	"forest_type_code_id_" bigint NOT NULL PRIMARY KEY,
	"forest_type" varchar(255) NOT NULL,
	"forest_type_label" varchar(255),
	"forest_type_label_en" varchar(255),
	"forest_type_desc" varchar(511),
	"forest_type_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."origin_code" (
	"origin_code_id_" bigint NOT NULL PRIMARY KEY,
	"origin" varchar(255) NOT NULL,
	"origin_label" varchar(255),
	"origin_label_en" varchar(255),
	"origin_desc" varchar(511),
	"origin_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."development_status_code" (
	"development_status_code_id_" bigint NOT NULL PRIMARY KEY,
	"development_status" varchar(255) NOT NULL,
	"development_status_label" varchar(255),
	"development_status_label_en" varchar(255),
	"development_status_desc" varchar(511),
	"development_status_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."quality_class_code" (
	"quality_class_code_id_" bigint NOT NULL PRIMARY KEY,
	"quality_class" varchar(255) NOT NULL,
	"quality_class_label" varchar(255),
	"quality_class_label_en" varchar(255),
	"quality_class_desc" varchar(511),
	"quality_class_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."crown_class_code" (
	"crown_class_code_id_" bigint NOT NULL PRIMARY KEY,
	"crown_class" varchar(255) NOT NULL,
	"crown_class_label" varchar(255),
	"crown_class_label_en" varchar(255),
	"crown_class_desc" varchar(511),
	"crown_class_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."macro_topography_code" (
	"macro_topography_code_id_" bigint NOT NULL PRIMARY KEY,
	"macro_topography" varchar(255) NOT NULL,
	"macro_topography_label" varchar(255),
	"macro_topography_label_en" varchar(255),
	"macro_topography_desc" varchar(511),
	"macro_topography_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."sample_tree_type_code" (
	"sample_tree_type_code_id_" bigint NOT NULL PRIMARY KEY,
	"sample_tree_type" varchar(255) NOT NULL,
	"sample_tree_type_label" varchar(255),
	"sample_tree_type_label_en" varchar(255),
	"sample_tree_type_desc" varchar(511),
	"sample_tree_type_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."crew_leader_code" (
	"crew_leader_code_id_" bigint NOT NULL PRIMARY KEY,
	"crew_leader" varchar(255) NOT NULL,
	"crew_leader_label" varchar(255),
	"crew_leader_label_en" varchar(255),
	"crew_leader_desc" varchar(511),
	"crew_leader_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."disturbances_code" (
	"disturbances_code_id_" bigint NOT NULL PRIMARY KEY,
	"disturbances" varchar(255) NOT NULL,
	"disturbances_label" varchar(255),
	"disturbances_label_en" varchar(255),
	"disturbances_desc" varchar(511),
	"disturbances_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."disturbance_intensity_code" (
	"disturbance_intensity_code_id_" bigint NOT NULL PRIMARY KEY,
	"disturbance_intensity" varchar(255) NOT NULL,
	"disturbance_intensity_label" varchar(255),
	"disturbance_intensity_label_en" varchar(255),
	"disturbance_intensity_desc" varchar(511),
	"disturbance_intensity_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."soil_horizon_code" (
	"soil_horizon_code_id_" bigint NOT NULL PRIMARY KEY,
	"soil_horizon" varchar(255) NOT NULL,
	"soil_horizon_label" varchar(255),
	"soil_horizon_label_en" varchar(255),
	"soil_horizon_desc" varchar(511),
	"soil_horizon_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."composite_layer_code" (
	"composite_layer_code_id_" bigint NOT NULL PRIMARY KEY,
	"composite_layer" varchar(255) NOT NULL,
	"composite_layer_label" varchar(255),
	"composite_layer_label_en" varchar(255),
	"composite_layer_desc" varchar(511),
	"composite_layer_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."cardinal_directions_code" (
	"cardinal_directions_code_id_" bigint NOT NULL PRIMARY KEY,
	"cardinal_directions" varchar(255) NOT NULL,
	"cardinal_directions_label" varchar(255),
	"cardinal_directions_label_en" varchar(255),
	"cardinal_directions_desc" varchar(511),
	"cardinal_directions_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."observation_type_mammal_code" (
	"observation_type_mammal_code_id_" bigint NOT NULL PRIMARY KEY,
	"observation_type_mammal" varchar(255) NOT NULL,
	"observation_type_mammal_label" varchar(255),
	"observation_type_mammal_label_en" varchar(255),
	"observation_type_mammal_desc" varchar(511),
	"observation_type_mammal_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."district_code" (
	"district_code_id_" bigint NOT NULL PRIMARY KEY,
	"district" varchar(255) NOT NULL,
	"district_label" varchar(255),
	"district_label_en" varchar(255),
	"district_desc" varchar(511),
	"district_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."invasive_status_code" (
	"invasive_status_code_id_" bigint NOT NULL PRIMARY KEY,
	"invasive_status" varchar(255) NOT NULL,
	"invasive_status_label" varchar(255),
	"invasive_status_label_en" varchar(255),
	"invasive_status_desc" varchar(511),
	"invasive_status_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."ntfp_usage_code" (
	"ntfp_usage_code_id_" bigint NOT NULL PRIMARY KEY,
	"ntfp_usage" varchar(255) NOT NULL,
	"ntfp_usage_label" varchar(255),
	"ntfp_usage_label_en" varchar(255),
	"ntfp_usage_desc" varchar(511),
	"ntfp_usage_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."ntfp_importance_code" (
	"ntfp_importance_code_id_" bigint NOT NULL PRIMARY KEY,
	"ntfp_importance" varchar(255) NOT NULL,
	"ntfp_importance_label" varchar(255),
	"ntfp_importance_label_en" varchar(255),
	"ntfp_importance_desc" varchar(511),
	"ntfp_importance_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."disease_infected_part_code" (
	"disease_infected_part_code_id_" bigint NOT NULL PRIMARY KEY,
	"disease_infected_part" varchar(255) NOT NULL,
	"disease_infected_part_label" varchar(255),
	"disease_infected_part_label_en" varchar(255),
	"disease_infected_part_desc" varchar(511),
	"disease_infected_part_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."disease_severity_code" (
	"disease_severity_code_id_" bigint NOT NULL PRIMARY KEY,
	"disease_severity" varchar(255) NOT NULL,
	"disease_severity_label" varchar(255),
	"disease_severity_label_en" varchar(255),
	"disease_severity_desc" varchar(511),
	"disease_severity_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."tof_abundance_code" (
	"tof_abundance_code_id_" bigint NOT NULL PRIMARY KEY,
	"tof_abundance" varchar(255) NOT NULL,
	"tof_abundance_label" varchar(255),
	"tof_abundance_label_en" varchar(255),
	"tof_abundance_desc" varchar(511),
	"tof_abundance_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."infection_type_code" (
	"infection_type_code_id_" bigint NOT NULL PRIMARY KEY,
	"infection_type" varchar(255) NOT NULL,
	"infection_type_label" varchar(255),
	"infection_type_label_en" varchar(255),
	"infection_type_desc" varchar(511),
	"infection_type_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."status_code" (
	"status_code_id_" bigint NOT NULL PRIMARY KEY,
	"status" varchar(255) NOT NULL,
	"status_label" varchar(255),
	"status_label_en" varchar(255),
	"status_desc" varchar(511),
	"status_desc_en" varchar(511)
);
CREATE TABLE "fra_high_mountain_high_himal"."plot" (
	"plot_id_" bigint NOT NULL PRIMARY KEY,
	"col" varchar(255),
	"row" varchar(255),
	"number" varchar(255),
	"phy_zone" varchar(255),
	"phy_zone_code_id_" bigint,
	"district_code" varchar(255),
	"district_code_code_id_" bigint,
	"utm_coordinate_x" float(24),
	"utm_coordinate_y" float(24),
	"utm_coordinate_srs" varchar(255),
	"utm_coordinate_altitude" float(24),
	"utm_coordinate_accuracy" float(24),
	"utm_coordinate_lat" float(24),
	"utm_coordinate_long" float(24),
	"positioning_method" varchar(255),
	"positioning_method_code_id_" bigint,
	"date" date,
	"date_year" integer,
	"date_month" integer,
	"date_day" integer,
	"crew_leader" varchar(255),
	"crew_leader_code_id_" bigint,
	"fao_landuse_class1" varchar(255),
	"fao_landuse_class1_code_id_" bigint,
	"tree_outside_forest" varchar(255),
	"tree_outside_forest_code_id_" bigint,
	"reachability1" varchar(255),
	"reachability1_code_id_" bigint,
	"municipality" varchar(255),
	"forest_name" varchar(255),
	"bearing_to_settlement" integer,
	"distance_to_settlement" integer,
	"bearing_to_other_landuse" integer,
	"distance_to_other_landuse" integer,
	"aspect" integer,
	"slope" integer,
	"altitude" integer,
	"macro_topography" varchar(255),
	"macro_topography_code_id_" bigint,
	"inaccessible_plot_area" integer,
	 FOREIGN KEY ("phy_zone_code_id_") REFERENCES "fra_high_mountain_high_himal"."phy_zones_code"("phy_zones_code_id_"), 
	 FOREIGN KEY ("district_code_code_id_") REFERENCES "fra_high_mountain_high_himal"."district_code"("district_code_id_"), 
	 FOREIGN KEY ("positioning_method_code_id_") REFERENCES "fra_high_mountain_high_himal"."positioning_method_code"("positioning_method_code_id_"), 
	 FOREIGN KEY ("crew_leader_code_id_") REFERENCES "fra_high_mountain_high_himal"."crew_leader_code"("crew_leader_code_id_"), 
	 FOREIGN KEY ("fao_landuse_class1_code_id_") REFERENCES "fra_high_mountain_high_himal"."fao_landuse_class_code"("fao_landuse_class_code_id_"), 
	 FOREIGN KEY ("tree_outside_forest_code_id_") REFERENCES "fra_high_mountain_high_himal"."tree_outside_forest_code"("tree_outside_forest_code_id_"), 
	 FOREIGN KEY ("reachability1_code_id_") REFERENCES "fra_high_mountain_high_himal"."rechability_code"("rechability_code_id_"), 
	 FOREIGN KEY ("macro_topography_code_id_") REFERENCES "fra_high_mountain_high_himal"."macro_topography_code"("macro_topography_code_id_")
);
CREATE TABLE "fra_high_mountain_high_himal"."stand" (
	"stand_id_" bigint NOT NULL PRIMARY KEY,
	"plot_id_" bigint NOT NULL,
	"forest_stand" integer,
	"reachability2" varchar(255),
	"reachability2_code_id_" bigint,
	"fao_landuse_class2" varchar(255),
	"fao_landuse_class2_code_id_" bigint,
	"lrmp_landuse_class" varchar(255),
	"lrmp_landuse_class_code_id_" bigint,
	"management_regime" varchar(255),
	"management_regime_code_id_" bigint,
	"soil_depth" varchar(255),
	"soil_depth_code_id_" bigint,
	"mean_penetration_depth" integer,
	"organic_layer_type" varchar(255),
	"organic_layer_type_code_id_" bigint,
	"organic_layer_thickness" integer,
	"soil_texture" varchar(255),
	"soil_texture_code_id_" bigint,
	"main_site_type" varchar(255),
	"main_site_type_code_id_" bigint,
	"forest_type" varchar(255),
	"forest_type_code_id_" bigint,
	"origin" varchar(255),
	"origin_code_id_" bigint,
	"crown_cover" integer,
	"development_status" varchar(255),
	"development_status_code_id_" bigint,
	 FOREIGN KEY ("plot_id_") REFERENCES "fra_high_mountain_high_himal"."plot"("plot_id_"), 
	 FOREIGN KEY ("reachability2_code_id_") REFERENCES "fra_high_mountain_high_himal"."rechability_code"("rechability_code_id_"), 
	 FOREIGN KEY ("fao_landuse_class2_code_id_") REFERENCES "fra_high_mountain_high_himal"."fao_landuse_class_code"("fao_landuse_class_code_id_"), 
	 FOREIGN KEY ("lrmp_landuse_class_code_id_") REFERENCES "fra_high_mountain_high_himal"."lrmp_landuse_code"("lrmp_landuse_code_id_"), 
	 FOREIGN KEY ("management_regime_code_id_") REFERENCES "fra_high_mountain_high_himal"."management_regime_code"("management_regime_code_id_"), 
	 FOREIGN KEY ("soil_depth_code_id_") REFERENCES "fra_high_mountain_high_himal"."soil_depth_code"("soil_depth_code_id_"), 
	 FOREIGN KEY ("organic_layer_type_code_id_") REFERENCES "fra_high_mountain_high_himal"."organic_layer_code"("organic_layer_code_id_"), 
	 FOREIGN KEY ("soil_texture_code_id_") REFERENCES "fra_high_mountain_high_himal"."soil_texture_code"("soil_texture_code_id_"), 
	 FOREIGN KEY ("main_site_type_code_id_") REFERENCES "fra_high_mountain_high_himal"."main_site_type_code"("main_site_type_code_id_"), 
	 FOREIGN KEY ("forest_type_code_id_") REFERENCES "fra_high_mountain_high_himal"."forest_type_code"("forest_type_code_id_"), 
	 FOREIGN KEY ("origin_code_id_") REFERENCES "fra_high_mountain_high_himal"."origin_code"("origin_code_id_"), 
	 FOREIGN KEY ("development_status_code_id_") REFERENCES "fra_high_mountain_high_himal"."development_status_code"("development_status_code_id_")
);
CREATE TABLE "fra_high_mountain_high_himal"."fixed_points" (
	"fixed_points_id_" bigint NOT NULL PRIMARY KEY,
	"plot_id_" bigint NOT NULL,
	"species_code" varchar(255),
	"species_scientific_name" varchar(255),
	"species_vernacular_name" varchar(255),
	"species_language_code" varchar(255),
	"species_language_variety" varchar(255),
	"species_family_code" varchar(255),
	"species_family_scientific_name" varchar(255),
	"bearing" integer,
	"distance" float(24),
	"dbh" float(24),
	"remarks" varchar(255),
	 FOREIGN KEY ("plot_id_") REFERENCES "fra_high_mountain_high_himal"."plot"("plot_id_")
);
CREATE TABLE "fra_high_mountain_high_himal"."tree_and_climber" (
	"tree_and_climber_id_" bigint NOT NULL PRIMARY KEY,
	"plot_id_" bigint NOT NULL,
	"tree_no" integer,
	"tree_and_climber_forest_stand" integer,
	"tree_and_climber_bearing" integer,
	"tree_and_climber_distance" float(24),
	"tree_and_climber_species_code" varchar(255),
	"tree_and_climber_species_scientific_name" varchar(255),
	"tree_and_climber_species_vernacular_name" varchar(255),
	"tree_and_climber_species_language_code" varchar(255),
	"tree_and_climber_species_language_variety" varchar(255),
	"tree_and_climber_species_family_code" varchar(255),
	"tree_and_climber_species_family_scientific_name" varchar(255),
	"tree_and_climber_dbh" float(24),
	"quality_class" varchar(255),
	"quality_class_code_id_" bigint,
	"crown_class" varchar(255),
	"crown_class_code_id_" bigint,
	"lopping" integer,
	"sample_tree_type" varchar(255),
	"sample_tree_type_code_id_" bigint,
	"height" float(24),
	"crown_height" float(24),
	"base_tree_height" float(24),
	"base_crown_height" float(24),
	"base_slope" float(24),
	"age" integer,
	"radial_growth" integer,
	"tree_and_climber_remarks" varchar(255),
	"status" varchar(255),
	"status_code_id_" bigint,
	 FOREIGN KEY ("plot_id_") REFERENCES "fra_high_mountain_high_himal"."plot"("plot_id_"), 
	 FOREIGN KEY ("quality_class_code_id_") REFERENCES "fra_high_mountain_high_himal"."quality_class_code"("quality_class_code_id_"), 
	 FOREIGN KEY ("crown_class_code_id_") REFERENCES "fra_high_mountain_high_himal"."crown_class_code"("crown_class_code_id_"), 
	 FOREIGN KEY ("sample_tree_type_code_id_") REFERENCES "fra_high_mountain_high_himal"."sample_tree_type_code"("sample_tree_type_code_id_"), 
	 FOREIGN KEY ("status_code_id_") REFERENCES "fra_high_mountain_high_himal"."status_code"("status_code_id_")
);
CREATE TABLE "fra_high_mountain_high_himal"."dead_trees" (
	"dead_trees_id_" bigint NOT NULL PRIMARY KEY,
	"plot_id_" bigint NOT NULL,
	"tree_number" integer,
	"dead_trees_species_code" varchar(255),
	"dead_trees_species_scientific_name" varchar(255),
	"dead_trees_species_vernacular_name" varchar(255),
	"dead_trees_species_language_code" varchar(255),
	"dead_trees_species_language_variety" varchar(255),
	"dead_trees_species_family_code" varchar(255),
	"dead_trees_species_family_scientific_name" varchar(255),
	"diameter_at_base" float(24),
	"diameter_at_tip" float(24),
	"length" float(24),
	"dead_trees_remarks" varchar(255),
	 FOREIGN KEY ("plot_id_") REFERENCES "fra_high_mountain_high_himal"."plot"("plot_id_")
);
CREATE TABLE "fra_high_mountain_high_himal"."disturbances" (
	"disturbances_id_" bigint NOT NULL PRIMARY KEY,
	"plot_id_" bigint NOT NULL,
	"disturbance" varchar(255),
	"disturbance_code_id_" bigint,
	"intensity" varchar(255),
	"intensity_code_id_" bigint,
	"disturbances_remarks" varchar(255),
	 FOREIGN KEY ("plot_id_") REFERENCES "fra_high_mountain_high_himal"."plot"("plot_id_"), 
	 FOREIGN KEY ("disturbance_code_id_") REFERENCES "fra_high_mountain_high_himal"."disturbances_code"("disturbances_code_id_"), 
	 FOREIGN KEY ("intensity_code_id_") REFERENCES "fra_high_mountain_high_himal"."disturbance_intensity_code"("disturbance_intensity_code_id_")
);
CREATE TABLE "fra_high_mountain_high_himal"."shrub_general" (
	"shrub_general_id_" bigint NOT NULL PRIMARY KEY,
	"plot_id_" bigint NOT NULL,
	"sub_plot" varchar(255),
	"sub_plot_code_id_" bigint,
	"shrub_general_crown_cover" integer,
	"shrub_general_forest_stand" integer,
	 FOREIGN KEY ("plot_id_") REFERENCES "fra_high_mountain_high_himal"."plot"("plot_id_"), 
	 FOREIGN KEY ("sub_plot_code_id_") REFERENCES "fra_high_mountain_high_himal"."sub_plot_code"("sub_plot_code_id_")
);
CREATE TABLE "fra_high_mountain_high_himal"."shrub_tally" (
	"shrub_tally_id_" bigint NOT NULL PRIMARY KEY,
	"plot_id_" bigint NOT NULL,
	"shrub_tally_sub_plot" varchar(255),
	"shrub_tally_sub_plot_code_id_" bigint,
	"shrub_tally_species_code" varchar(255),
	"shrub_tally_species_scientific_name" varchar(255),
	"shrub_tally_species_vernacular_name" varchar(255),
	"shrub_tally_species_language_code" varchar(255),
	"shrub_tally_species_language_variety" varchar(255),
	"shrub_tally_species_family_code" varchar(255),
	"shrub_tally_species_family_scientific_name" varchar(255),
	"frequency" integer,
	"diameter" float(24),
	"shrub_tally_height" float(24),
	"shrub_tally_remarks" varchar(255),
	 FOREIGN KEY ("plot_id_") REFERENCES "fra_high_mountain_high_himal"."plot"("plot_id_"), 
	 FOREIGN KEY ("shrub_tally_sub_plot_code_id_") REFERENCES "fra_high_mountain_high_himal"."sub_plot_code"("sub_plot_code_id_")
);
CREATE TABLE "fra_high_mountain_high_himal"."seedling" (
	"seedling_id_" bigint NOT NULL PRIMARY KEY,
	"plot_id_" bigint NOT NULL,
	"seedling_sub_plot" varchar(255),
	"seedling_sub_plot_code_id_" bigint,
	"seedling_species_code" varchar(255),
	"seedling_species_scientific_name" varchar(255),
	"seedling_species_vernacular_name" varchar(255),
	"seedling_species_language_code" varchar(255),
	"seedling_species_language_variety" varchar(255),
	"seedling_species_family_code" varchar(255),
	"seedling_species_family_scientific_name" varchar(255),
	"seedling_frequency" integer,
	"seedling_height" float(24),
	"seedling_remarks" varchar(255),
	 FOREIGN KEY ("plot_id_") REFERENCES "fra_high_mountain_high_himal"."plot"("plot_id_"), 
	 FOREIGN KEY ("seedling_sub_plot_code_id_") REFERENCES "fra_high_mountain_high_himal"."sub_plot_code"("sub_plot_code_id_")
);
CREATE TABLE "fra_high_mountain_high_himal"."sapling" (
	"sapling_id_" bigint NOT NULL PRIMARY KEY,
	"plot_id_" bigint NOT NULL,
	"sapling_sub_plot" varchar(255),
	"sapling_sub_plot_code_id_" bigint,
	"sapling_species_code" varchar(255),
	"sapling_species_scientific_name" varchar(255),
	"sapling_species_vernacular_name" varchar(255),
	"sapling_species_language_code" varchar(255),
	"sapling_species_language_variety" varchar(255),
	"sapling_species_family_code" varchar(255),
	"sapling_species_family_scientific_name" varchar(255),
	"sapling_frequency" integer,
	"sapling_height" float(24),
	"sapling_remarks" varchar(255),
	 FOREIGN KEY ("plot_id_") REFERENCES "fra_high_mountain_high_himal"."plot"("plot_id_"), 
	 FOREIGN KEY ("sapling_sub_plot_code_id_") REFERENCES "fra_high_mountain_high_himal"."sub_plot_code"("sub_plot_code_id_")
);
CREATE TABLE "fra_high_mountain_high_himal"."soil_pit_description" (
	"soil_pit_description_id_" bigint NOT NULL PRIMARY KEY,
	"plot_id_" bigint NOT NULL,
	"sample_point" varchar(255),
	"sample_point_code_id_" bigint,
	"horizon" varchar(255),
	"horizon_code_id_" bigint,
	"horizon_thickness" integer,
	"munsell_color" varchar(255),
	"soil_pit_description_soil_texture" varchar(255),
	"soil_pit_description_soil_texture_code_id_" bigint,
	"coarse_fraction" integer,
	 FOREIGN KEY ("plot_id_") REFERENCES "fra_high_mountain_high_himal"."plot"("plot_id_"), 
	 FOREIGN KEY ("sample_point_code_id_") REFERENCES "fra_high_mountain_high_himal"."cardinal_directions_code"("cardinal_directions_code_id_"), 
	 FOREIGN KEY ("horizon_code_id_") REFERENCES "fra_high_mountain_high_himal"."soil_horizon_code"("soil_horizon_code_id_"), 
	 FOREIGN KEY ("soil_pit_description_soil_texture_code_id_") REFERENCES "fra_high_mountain_high_himal"."soil_texture_code"("soil_texture_code_id_")
);
CREATE TABLE "fra_high_mountain_high_himal"."composite_sample" (
	"composite_sample_id_" bigint NOT NULL PRIMARY KEY,
	"plot_id_" bigint NOT NULL,
	"sample_layer" varchar(255),
	"sample_layer_code_id_" bigint,
	"no_of_sub_samples" integer,
	"no_of_corer_samples" integer,
	"total_vol_of_non_corer_sample" float(24),
	"total_vol_of_composite_samples" float(24),
	"composite_total___sample_fresh_wt" varchar(255),
	 FOREIGN KEY ("plot_id_") REFERENCES "fra_high_mountain_high_himal"."plot"("plot_id_"), 
	 FOREIGN KEY ("sample_layer_code_id_") REFERENCES "fra_high_mountain_high_himal"."composite_layer_code"("composite_layer_code_id_")
);
CREATE TABLE "fra_high_mountain_high_himal"."epiphytes" (
	"epiphytes_id_" bigint NOT NULL PRIMARY KEY,
	"plot_id_" bigint NOT NULL,
	"epiphytes_species_code" varchar(255),
	"epiphytes_species_scientific_name" varchar(255),
	"epiphytes_species_vernacular_name" varchar(255),
	"epiphytes_species_language_code" varchar(255),
	"epiphytes_species_language_variety" varchar(255),
	"epiphytes_species_family_code" varchar(255),
	"epiphytes_species_family_scientific_name" varchar(255),
	"host_species_code" varchar(255),
	"host_species_scientific_name" varchar(255),
	"host_species_vernacular_name" varchar(255),
	"host_species_language_code" varchar(255),
	"host_species_language_variety" varchar(255),
	"host_species_family_code" varchar(255),
	"host_species_family_scientific_name" varchar(255),
	"epiphytes_forest_stand" integer,
	"epiphytes_remarks" varchar(255),
	 FOREIGN KEY ("plot_id_") REFERENCES "fra_high_mountain_high_himal"."plot"("plot_id_")
);
CREATE TABLE "fra_high_mountain_high_himal"."herbaceous" (
	"herbaceous_id_" bigint NOT NULL PRIMARY KEY,
	"plot_id_" bigint NOT NULL,
	"herbaceous_sub_plot" varchar(255),
	"herbaceous_sub_plot_code_id_" bigint,
	"herbaceous_species_code" varchar(255),
	"herbaceous_species_scientific_name" varchar(255),
	"herbaceous_species_vernacular_name" varchar(255),
	"herbaceous_species_language_code" varchar(255),
	"herbaceous_species_language_variety" varchar(255),
	"herbaceous_species_family_code" varchar(255),
	"herbaceous_species_family_scientific_name" varchar(255),
	"cover" integer,
	"herbaceous_forest_stand" integer,
	 FOREIGN KEY ("plot_id_") REFERENCES "fra_high_mountain_high_himal"."plot"("plot_id_"), 
	 FOREIGN KEY ("herbaceous_sub_plot_code_id_") REFERENCES "fra_high_mountain_high_himal"."sub_plot_code"("sub_plot_code_id_")
);
CREATE TABLE "fra_high_mountain_high_himal"."mammals" (
	"mammals_id_" bigint NOT NULL PRIMARY KEY,
	"plot_id_" bigint NOT NULL,
	"mammals_species_code" varchar(255),
	"mammals_species_scientific_name" varchar(255),
	"mammals_species_vernacular_name" varchar(255),
	"mammals_species_language_code" varchar(255),
	"mammals_species_language_variety" varchar(255),
	"mammals_species_family_code" varchar(255),
	"mammals_species_family_scientific_name" varchar(255),
	"type_of_observation" varchar(255),
	"type_of_observation_code_id_" bigint,
	"mammals_remarks" varchar(255),
	 FOREIGN KEY ("plot_id_") REFERENCES "fra_high_mountain_high_himal"."plot"("plot_id_"), 
	 FOREIGN KEY ("type_of_observation_code_id_") REFERENCES "fra_high_mountain_high_himal"."observation_type_mammal_code"("observation_type_mammal_code_id_")
);
CREATE TABLE "fra_high_mountain_high_himal"."ntfp" (
	"ntfp_id_" bigint NOT NULL PRIMARY KEY,
	"plot_id_" bigint NOT NULL,
	"ntfp_species_code" varchar(255),
	"ntfp_species_scientific_name" varchar(255),
	"ntfp_species_vernacular_name" varchar(255),
	"ntfp_species_language_code" varchar(255),
	"ntfp_species_language_variety" varchar(255),
	"ntfp_species_family_code" varchar(255),
	"ntfp_species_family_scientific_name" varchar(255),
	"usage" varchar(255),
	"usage_code_id_" bigint,
	"importance" varchar(255),
	"importance_code_id_" bigint,
	"photo_id" varchar(255),
	 FOREIGN KEY ("plot_id_") REFERENCES "fra_high_mountain_high_himal"."plot"("plot_id_"), 
	 FOREIGN KEY ("usage_code_id_") REFERENCES "fra_high_mountain_high_himal"."ntfp_usage_code"("ntfp_usage_code_id_"), 
	 FOREIGN KEY ("importance_code_id_") REFERENCES "fra_high_mountain_high_himal"."ntfp_importance_code"("ntfp_importance_code_id_")
);
CREATE TABLE "fra_high_mountain_high_himal"."tof" (
	"tof_id_" bigint NOT NULL PRIMARY KEY,
	"plot_id_" bigint NOT NULL,
	"tof_species_code" varchar(255),
	"tof_species_scientific_name" varchar(255),
	"tof_species_vernacular_name" varchar(255),
	"tof_species_language_code" varchar(255),
	"tof_species_language_variety" varchar(255),
	"tof_species_family_code" varchar(255),
	"tof_species_family_scientific_name" varchar(255),
	"abundance" varchar(255),
	"abundance_code_id_" bigint,
	 FOREIGN KEY ("plot_id_") REFERENCES "fra_high_mountain_high_himal"."plot"("plot_id_"), 
	 FOREIGN KEY ("abundance_code_id_") REFERENCES "fra_high_mountain_high_himal"."tof_abundance_code"("tof_abundance_code_id_")
);
CREATE TABLE "fra_high_mountain_high_himal"."invasive" (
	"invasive_id_" bigint NOT NULL PRIMARY KEY,
	"plot_id_" bigint NOT NULL,
	"invasive_species_code" varchar(255),
	"invasive_species_scientific_name" varchar(255),
	"invasive_species_vernacular_name" varchar(255),
	"invasive_species_language_code" varchar(255),
	"invasive_species_language_variety" varchar(255),
	"invasive_species_family_code" varchar(255),
	"invasive_species_family_scientific_name" varchar(255),
	"invasion_status" varchar(255),
	"invasion_status_code_id_" bigint,
	 FOREIGN KEY ("plot_id_") REFERENCES "fra_high_mountain_high_himal"."plot"("plot_id_"), 
	 FOREIGN KEY ("invasion_status_code_id_") REFERENCES "fra_high_mountain_high_himal"."invasive_status_code"("invasive_status_code_id_")
);
CREATE TABLE "fra_high_mountain_high_himal"."disease_and_pests" (
	"disease_and_pests_id_" bigint NOT NULL PRIMARY KEY,
	"plot_id_" bigint NOT NULL,
	"disease_and_pests_species_code" varchar(255),
	"disease_and_pests_species_scientific_name" varchar(255),
	"disease_and_pests_species_vernacular_name" varchar(255),
	"disease_and_pests_species_language_code" varchar(255),
	"disease_and_pests_species_language_variety" varchar(255),
	"disease_and_pests_species_family_code" varchar(255),
	"disease_and_pests_species_family_scientific_name" varchar(255),
	"infection_type" varchar(255),
	"infection_type_code_id_" bigint,
	"parts_infected" varchar(255),
	"parts_infected_code_id_" bigint,
	"severity" varchar(255),
	"severity_code_id_" bigint,
	"photo" varchar(255),
	"disease_and_pests_remarks" varchar(255),
	 FOREIGN KEY ("plot_id_") REFERENCES "fra_high_mountain_high_himal"."plot"("plot_id_"), 
	 FOREIGN KEY ("infection_type_code_id_") REFERENCES "fra_high_mountain_high_himal"."infection_type_code"("infection_type_code_id_"), 
	 FOREIGN KEY ("parts_infected_code_id_") REFERENCES "fra_high_mountain_high_himal"."disease_infected_part_code"("disease_infected_part_code_id_"), 
	 FOREIGN KEY ("severity_code_id_") REFERENCES "fra_high_mountain_high_himal"."disease_severity_code"("disease_severity_code_id_")
);
CREATE TABLE "fra_high_mountain_high_himal"."time_measurement" (
	"time_measurement_id_" bigint NOT NULL PRIMARY KEY,
	"plot_id_" bigint NOT NULL,
	"action" varchar(255),
	"action_code_id_" bigint,
	"time" time,
	"time_hour" integer,
	"time_minute" integer,
	 FOREIGN KEY ("plot_id_") REFERENCES "fra_high_mountain_high_himal"."plot"("plot_id_"), 
	 FOREIGN KEY ("action_code_id_") REFERENCES "fra_high_mountain_high_himal"."time_measurement_code"("time_measurement_code_id_")
);
CREATE TABLE "fra_high_mountain_high_himal"."plot_photo" (
	"plot_photo_id_" bigint NOT NULL PRIMARY KEY,
	"plot_id_" bigint NOT NULL,
	"direction" varchar(255),
	"direction_code_id_" bigint,
	"plot_photo_photo" varchar(255),
	 FOREIGN KEY ("plot_id_") REFERENCES "fra_high_mountain_high_himal"."plot"("plot_id_"), 
	 FOREIGN KEY ("direction_code_id_") REFERENCES "fra_high_mountain_high_himal"."sub_plot_code"("sub_plot_code_id_")
);
CREATE TABLE "fra_high_mountain_high_himal"."soil_pit_photo" (
	"soil_pit_photo_id_" bigint NOT NULL PRIMARY KEY,
	"plot_id_" bigint NOT NULL,
	"soil_pit_photo_direction" varchar(255),
	"soil_pit_photo_direction_code_id_" bigint,
	"soil_pit_photo_photo" varchar(255),
	 FOREIGN KEY ("plot_id_") REFERENCES "fra_high_mountain_high_himal"."plot"("plot_id_"), 
	 FOREIGN KEY ("soil_pit_photo_direction_code_id_") REFERENCES "fra_high_mountain_high_himal"."cardinal_directions_code"("cardinal_directions_code_id_")
);
CREATE TABLE "fra_high_mountain_high_himal"."sympodial_bamboo_spps" (
	"sympodial_bamboo_spps_id_" bigint NOT NULL PRIMARY KEY,
	"plot_id_" bigint NOT NULL,
	"species_name_code" varchar(255),
	"species_name_scientific_name" varchar(255),
	"species_name_vernacular_name" varchar(255),
	"species_name_language_code" varchar(255),
	"species_name_language_variety" varchar(255),
	"species_name_family_code" varchar(255),
	"species_name_family_scientific_name" varchar(255),
	"sympodial_bamboo_spps_bearing" integer,
	"sympodial_bamboo_spps_distance" float(24),
	"culms_no" integer,
	"dbh_cm" float(24),
	"clump_height_m" float(24),
	"internode_distance_cm" float(24),
	"sympodial_bamboo_spps_remarks" varchar(255),
	 FOREIGN KEY ("plot_id_") REFERENCES "fra_high_mountain_high_himal"."plot"("plot_id_")
);
CREATE TABLE "fra_high_mountain_high_himal"."monopodial_bamboo_spps" (
	"monopodial_bamboo_spps_id_" bigint NOT NULL PRIMARY KEY,
	"plot_id_" bigint NOT NULL,
	"monopodial_bamboo_spps_species_name_code" varchar(255),
	"monopodial_bamboo_spps_species_name_scientific_name" varchar(255),
	"monopodial_bamboo_spps_species_name_vernacular_name" varchar(255),
	"monopodial_bamboo_spps_species_name_language_code" varchar(255),
	"monopodial_bamboo_spps_species_name_language_variety" varchar(255),
	"monopodial_bamboo_spps_species_name_family_code" varchar(255),
	"monopodial_bamboo_spps_species_name_family_scientific_name" varchar(255),
	"monopodial_bamboo_spps_bearing" integer,
	"monopodial_bamboo_spps_distance" float(24),
	"cardinal" varchar(255),
	"cardinal_code_id_" bigint,
	"monopodial_bamboo_spps_dbh" float(24),
	"height_m" float(24),
	"monopodial_bamboo_spps_internode_distance_cm" float(24),
	"total_no_of_culms" integer,
	"monopodial_bamboo_spps_remarks" varchar(255),
	 FOREIGN KEY ("plot_id_") REFERENCES "fra_high_mountain_high_himal"."plot"("plot_id_"), 
	 FOREIGN KEY ("cardinal_code_id_") REFERENCES "fra_high_mountain_high_himal"."cardinal_directions_code"("cardinal_directions_code_id_")
);
CREATE TABLE "fra_high_mountain_high_himal"."bamboo_assessment" (
	"bamboo_assessment_id_" bigint NOT NULL PRIMARY KEY,
	"plot_id_" bigint NOT NULL,
	"name_of_bamboo" varchar(255),
	"x_coordinate" integer,
	"y_coordinate" integer,
	"no_of_culms" integer,
	"bamboo_assessment_photo_id" varchar(255),
	"bamboo_assessment_remarks" varchar(255),
	 FOREIGN KEY ("plot_id_") REFERENCES "fra_high_mountain_high_himal"."plot"("plot_id_")
);