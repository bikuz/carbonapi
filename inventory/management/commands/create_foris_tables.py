from django.core.management.base import BaseCommand
# from django.db import connection
from carbonapi.database.connection import get_foris_connection
from django.db.utils import ProgrammingError

class Command(BaseCommand):
    help = 'Creates all database tables for the inventory app based on the provided schema'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Starting to create inventory tables...'))
        
        # Get database connection parameters
        # db_config = settings.DATABASES['default']
        conn = get_foris_connection()
        
        try:
            with conn.cursor() as cursor:
                self.create_plot_table(cursor)
                self.create_stand_table(cursor)
                self.create_fixed_points_table(cursor)
                self.create_tree_and_climber_table(cursor)
                self.create_dead_trees_table(cursor)
                self.create_disturbances_table(cursor)
                self.create_shrub_general_table(cursor)
                self.create_shrub_tally_table(cursor)
                self.create_seedling_table(cursor)
                self.create_sapling_table(cursor)
                self.create_soil_pit_description_table(cursor)
                self.create_composite_sample_table(cursor)
                self.create_epiphytes_table(cursor)
                self.create_herbaceous_table(cursor)
                self.create_mammals_table(cursor)
                self.create_ntfp_table(cursor)
                self.create_tof_table(cursor)
                self.create_invasive_table(cursor)
                self.create_disease_and_pests_table(cursor)
                self.create_time_measurement_table(cursor)
                self.create_plot_photo_table(cursor)
                self.create_soil_pit_photo_table(cursor)
                self.create_sympodial_bamboo_spps_table(cursor)
                self.create_monopodial_bamboo_spps_table(cursor)
                self.create_bamboo_assessment_table(cursor)
                
                conn.commit() 
                cursor.close()
                conn.close()
                self.stdout.write(self.style.SUCCESS('Successfully created all inventory tables!'))
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error creating tables: {str(e)}'))
            raise e

    def create_plot_table(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_plot (
                id SERIAL PRIMARY KEY,
                plot_col VARCHAR(50),
                plot_row VARCHAR(50),
                plot_number VARCHAR(50) UNIQUE,
                plot_position VARCHAR(50),
                utm_coordinate_srs VARCHAR(50),
                utm_coordinate_x DECIMAL(12,6),
                utm_coordinate_y DECIMAL(12,6),
                positioning_method VARCHAR(100),
                date_year INTEGER,
                date_month INTEGER,
                date_day INTEGER,
                crew_leader VARCHAR(100),
                fao_landuse_class1 VARCHAR(100),
                tree_outside_forest BOOLEAN,
                reachability1 VARCHAR(100),
                municipality VARCHAR(100),
                forest_name VARCHAR(100),
                bearing_to_settlement VARCHAR(50),
                distance_to_settlement DECIMAL(10,2),
                bearing_to_other_landuse VARCHAR(50),
                distance_to_other_landuse DECIMAL(10,2),
                aspect VARCHAR(50),
                slope DECIMAL(5,2),
                altitude DECIMAL(7,2),
                macro_topography VARCHAR(100),
                inaccessible_plot_area DECIMAL(10,2)
            )
        """)
        self.stdout.write(self.style.SUCCESS('Created plot table'))

    def create_stand_table(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_stand (
                id SERIAL PRIMARY KEY,
                plot_number VARCHAR(50) REFERENCES inventory_plot(plot_number),
                stand_position VARCHAR(50),
                forest_stand VARCHAR(100),
                reachability2 VARCHAR(100),
                fao_landuse_class2 VARCHAR(100),
                lrmp_landuse_class VARCHAR(100),
                management_regime VARCHAR(100),
                soil_depth DECIMAL(5,2),
                mean_penetration_depth DECIMAL(5,2),
                organic_layer_type VARCHAR(100),
                organic_layer_thickness DECIMAL(5,2),
                soil_texture VARCHAR(100),
                main_site_type VARCHAR(100),
                forest_type VARCHAR(100),
                origin VARCHAR(100),
                crown_cover DECIMAL(5,2),
                development_status VARCHAR(100)
            )
        """)
        self.stdout.write(self.style.SUCCESS('Created stand table'))

    def create_fixed_points_table(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_fixed_points (
                id SERIAL PRIMARY KEY,
                plot_number VARCHAR(50) REFERENCES inventory_plot(plot_number),
                fixed_points_position VARCHAR(50),
                species_code VARCHAR(50),
                species_scientific_name VARCHAR(100),
                species_vernacular_name VARCHAR(100),
                bearing DECIMAL(5,2),
                distance DECIMAL(10,2),
                distance_unit_name VARCHAR(20)
            )
        """)
        self.stdout.write(self.style.SUCCESS('Created fixed_points table'))

    def create_tree_and_climber_table(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_tree_and_climber (
                id SERIAL PRIMARY KEY,
                plot_number VARCHAR(50) REFERENCES inventory_plot(plot_number),
                tree_and_climber_position VARCHAR(50),
                species_code VARCHAR(50),
                species_scientific_name VARCHAR(100),
                species_vernacular_name VARCHAR(100),
                tree_no VARCHAR(50),
                forest_stand VARCHAR(100),
                bearing DECIMAL(5,2),
                distance DECIMAL(10,2),
                dbh DECIMAL(7,2),
                remarks TEXT,
                quality_class VARCHAR(50),
                crown_class VARCHAR(50),
                lopping VARCHAR(100),
                sample_tree_type VARCHAR(50),
                height DECIMAL(7,2),
                crown_height DECIMAL(7,2),
                base_tree_height DECIMAL(7,2),
                base_crown_height DECIMAL(7,2),
                base_slope DECIMAL(5,2),
                age INTEGER,
                radial_growth DECIMAL(5,2),
                status VARCHAR(50)
            )
        """)
        self.stdout.write(self.style.SUCCESS('Created tree_and_climber table'))

    def create_dead_trees_table(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_dead_trees (
                id SERIAL PRIMARY KEY,
                plot_number VARCHAR(50) REFERENCES inventory_plot(plot_number),
                dead_trees_position VARCHAR(50),
                tree_number VARCHAR(50),
                species_code VARCHAR(50),
                species_scientific_name VARCHAR(100),
                species_vernacular_name VARCHAR(100),
                diameter_at_base DECIMAL(7,2),
                diameter_at_tip DECIMAL(7,2),
                length DECIMAL(7,2),
                remarks TEXT
            )
        """)
        self.stdout.write(self.style.SUCCESS('Created dead_trees table'))

    def create_disturbances_table(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_disturbances (
                id SERIAL PRIMARY KEY,
                plot_number VARCHAR(50) REFERENCES inventory_plot(plot_number),
                disturbances_position VARCHAR(50),
                disturbance VARCHAR(100),
                intensity VARCHAR(50),
                remarks TEXT
            )
        """)
        self.stdout.write(self.style.SUCCESS('Created disturbances table'))

    def create_shrub_general_table(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_shrub_general (
                id SERIAL PRIMARY KEY,
                plot_number VARCHAR(50) REFERENCES inventory_plot(plot_number),
                shrub_general_position VARCHAR(50),
                sub_plot VARCHAR(50),
                crown_cover DECIMAL(5,2),
                forest_stand VARCHAR(100),
                diameter DECIMAL(5,2),
                height DECIMAL(5,2),
                remarks TEXT
            )
        """)
        self.stdout.write(self.style.SUCCESS('Created shrub_general table'))

    def create_shrub_tally_table(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_shrub_tally (
                id SERIAL PRIMARY KEY,
                plot_number VARCHAR(50) REFERENCES inventory_plot(plot_number),
                shrub_tally_position VARCHAR(50),
                sub_plot VARCHAR(50),
                species_code VARCHAR(50),
                species_scientific_name VARCHAR(100),
                species_vernacular_name VARCHAR(100),
                frequency INTEGER,
                remarks TEXT
            )
        """)
        self.stdout.write(self.style.SUCCESS('Created shrub_tally table'))

    def create_seedling_table(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_seedling (
                id SERIAL PRIMARY KEY,
                plot_number VARCHAR(50) REFERENCES inventory_plot(plot_number),
                seedling_position VARCHAR(50),
                sub_plot VARCHAR(50),
                species_code VARCHAR(50),
                species_scientific_name VARCHAR(100),
                species_vernacular_name VARCHAR(100),
                height DECIMAL(5,2),
                remarks TEXT
            )
        """)
        self.stdout.write(self.style.SUCCESS('Created seedling table'))

    def create_sapling_table(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_sapling (
                id SERIAL PRIMARY KEY,
                plot_number VARCHAR(50) REFERENCES inventory_plot(plot_number),
                sapling_position VARCHAR(50),
                sub_plot VARCHAR(50),
                species_code VARCHAR(50),
                species_scientific_name VARCHAR(100),
                species_vernacular_name VARCHAR(100),
                height DECIMAL(5,2),
                remarks TEXT
            )
        """)
        self.stdout.write(self.style.SUCCESS('Created sapling table'))

    def create_soil_pit_description_table(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_soil_pit_description (
                id SERIAL PRIMARY KEY,
                plot_number VARCHAR(50) REFERENCES inventory_plot(plot_number),
                soil_pit_description_position VARCHAR(50),
                sample_point VARCHAR(50),
                horizon VARCHAR(50),
                horizon_thickness DECIMAL(5,2),
                munsell_color VARCHAR(50),
                soil_texture VARCHAR(100),
                coarse_fraction DECIMAL(5,2)
            )
        """)
        self.stdout.write(self.style.SUCCESS('Created soil_pit_description table'))

    def create_composite_sample_table(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_composite_sample (
                id SERIAL PRIMARY KEY,
                plot_number VARCHAR(50) REFERENCES inventory_plot(plot_number),
                composite_sample_position VARCHAR(50),
                sample_layer VARCHAR(50),
                no_of_sub_samples INTEGER,
                no_of_corer_samples INTEGER,
                total_vol_of_non_corer_sample DECIMAL(7,2),
                total_vol_of_composite_samples DECIMAL(7,2),
                composite_total_sample_fresh_wt DECIMAL(7,2)
            )
        """)
        self.stdout.write(self.style.SUCCESS('Created composite_sample table'))

    def create_epiphytes_table(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_epiphytes (
                id SERIAL PRIMARY KEY,
                plot_number VARCHAR(50) REFERENCES inventory_plot(plot_number),
                epiphytes_position VARCHAR(50),
                species_code VARCHAR(50),
                species_scientific_name VARCHAR(100),
                species_vernacular_name VARCHAR(100),
                host_species_code VARCHAR(50),
                host_species_scientific_name VARCHAR(100),
                host_species_vernacular_name VARCHAR(100),
                cover DECIMAL(5,2),
                forest_stand VARCHAR(100),
                remarks TEXT
            )
        """)
        self.stdout.write(self.style.SUCCESS('Created epiphytes table'))

    def create_herbaceous_table(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_herbaceous (
                id SERIAL PRIMARY KEY,
                plot_number VARCHAR(50) REFERENCES inventory_plot(plot_number),
                herbaceous_position VARCHAR(50),
                sub_plot VARCHAR(50),
                species_code VARCHAR(50),
                species_scientific_name VARCHAR(100),
                species_vernacular_name VARCHAR(100),
                type_of_observation VARCHAR(100),
                remarks TEXT,
                photo_id VARCHAR(50)
            )
        """)
        self.stdout.write(self.style.SUCCESS('Created herbaceous table'))

    def create_mammals_table(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_mammals (
                id SERIAL PRIMARY KEY,
                plot_number VARCHAR(50) REFERENCES inventory_plot(plot_number),
                mammals_position VARCHAR(50),
                species_code VARCHAR(50),
                species_scientific_name VARCHAR(100),
                species_vernacular_name VARCHAR(100),
                usage VARCHAR(100),
                importance VARCHAR(100),
                photo_id VARCHAR(50),
                remarks TEXT
            )
        """)
        self.stdout.write(self.style.SUCCESS('Created mammals table'))

    def create_ntfp_table(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_ntfp (
                id SERIAL PRIMARY KEY,
                plot_number VARCHAR(50) REFERENCES inventory_plot(plot_number),
                ntfp_position VARCHAR(50),
                species_code VARCHAR(50),
                species_scientific_name VARCHAR(100),
                species_vernacular_name VARCHAR(100),
                abundance VARCHAR(100),
                photo_id VARCHAR(50),
                remarks TEXT
            )
        """)
        self.stdout.write(self.style.SUCCESS('Created ntfp table'))

    def create_tof_table(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_tof (
                id SERIAL PRIMARY KEY,
                plot_number VARCHAR(50) REFERENCES inventory_plot(plot_number),
                tof_position VARCHAR(50),
                species_code VARCHAR(50),
                species_scientific_name VARCHAR(100),
                species_vernacular_name VARCHAR(100),
                invasion_status VARCHAR(100),
                remarks TEXT
            )
        """)
        self.stdout.write(self.style.SUCCESS('Created tof table'))

    def create_invasive_table(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_invasive (
                id SERIAL PRIMARY KEY,
                plot_number VARCHAR(50) REFERENCES inventory_plot(plot_number),
                invasive_position VARCHAR(50),
                species_code VARCHAR(50),
                species_scientific_name VARCHAR(100),
                species_vernacular_name VARCHAR(100),
                infection_type VARCHAR(100),
                parts_infected VARCHAR(100),
                severity VARCHAR(100),
                photo_id VARCHAR(50),
                remarks TEXT
            )
        """)
        self.stdout.write(self.style.SUCCESS('Created invasive table'))

    def create_disease_and_pests_table(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_disease_and_pests (
                id SERIAL PRIMARY KEY,
                plot_number VARCHAR(50) REFERENCES inventory_plot(plot_number),
                disease_and_pests_position VARCHAR(50),
                action VARCHAR(100),
                time_hour INTEGER,
                time_minute INTEGER,
                remarks TEXT
            )
        """)
        self.stdout.write(self.style.SUCCESS('Created disease_and_pests table'))

    def create_time_measurement_table(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_time_measurement (
                id SERIAL PRIMARY KEY,
                plot_number VARCHAR(50) REFERENCES inventory_plot(plot_number),
                time_measurement_position VARCHAR(50),
                direction VARCHAR(50),
                photo VARCHAR(100),
                remarks TEXT
            )
        """)
        self.stdout.write(self.style.SUCCESS('Created time_measurement table'))

    def create_plot_photo_table(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_plot_photo (
                id SERIAL PRIMARY KEY,
                plot_number VARCHAR(50) REFERENCES inventory_plot(plot_number),
                plot_photo_position VARCHAR(50),
                direction VARCHAR(50),
                photo VARCHAR(100),
                remarks TEXT
            )
        """)
        self.stdout.write(self.style.SUCCESS('Created plot_photo table'))

    def create_soil_pit_photo_table(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_soil_pit_photo (
                id SERIAL PRIMARY KEY,
                plot_number VARCHAR(50) REFERENCES inventory_plot(plot_number),
                soil_pit_photo_position VARCHAR(50),
                direction VARCHAR(50),
                photo VARCHAR(100),
                remarks TEXT
            )
        """)
        self.stdout.write(self.style.SUCCESS('Created soil_pit_photo table'))

    def create_sympodial_bamboo_spps_table(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_sympodial_bamboo_spps (
                id SERIAL PRIMARY KEY,
                plot_number VARCHAR(50) REFERENCES inventory_plot(plot_number),
                sympodial_bamboo_spps_position VARCHAR(50),
                species_name_code VARCHAR(50),
                species_name_scientific_name VARCHAR(100),
                species_name_vernacular_name VARCHAR(100),
                species_name_language_code VARCHAR(50),
                species_name_language_variety VARCHAR(100),
                bearing DECIMAL(5,2),
                distance DECIMAL(10,2),
                culms_no INTEGER,
                dbh_cm DECIMAL(5,2),
                clump_height_m DECIMAL(5,2),
                internode_distance_cm DECIMAL(5,2),
                remarks TEXT
            )
        """)
        self.stdout.write(self.style.SUCCESS('Created sympodial_bamboo_spps table'))

    def create_monopodial_bamboo_spps_table(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_monopodial_bamboo_spps (
                id SERIAL PRIMARY KEY,
                plot_number VARCHAR(50) REFERENCES inventory_plot(plot_number),
                monopodial_bamboo_spps_position VARCHAR(50),
                species_name_code VARCHAR(50),
                species_name_scientific_name VARCHAR(100),
                species_name_vernacular_name VARCHAR(100),
                species_name_language_code VARCHAR(50),
                species_name_language_variety VARCHAR(100),
                bearing DECIMAL(5,2),
                distance DECIMAL(10,2),
                dbh DECIMAL(5,2),
                height_m DECIMAL(5,2),
                internode_distance_cm DECIMAL(5,2),
                total_no_of_culms INTEGER,
                remarks TEXT
            )
        """)
        self.stdout.write(self.style.SUCCESS('Created monopodial_bamboo_spps table'))

    def create_bamboo_assessment_table(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_bamboo_assessment (
                id SERIAL PRIMARY KEY,
                plot_number VARCHAR(50) REFERENCES inventory_plot(plot_number),
                bamboo_assessment_position VARCHAR(50),
                name_of_bamboo VARCHAR(100),
                x_coordinate DECIMAL(12,6),
                y_coordinate DECIMAL(12,6),
                no_of_culms INTEGER,
                photo_id VARCHAR(50),
                remarks TEXT
            )
        """)
        self.stdout.write(self.style.SUCCESS('Created bamboo_assessment table'))