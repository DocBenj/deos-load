import os
import clickhouse_connect
import pandas as pd
from dotenv import load_dotenv

def connect():
    # 1. Load credentials from your .env file
    load_dotenv()
    
    try:
        # 2. Initialize the client
        # We use os.getenv to pull values set in your .env file
        client = clickhouse_connect.get_client(
            host=os.getenv('CH_HOST'),
            port=8443, # Default to 8443 if not specified
            user='default',
            password=os.getenv('CH_PASSWORD'),
            database='deos_dev',
            secure=True  # Required for ClickHouse Cloud/AWS
        )
        
        # 3. Verification Query
        # A simple query to prove the connection is "alive"
        server_version = client.command('SELECT version()')
        
        print("✅ Connection Successful!")
        print(f"Connected to ClickHouse server version: {server_version}")
        
        return client

    except Exception as e:
        print("❌ Connection Failed.")
        print(f"Error details: {e}")
        return None


def validate_and_gatekeep():
    # Define the files and their expected column schemas
    required_files = ['activity_area_metrics.csv', 'actual_consumption.csv','aeecr_projects.csv','completed_projects.csv','elec_generation.csv',
                     'elec_util_aux.csv','energy_audit.csv','establishments.csv','production_volume.csv','purchased_electricity.csv',
                      'submissions.csv','transportation_energy_use.csv','waste_oil.csv']
    if 'SchemaValidationError' not in globals():
        class SchemaValidationError(Exception): pass
    errors = []
    file_configs = {
        'activity_area_metrics.csv': [
            'metrics_id', 'submission_id', 'establishment_id', 'activity_area',
       'floor_area', 'hours_of_operation', 'hours_shut_down',
       'rated_stream_hours'
        ],
        'actual_consumption.csv': [
            'establishment_id', 'submission_id', 'fuel_type_name', 'unit_name',
       'year', 'actual_consumptions'
        ],
        'aeecr_projects.csv': [
            'project_id', 'submission_id', 'project_name', 'start_date',
       'completion_date', 'current_energy_savings',
       'current_energy_savings_unit', 'completion_energy_savings',
       'total_investment'
        ],
        'completed_projects.csv': [
            'aeecr_completed_projects_table_id', 'submission_id',
       'establishment_id', 'project_name', 'start_date', 'date_completed',
       'energy_savings', 'energy_savings_unit', 'total_investment'
        ],
        'elec_generation.csv': [
            'aeur_elec_generation_id', 'submission_id', 'establishment_id',
       'generating_unit', 'fuel_type', 'capacity_rating',
       'consumption_quantity', 'hours_of_operation', 'generated_electricity',
       'kwh'
        ],
        'elec_util_aux.csv': [
            'aux_activity_area_id', 'submission_id', 'establishment_id',
       'fuel_type', 'aux_activity_area', 'energy_consumption', 'kwh'
        ],
        'energy_audit.csv': [
            'energy_audit_table_id', 'submission_id', 'establishment_id',
       'conducted_level', 'auditor_name', 'year_conducted',
       'total_energy_savings', 'total_investment', 'eei_eui_sec_value',
       'eei_eui_sec_unit', 'recommendations', 'other_recommendations'
        ],
        'establishments.csv': [
            'establishment_id', 'establishment_name', 'sector_type',
       'business_type', 'address', 'region', 'province', 'city',
       'year_founded', 'lat', 'lon', 'status'
        ],
        'production_volume.csv': [
            'id', 'submission_id', 'establishment_id', 'product_line', 'unit',
       'quantity', 'rated_capacity', 'psic_code', 'psic_name'
        ],
        'purchased_electricity.csv': [
            'purchased_electricity_id', 'submission_id', 'establishment_id',
       'distribution_utility', 'energy_consumption', 'start_date', 'end_date',
       'kwh'
        ],
        'submissions.csv': [
            'submission_id', 'establishment_id', 'submission_type_name',
       'submission_report_year', 'submission_male_employees',
       'submission_female_employees', 'submission_progress', 'submitted',
       'submission_total_kwh'
        ],
        'transportation_energy_use.csv': [
            'transportation_energy_use_id', 'submission_id', 'establishment_id',
       'fuel_type', 'quantity', 'activity', 'kwh'
        ],
        'waste_oil.csv': [
            'waste_oil_utilization_id', 'submission_id', 'establishment_id',
       'waste_oil_collected', 'waste_oil_sold', 'waste_oil_recycled',
       'lube_oil_consumption'
        ],
    }

    results = {}

            
    for file_name, expected_cols in file_configs.items():
        print(f"\n🔍 Checking: {file_name}")
        file_path = os.path.join(os.getcwd(), file_name)

        if not os.path.exists(file_path):
            errors.append(f"Missing file: {file_name}")
            continue

        # Load only the first few rows to save memory during validation
        df = pd.read_csv(file_path, encoding='latin1', nrows=100)
        actual_cols = df.columns.tolist()

            # 1. Column Match Check
        missing = set(expected_cols) - set(actual_cols)
        extra = set(actual_cols) - set(expected_cols)

        if not missing:
            print(f"✅ Columns match! ({len(actual_cols)} total)")
        else:
            errors.append(f"Failed Schema: {file_name}")
    if errors:
        print("🛑 STOPPING EXECUTION: Data issues detected.")
        for err in errors:
            print(f"  - {err}")
        raise SchemaValidationError("Validation failed. Please fix the files and Restart & Run All.")

    print("✅ Validation successful! Moving to next step...")

    return results


def upload_csvs_to_clickhouse():
    from utils import connect
    from clickhouse_connect.driver.tools import insert_file
    import pandas as pd
    df = pd.read_csv('submissions.csv')
    df.to_csv('submissions.csv', index=False)
    client = connect()
    client.command('''TRUNCATE TABLE IF EXISTS activity_area_metrics ''')
    client.command('''TRUNCATE TABLE IF EXISTS actual_consumption''')
    client.command('''TRUNCATE TABLE IF EXISTS aeecr_projects''')
    client.command('''TRUNCATE TABLE IF EXISTS completed_projects ''')
    client.command('''TRUNCATE TABLE IF EXISTS elec_generation''')
    client.command('''TRUNCATE TABLE IF EXISTS elec_util_aux''')
    client.command('''TRUNCATE TABLE IF EXISTS energy_audit ''')
    client.command('''TRUNCATE TABLE IF EXISTS establishments''')
    client.command('''TRUNCATE TABLE IF EXISTS production_volume''')
    client.command('''TRUNCATE TABLE IF EXISTS purchased_electricity ''')
    client.command('''TRUNCATE TABLE IF EXISTS submissions''')
    client.command('''TRUNCATE TABLE IF EXISTS transportation_energy_use''')
    client.command('''TRUNCATE TABLE IF EXISTS waste_oil''')
    # load data 
    insert_file(client, 'activity_area_metrics', 'activity_area_metrics.csv')
    insert_file(client, 'actual_consumption', 'actual_consumption.csv')
    insert_file(client, 'aeecr_projects', 'aeecr_projects.csv')
    insert_file(client, 'completed_projects', 'completed_projects.csv')
    insert_file(client, 'elec_generation', 'elec_generation.csv')
    insert_file(client, 'elec_util_aux', 'elec_util_aux.csv')
    insert_file(client, 'energy_audit', 'energy_audit.csv')
    insert_file(client, 'establishments', 'establishments.csv')
    insert_file(client, 'production_volume', 'production_volume.csv')
    insert_file(client, 'purchased_electricity', 'purchased_electricity.csv')
    insert_file(client, 'submissions', 'submissions.csv')
    insert_file(client, 'transportation_energy_use', 'transportation_energy_use.csv')
    insert_file(client, 'waste_oil', 'waste_oil.csv')
    print(f"✅ Data Upload Successfull")

def delete_project_csvs():
    import os
    # List of files to target
    files_to_delete = ['activity_area_metrics.csv', 'actual_consumption.csv','aeecr_projects.csv','completed_projects.csv','elec_generation.csv',
                     'elec_util_aux.csv','energy_audit.csv','establishments.csv','production_volume.csv','purchased_electricity.csv',
                      'submissions.csv','transportation_energy_use.csv','waste_oil.csv']
    
    deleted_count = 0
    
    for file_name in files_to_delete:
        # Construct the full path (CWD + filename)
        file_path = os.path.join(os.getcwd(), file_name)
        
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                print(f"🗑️ Successfully deleted: {file_name}")
                deleted_count += 1
            except Exception as e:
                print(f"❌ Error deleting {file_name}: {e}")
        else:
            print(f"ℹ️ File not found, skipping: {file_name}")
            
    print(f"\nCleanup complete. Total files removed: {deleted_count}")
    