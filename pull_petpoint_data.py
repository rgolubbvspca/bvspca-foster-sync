from delta_sharing import SharingClient
import pandas as pd
import json
from datetime import datetime
import numpy as np
from azure.storage.blob import BlobClient
import tempfile
import os

BEARER_TOKEN = os.environ.get('BEARER_TOKEN')
SHARE_URL = "https://eastus-c3.azuredatabricks.net/api/2.0/delta-sharing/metastores/13b9dbda-9b86-4bf1-a01d-bd41e8e72469"
STORAGE_ACCOUNT = "bvspcastorage"
CONTAINER_NAME = "foster-animals"
STORAGE_KEY = os.environ.get('STORAGE_KEY')

try:
    print("Connecting to Delta Share...")
    
    profile_content = {
        "shareCredentialsVersion": 1,
        "endpoint": SHARE_URL,
        "bearerToken": BEARER_TOKEN
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(profile_content, f)
        profile_path = f.name
    
    client = SharingClient(profile_path)
    rest = client._rest_client
    
    print("Finding tables...")
    all_tables = client.list_all_tables()
    
    animal_table = None
    details_table = None
    for table in all_tables:
        if table.name == "dimanimalcurrent":
            animal_table = table
        elif table.name == "dimanimaldetails":
            details_table = table
    
    if animal_table and details_table:
        rest.set_delta_format_header()
        
        print("Loading current animals...")
        files_response = rest.list_files_in_table(animal_table)
        parquet_url = None
        for line in files_response.lines:
            data = json.loads(line)
            if "file" in data and "deltaSingleAction" in data["file"]:
                add_action = data["file"]["deltaSingleAction"].get("add", {})
                if add_action and "path" in add_action:
                    parquet_url = add_action["path"]
                    break
        
        df = pd.read_parquet(parquet_url)
        print(f"Loaded {len(df)} total animals")
        
        print("Loading animal details...")
        details_response = rest.list_files_in_table(details_table)
        details_url = None
        for line in details_response.lines:
            data = json.loads(line)
            if "file" in data and "deltaSingleAction" in data["file"]:
                add_action = data["file"]["deltaSingleAction"].get("add", {})
                if add_action and "path" in add_action:
                    details_url = add_action["path"]
                    break
        
        details_df = pd.read_parquet(details_url)
        print(f"Loaded details for {len(details_df)} animals")
        
        foster_animals = df[df['Stage'].isin(['Needs Foster', 'Needs Foster - Available'])].copy()
        print(f"Found {len(foster_animals)} animals needing foster")
        
        foster_with_details = foster_animals.merge(
            details_df, 
            on='AnimalID', 
            how='left',
            suffixes=('', '_detail')
        )
        
        foster_with_details = foster_with_details.astype(object).where(pd.notna(foster_with_details), None)
        records = foster_with_details.to_dict('records')
        
        def clean_record(record):
            for key, value in record.items():
                if pd.isna(value):
                    record[key] = None
                elif isinstance(value, (pd.Timestamp, np.datetime64)):
                    record[key] = str(value) if pd.notna(value) else None
            return record
        
        records = [clean_record(r) for r in records]
        
        output = {
            "last_updated": datetime.now().isoformat(),
            "animal_count": len(records),
            "animals": records
        }
        
        json_str = json.dumps(output, indent=2, default=str)
        
        print("Uploading to blob storage...")
        blob_client = BlobClient.from_connection_string(
            f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT};AccountKey={STORAGE_KEY};EndpointSuffix=core.windows.net",
            container_name=CONTAINER_NAME,
            blob_name="foster_animals.json"
        )
        blob_client.upload_blob(json_str, overwrite=True)
        
        print(f"✓ Saved {len(records)} foster animals to blob storage")
        os.unlink(profile_path)

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
