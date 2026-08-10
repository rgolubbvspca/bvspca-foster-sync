from delta_sharing import SharingClient
import pandas as pd
import json
from datetime import datetime
import numpy as np
import tempfile
import os

PROFILE_PATH = "delta_sharing_profile.json"
BEARER_TOKEN = "FJOUna1PwGLjyfIa9PEzr6fGpScou4q5bylgIhwa6WfvJIzRqIyPRD7Zp9DPWveq"
SHARE_URL = "https://eastus-c3.azuredatabricks.net/api/2.0/delta-sharing/metastores/13b9dbda-9b86-4bf1-a01d-bd41e8e72469"

try:
    print("Connecting to Delta Share...")
    
    # Create temporary profile file
    profile_content = {
        "shareCredentialsVersion": 1,
        "endpoint": SHARE_URL,
        "bearerToken": BEARER_TOKEN
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(profile_content, f)
        profile_path = f.name
    
    try:
        client = SharingClient(profile_path)
        rest = client._rest_client
        
        print("Finding tables...")
        all_tables = client.list_all_tables()
        
        animal_table = None
        details_table = None
        memos_table = None
        
        for table in all_tables:
            if table.name == "dimanimalcurrent":
                animal_table = table
            elif table.name == "dimanimaldetails":
                details_table = table
            elif table.name == "factmemo":
                memos_table = table
        
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
            print(f"✓ Loaded {len(df)} total animals")
            
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
            print(f"✓ Loaded details for {len(details_df)} animals")
            
            # Filter for foster animals
            foster_animals = df[df['Stage'].isin(['Needs Foster', 'Needs Foster - Available'])].copy()
            print(f"✓ Found {len(foster_animals)} animals needing foster")
            
            # Merge with details
            foster_with_details = foster_animals.merge(
                details_df, 
                on='AnimalID', 
                how='left',
                suffixes=('', '_detail')
            )
            
            # Load memos if available
            if memos_table:
                print("Loading memos...")
                try:
                    memos_response = rest.list_files_in_table(memos_table)
                    memos_url = None
                    for line in memos_response.lines:
                        data = json.loads(line)
                        if "file" in data and "deltaSingleAction" in data["file"]:
                            add_action = data["file"]["deltaSingleAction"].get("add", {})
                            if add_action and "path" in add_action:
                                memos_url = add_action["path"]
                                break
                    
                    if memos_url:
                        memos_df = pd.read_parquet(memos_url)
                        
                        # Filter for Foster Communications + Foster Plea
                        foster_memos = memos_df[
                            (memos_df['MemoType'] == 'Foster Communications') & 
                            (memos_df['MemoSubType'] == 'Foster Plea')
                        ].copy()
                        
                        # Get most recent memo per animal
                        foster_memos = foster_memos.sort_values('MemoCreateDate', ascending=False)
                        most_recent_memos = foster_memos.drop_duplicates(subset=['AnimalID'], keep='first')
                        
                        # Select AnimalID and MemoText
                        memo_data = most_recent_memos[['AnimalID', 'MemoText']].rename(
                            columns={'MemoText': 'FosterDetails'}
                        )
                        
                        # Join to animals
                        foster_with_details = foster_with_details.merge(
                            memo_data,
                            on='AnimalID',
                            how='left'
                        )
                        print(f"✓ Loaded foster memos for {len(most_recent_memos)} animals")
                except Exception as e:
                    print(f"Warning: Could not load memos: {e}")
                    foster_with_details['FosterDetails'] = None
            else:
                foster_with_details['FosterDetails'] = None
            
            # Clean data
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
            
            with open("foster_animals.json", "w") as f:
                json.dump(output, f, indent=2, default=str)
            
            print(f"\n✓ Saved {len(records)} foster animals to foster_animals.json")
    
    finally:
        os.unlink(profile_path)

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()