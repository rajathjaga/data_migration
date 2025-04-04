import pandas as pd
import json
import os
import sqlalchemy
from sqlalchemy.dialects.postgresql import JSONB

def load_to_rds(engine, table_name, json_file_path):
    try:
        with open(json_file_path, 'r') as file:
            data = json.load(file)
        
        if isinstance(data, dict):
            records = [data]
        elif isinstance(data, list):
            records = data
        else:
            print(f"Unexpected data type in {json_file_path}: {type(data)}")
            return
        
        records_to_insert = []
        for record_data in records:
            if not isinstance(record_data, dict):
                print(f"Skipping non-dictionary item in the list: {type(record_data)}")
                continue
                
            cik = record_data.get('cik', '')
            entity_name = record_data.get('entityName', '')
            facts = record_data.get('facts', {})
            file_name = os.path.basename(json_file_path)
            
            record = {
                'cik': cik,
                'entity_name': entity_name,
                'file_name': file_name,
                'facts': facts
            }
            records_to_insert.append(record)
        
        if not records_to_insert:
            print("No valid records found to insert")
            return
            
        df = pd.DataFrame(records_to_insert)
        dtypes = {'cik': sqlalchemy.types.Integer, 'entity_name': sqlalchemy.types.String(255), 'facts': JSONB}
        df.to_sql(table_name, engine, if_exists='replace', index=False, dtype=dtypes)

        print(f"Successfully loaded {len(records_to_insert)} records to {table_name} table")
    except json.JSONDecodeError as e:
        print(f"JSON decode error in file {json_file_path}: {str(e)}")
    except Exception as e:
        print(f"Error processing {json_file_path} for RDS: {str(e)}")
        raise