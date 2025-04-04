import pandas as pd
import xml.etree.ElementTree as ET
import os

def ensure_directory_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def transform_csv(file_path):
    try:
        df = pd.read_csv(file_path)

        ensure_directory_exists("data/transformed")

        # Save as JSON
        transformed_path = os.path.join("data", "transformed", os.path.basename(file_path).replace('.csv', '.json'))
        df.to_json(transformed_path, orient='records', lines=True)

        return transformed_path
    except Exception as e:
        print(f"Error during CSV transformation: {e}")
        return None

def transform_json(file_path):
    try:
        df = pd.read_json(file_path, lines=True)

        ensure_directory_exists("data/transformed")

        # Save as JSON (keeping format)
        transformed_path = os.path.join("data", "transformed", os.path.basename(file_path))
        df.to_json(transformed_path, orient='records', lines=True)

        return transformed_path
    except Exception as e:
        print(f"Error during JSON transformation: {e}")
        return None

def transform_xml(file_path):
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        data = []

        for element in root:
            record = {}
            for child in element:
                record[child.tag] = child.text
            data.append(record)

        df = pd.DataFrame(data)

        ensure_directory_exists("data/transformed")

        # Save as JSON
        transformed_path = os.path.join("data", "transformed", os.path.basename(file_path).replace('.xml', '.json'))
        df.to_json(transformed_path, orient='records', lines=True)
        
        return transformed_path
    except Exception as e:
        print(f"Error during XML transformation: {e}")
        return None
