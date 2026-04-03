import re
import json
import pandas as pd
import numpy as np
import os
from datetime import datetime

# Paths
# Adjust paths as needed for your specific environment
SQL_PATH = 'nimbus_core.sql'
MONGO_PATH = 'nimbus_events.js'

def parse_sql(file_path):
    """
    Parses the PostgreSQL INSERT INTO statements from nimbus_core.sql.
    Returns a dictionary of DataFrames.
    """
    tables = {}
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # regex matches INSERT INTO table_name (columns) VALUES (v1, v2, ...);
    # The table name might be prefixed with "nimbus." or not.
    # We use a pattern that handles both.
    insert_pattern = re.compile(r"INSERT INTO\s+(?:nimbus\.)?(\w+)\s*\((.*?)\)\s*VALUES\s*(.*?);", re.DOTALL | re.IGNORECASE)
    
    print(f"Searching for SQL inserts in {file_path}...")
    matches = list(insert_pattern.finditer(content))
    print(f"Found {len(matches)} INSERT statements.")
    
    for match in matches:
        table_name = match.group(1).lower()
        cols = [c.strip() for c in match.group(2).split(',')]
        values_str = match.group(3).strip()
        
        # Split rows (v1, v2, ...), (v1, v2, ...) while considering possible nested commas in strings
        rows = []
        # Finding everything between ( ) that forms a single row insert
        # We need a robust way to split rows. Rows are separated by "),"
        # But we must be careful with commas inside strings.
        
        # Split by "), (" or similar.
        # A better approach: 
        row_contents = []
        current_row = ""
        in_row = False
        in_quote = False
        
        # Simplified row extractor: find all ( ... ) blocks
        # This works if row values don't contain literal "),"
        row_matches = re.finditer(r"\((.*?)\)(?:,\s*|\s*$)", values_str, re.DOTALL)
        
        for rm in row_matches:
            val_list = []
            current_val = ""
            in_q = False
            for char in rm.group(1):
                if char == "'" and not in_q: in_q = True
                elif char == "'" and in_q: in_q = False
                elif char == "," and not in_q:
                    val_list.append(current_val.strip())
                    current_val = ""
                else:
                    current_val += char
            val_list.append(current_val.strip())
            
            # Clean values
            cleaned_vals = []
            for v in val_list:
                v = v.strip()
                if v.upper() == 'NULL':
                    cleaned_vals.append(None)
                elif v.startswith("'") and v.endswith("'"):
                    cleaned_vals.append(v[1:-1])
                else:
                    # Try numeric conversion
                    try:
                        if '.' in v: cleaned_vals.append(float(v))
                        else: cleaned_vals.append(int(v))
                    except:
                        cleaned_vals.append(v)
            
            if len(cleaned_vals) == len(cols):
                rows.append(cleaned_vals)
            else:
                # print(f"Column mismatch in {table_name}: expected {len(cols)}, got {len(cleaned_vals)}")
                pass
        
        df_new = pd.DataFrame(rows, columns=cols)
        if table_name in tables:
            tables[table_name] = pd.concat([tables[table_name], df_new], ignore_index=True)
        else:
            tables[table_name] = df_new
            
    return tables

def parse_mongo(file_path):
    """
    Parses the MongoDB insertMany statements from nimbus_events.js.
    """
    collections = {}
    
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    current_coll = None
    objs = []
    
    print(f"Parsing MongoDB collections in {file_path}...")
    
    for line in lines:
        coll_match = re.search(r"db\.(\w+)\.insertMany\(\[", line)
        if coll_match:
            current_coll = coll_match.group(1)
            objs = []
            continue
        
        if "]);" in line:
            if current_coll and objs:
                collections[current_coll] = pd.DataFrame(objs)
                print(f"Loaded {len(objs)} records into {current_coll}")
            current_coll = None
            continue
        
        if current_coll:
            cleaned_line = line.strip()
            if cleaned_line.endswith(','): cleaned_line = cleaned_line[:-1]
            if not cleaned_line.startswith('{'): continue
            
            # Replace JS-specific parts for JSON parsing
            cleaned_line = re.sub(r'ISODate\("(.*?)"\)', r'"\1"', cleaned_line)
            
            try:
                objs.append(json.loads(cleaned_line))
            except:
                # Handle cases where keys aren't quoted "key": value
                try:
                    # Very basic fix for unquoted keys: { key: value } -> { "key": value }
                    # This is risky but often works for simple objects
                    fixed = re.sub(r'(\w+):', r'"\1":', cleaned_line)
                    objs.append(json.loads(fixed))
                except:
                    pass

    return collections

def clean_data(sql_data, mongo_data):
    """
    Standardize the data for Option A: Churn Analysis.
    """
    # SQL Coersion
    if 'customers' in sql_data:
        cust = sql_data['customers'].copy()
        # Coerce boolean/numbers
        cust['is_active'] = cust['is_active'].astype(str).str.upper().map({'TRUE': True, 'FALSE': False, '1': True, '0': False, '1.0': True, '0.0': False})
        cust['nps_score'] = pd.to_numeric(cust['nps_score'], errors='coerce')
        # date parsing
        cust['signup_date'] = pd.to_datetime(cust['signup_date'], errors='coerce')
        cust['churned_at'] = pd.to_datetime(cust['churned_at'], errors='coerce')
        sql_data['customers'] = cust
        
    if 'subscriptions' in sql_data:
        sub = sql_data['subscriptions'].copy()
        sub['start_date'] = pd.to_datetime(sub['start_date'], errors='coerce')
        sub['end_date'] = pd.to_datetime(sub['end_date'], errors='coerce')
        sql_data['subscriptions'] = sub

    # Mongo Coersion
    if 'user_activity_logs' in mongo_data:
        ua = mongo_data['user_activity_logs'].copy()
        
        # Coalesce various user ID fields
        id_fields = ['member_id', 'userId', 'userID']
        for f in id_fields:
            if f in ua.columns:
                if 'final_member_id' not in ua.columns:
                    ua['final_member_id'] = ua[f]
                else:
                    ua['final_member_id'] = ua['final_member_id'].combine_first(ua[f])
        
        cust_fields = ['customer_id', 'customerId']
        for f in cust_fields:
            if f in ua.columns:
                if 'final_customer_id' not in ua.columns:
                    ua['final_customer_id'] = ua[f]
                else:
                    ua['final_customer_id'] = ua['final_customer_id'].combine_first(ua[f])

        # Convert to numeric
        ua['final_customer_id'] = pd.to_numeric(ua['final_customer_id'], errors='coerce')
        ua['final_member_id'] = pd.to_numeric(ua['final_member_id'], errors='coerce')

        # Clean timestamps
        def smart_to_datetime(ts):
            if pd.isna(ts) or ts == '': return None
            for fmt in [None, '%m/%d/%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%SZ']:
                try: return pd.to_datetime(ts, format=fmt)
                except: continue
            return None
        
        ua['timestamp'] = ua['timestamp'].apply(smart_to_datetime)
        
        # Drop old ID columns to avoid confusion
        ua = ua.drop(columns=[f for f in (id_fields + cust_fields) if f in ua.columns])
        ua = ua.rename(columns={'final_member_id': 'member_id', 'final_customer_id': 'customer_id'})
        
        ua.drop_duplicates(inplace=True)
        mongo_data['user_activity_logs'] = ua
        
    return sql_data, mongo_data

if __name__ == "__main__":
    print("--- Starting NimbusAI Data Processing ---")
    
    sql_tables = parse_sql(SQL_PATH)
    mongo_cols = parse_mongo(MONGO_PATH)
    
    sql_tables, mongo_cols = clean_data(sql_tables, mongo_cols)
    
    output_dir = "processed_data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    print("\nSaving DataFrames to CSV...")
    for name, df in sql_tables.items():
        print(f"-> SQL: {name} ({len(df)} rows)")
        df.to_csv(os.path.join(output_dir, f"{name}.csv"), index=False)
    
    for name, df in mongo_cols.items():
        print(f"-> Mongo: {name} ({len(df)} rows)")
        df.to_csv(os.path.join(output_dir, f"{name}.csv"), index=False)
    
    print("\nDone.")
