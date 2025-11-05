#!/usr/bin/env python3
"""
Convert JSON output to Excel file
Usage: python json_to_excel.py
"""
import json
import os
import pandas as pd

JSON_FILE = "data/fmit_data.json"
EXCEL_FILE = "data/fmit_data.xlsx"

def convert_json_to_excel():
    """Convert JSON file to Excel format."""
    if not os.path.exists(JSON_FILE):
        print(f"❌ JSON file not found: {JSON_FILE}")
        return
    
    print(f"📖 Reading JSON file: {JSON_FILE}")
    
    try:
        with open(JSON_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        if not isinstance(data, list):
            print(f"❌ JSON file is not an array. Current format: {type(data)}")
            return
        
        if len(data) == 0:
            print("⚠️  JSON file is empty. No data to convert.")
            return
        
        print(f"✅ Found {len(data)} records in JSON file")
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Ensure columns exist
        for col in ["url", "h1", "h2", "content"]:
            if col not in df.columns:
                df[col] = ""
        
        # Reorder columns
        df = df[["url", "h1", "h2", "content"]]
        
        # Convert to Excel
        print(f"💾 Converting to Excel: {EXCEL_FILE}")
        df.to_excel(EXCEL_FILE, index=False, engine='openpyxl')
        
        file_size = os.path.getsize(EXCEL_FILE) / 1024 / 1024
        print(f"✅ Successfully converted to Excel!")
        print(f"   File: {EXCEL_FILE}")
        print(f"   Records: {len(df)}")
        print(f"   Size: {file_size:.2f} MB")
        print(f"\n💡 You can now open {EXCEL_FILE} in Excel!")
        
    except Exception as e:
        print(f"❌ Error converting JSON to Excel: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    convert_json_to_excel()

