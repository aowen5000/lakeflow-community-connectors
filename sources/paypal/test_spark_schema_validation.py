"""
Test that replicates EXACTLY what Spark does in the pipeline
This will show the actual error that's causing empty table
Run this in Databricks
"""

import sys
sys.path.insert(0, '/Workspace/Users/alex.owen@databricks.com/Artifacts_paypal_comunity_connector/paypal_hackathon_pipeline_a0')

from sources.paypal.paypal import LakeflowConnect
from pyspark.sql import SparkSession
import json

print("="*70)
print("SPARK SCHEMA VALIDATION TEST")
print("="*70)

# Get connector instance
connector = LakeflowConnect({
    "client_id": "Acpqp1DwjKoGnOxGJllN1BeS0PBc-thgcOMy2cgnSm0o67X8ReSGCSA2DBQ0_wstTV16AoolSc_l3Ja5",
    "client_secret": "EPJMwfXdAM-bUftXFuaLYziuTTWvwwzaC3ym4dCmBc4FWz7BMDvuyr3dyoFwZz8-PV7oh8WpfKnEDCF8",
    "environment": "sandbox"
})

# Get schema from connector
schema = connector.get_table_schema("subscriptions", {})
print("\n✅ Schema retrieved")
print(f"Schema has {len(schema.fields)} fields")

# Get data from connector
subscription_ids = ["I-S45EDF98N3AV", "I-FWEC6DA9AKVJ", "I-D6JVS5Y2V12P", "I-GN66ML8DL6NC", "I-BJK104AU5722"]
records_iter, next_offset = connector.read_table("subscriptions", None, {"subscription_ids": subscription_ids})
records = list(records_iter)

print(f"\n✅ Connector returned {len(records)} records")

if records:
    print("\n" + "="*70)
    print("TEST 1: Check first record structure")
    print("="*70)
    
    first_record = records[0]
    
    # Check which fields are in the data
    print("\nFields in API response:")
    for key in sorted(first_record.keys()):
        print(f"  - {key}: {type(first_record[key]).__name__}")
    
    # Check which fields are in the schema
    print("\nFields in schema:")
    for field in schema.fields:
        print(f"  - {field.name}: {field.dataType} (nullable={field.nullable})")
    
    # Find mismatches
    print("\n" + "="*70)
    print("TEST 2: Field Mismatches")
    print("="*70)
    
    data_fields = set(first_record.keys())
    schema_fields = set([f.name for f in schema.fields])
    
    extra_in_data = data_fields - schema_fields
    missing_in_data = schema_fields - data_fields
    
    if extra_in_data:
        print(f"\n⚠️  Extra fields in data (not in schema): {extra_in_data}")
        print("   These fields will be DROPPED by Spark")
    
    if missing_in_data:
        print(f"\n⚠️  Missing fields in data (expected by schema): {missing_in_data}")
        for field_name in missing_in_data:
            field = [f for f in schema.fields if f.name == field_name][0]
            if not field.nullable:
                print(f"   ❌ CRITICAL: '{field_name}' is NOT NULLABLE but missing from data!")
    
    if not extra_in_data and not missing_in_data:
        print("\n✅ All fields match!")
    
    # Try to create DataFrame
    print("\n" + "="*70)
    print("TEST 3: Create Spark DataFrame (THE REAL TEST)")
    print("="*70)
    
    try:
        df = spark.createDataFrame(records, schema)
        count = df.count()
        
        if count == len(records):
            print(f"\n✅✅✅ SUCCESS! DataFrame created with {count} records!")
            print("\nSample data:")
            df.select("id", "status", "plan_id", "create_time").show(5, truncate=False)
            
            print("\n" + "="*70)
            print("CONCLUSION: Schema is correct, pipeline should work!")
            print("="*70)
            print("\nIf pipeline still fails, check:")
            print("1. Connection configuration in Databricks")
            print("2. externalOptionsAllowList includes 'subscription_ids'")
            print("3. Destination table permissions")
            
        else:
            print(f"\n⚠️  DataFrame created but has {count} records instead of {len(records)}")
            
    except Exception as e:
        print(f"\n❌ FAILED TO CREATE DATAFRAME!")
        print(f"\nError: {e}")
        print("\n" + "="*70)
        print("DETAILED ERROR:")
        print("="*70)
        import traceback
        traceback.print_exc()
        
        print("\n" + "="*70)
        print("DIAGNOSIS:")
        print("="*70)
        error_str = str(e).lower()
        
        if "cannot cast" in error_str or "type mismatch" in error_str:
            print("❌ TYPE MISMATCH: A field has wrong data type")
            print("   Check which field types don't match between API and schema")
        elif "not nullable" in error_str or "null value" in error_str:
            print("❌ NULL VALUE: A non-nullable field has null value")
            print("   Check which required fields are missing from API response")
        elif "field" in error_str and "not found" in error_str:
            print("❌ MISSING FIELD: Schema expects a field that's not in data")
        else:
            print("❌ UNKNOWN ERROR: See detailed error above")
            
else:
    print("\n❌ No records returned from connector!")

print("\n" + "="*70)
print("TEST COMPLETE")
print("="*70)

