"""
═══════════════════════════════════════════════════════════════════════
PAYPAL SUBSCRIPTIONS DIAGNOSTIC - SPARK DECLARATIVE PIPELINES
═══════════════════════════════════════════════════════════════════════
Copy everything below this line and paste into a Databricks notebook cell
This tests the ACTUAL pipeline ingestion path
"""

print("="*70)
print("PAYPAL SUBSCRIPTIONS - SDP PIPELINE DIAGNOSTIC")
print("="*70)

# Import the pipeline components
from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

# Step 1: Register the PayPal connector
print("\n[STEP 1] Registering PayPal connector...")
try:
    register_lakeflow_source = get_register_function("paypal")
    register_lakeflow_source(spark)
    print("✅ Connector registered successfully")
except Exception as e:
    print(f"❌ Failed to register connector: {e}")
    raise

# Step 2: Create minimal pipeline spec for subscriptions only
print("\n[STEP 2] Creating pipeline spec for subscriptions...")

subscription_ids = ["I-S45EDF98N3AV", "I-FWEC6DA9AKVJ", "I-D6JVS5Y2V12P", "I-GN66ML8DL6NC", "I-BJK104AU5722"]

pipeline_spec = {
    "connection_name": "paypal_v2",
    "objects": [{
        "table": {
            "source_table": "subscriptions",
            "destination_catalog": "alex_owen_the_unity_catalog",
            "destination_schema": "paypal_comunity_connector_a0",
            "destination_table": "subscriptions_diagnostic_test",
            "table_configuration": {
                "scd_type": "SCD_TYPE_1",
                "subscription_ids": subscription_ids
            }
        }
    }]
}

print(f"✅ Pipeline spec created")
print(f"   Connection: {pipeline_spec['connection_name']}")
print(f"   Source table: subscriptions")
print(f"   Destination: alex_owen_the_unity_catalog.paypal_comunity_connector_a0.subscriptions_diagnostic_test")
print(f"   Subscription IDs: {len(subscription_ids)} IDs")

# Step 3: Run ingestion
print("\n[STEP 3] Running ingestion pipeline...")
print("-" * 70)

try:
    ingest(spark, pipeline_spec)
    print("-" * 70)
    print("✅ Ingestion completed without errors")
    
except Exception as e:
    error_msg = str(e)
    print("-" * 70)
    print(f"❌ Ingestion failed: {error_msg}")
    
    # Check for specific error types
    if "not allowed by connection" in error_msg or "OPTION_NOT_ALLOWED" in error_msg:
        print("\n" + "="*70)
        print("PROBLEM: subscription_ids NOT in externalOptionsAllowList")
        print("="*70)
        print("\nFIX: Update your connection with this SQL:")
        print("""
DROP CONNECTION IF EXISTS paypal_v2;

CREATE CONNECTION paypal_v2 TYPE LAKEFLOW OPTIONS (
    client_id = 'Acpqp1DwjKoGnOxGJllN1BeS0PBc-thgcOMy2cgnSm0o67X8ReSGCSA2DBQ0_wstTV16AoolSc_l3Ja5',
    client_secret = 'EPJMwfXdAM-bUftXFuaLYziuTTWvwwzaC3ym4dCmBc4FWz7BMDvuyr3dyoFwZz8-PV7oh8WpfKnEDCF8',
    environment = 'sandbox',
    externalOptionsAllowList = 'start_date,end_date,page_size,subscription_ids,include_transactions,product_id,plan_ids'
);
        """)
    else:
        print("\nFull error details:")
        import traceback
        traceback.print_exc()
    
    # Stop here if ingestion failed
    print("\n" + "="*70)
    print("DIAGNOSTIC FAILED - Fix the error above and re-run")
    print("="*70)
    raise

# Step 4: Check the results
print("\n[STEP 4] Checking ingested data...")

try:
    result_df = spark.sql("""
        SELECT * FROM alex_owen_the_unity_catalog.paypal_comunity_connector_a0.subscriptions_diagnostic_test
    """)
    
    count = result_df.count()
    print(f"✅ Query successful: {count} records in table")
    
    if count > 0:
        print("\n" + "="*70)
        print("✅✅✅ SUCCESS - PIPELINE IS WORKING! ✅✅✅")
        print("="*70)
        print("\nSubscription records:")
        result_df.select("id", "status", "plan_id", "create_time").show(10, truncate=False)
        
        print("\n" + "="*70)
        print("NEXT STEPS:")
        print("="*70)
        print("1. Your subscriptions table should now have data")
        print("2. Check your actual subscriptions table:")
        print("   SELECT * FROM alex_owen_the_unity_catalog.paypal_comunity_connector_a0.subscriptions")
        print("3. If that table is still empty, check your ingest.py:")
        print("   - Verify subscription_ids is in table_configuration")
        print("   - Make sure the subscription IDs match your test data")
        print("\n4. Clean up this diagnostic table:")
        print("   DROP TABLE alex_owen_the_unity_catalog.paypal_comunity_connector_a0.subscriptions_diagnostic_test")
        
    else:
        print("\n" + "="*70)
        print("❌ PROBLEM: Pipeline ran but 0 records written")
        print("="*70)
        print("\nPossible causes:")
        print("1. Connector code has offset bug (not returning same offset)")
        print("2. Subscription IDs are invalid")
        print("3. PayPal API authentication issue")
        print("\nDEBUG: Check connector logs above for API errors")
        print("\nFIX: Make sure you have the latest connector code:")
        print("     - Pull latest from repo: git pull origin master")
        print("     - Re-upload sources/paypal/_generated_paypal_python_source.py")
        
except Exception as e:
    print(f"❌ Failed to query results: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*70)
print("DIAGNOSTIC COMPLETE")
print("="*70)

