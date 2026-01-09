"""
═══════════════════════════════════════════════════════════════════════
PAYPAL SUBSCRIPTIONS DIAGNOSTIC - DATABRICKS NOTEBOOK
═══════════════════════════════════════════════════════════════════════
Copy everything below this line and paste into a Databricks notebook cell
"""

print("="*70)
print("PAYPAL SUBSCRIPTIONS TABLE DIAGNOSTIC")
print("="*70)

subscription_ids = ["I-S45EDF98N3AV", "I-FWEC6DA9AKVJ", "I-D6JVS5Y2V12P", "I-GN66ML8DL6NC", "I-BJK104AU5722"]

# TEST 1: Try to read subscriptions with subscription_ids option
print("\n[TEST 1] Testing if subscription_ids option is allowed...")
try:
    df = spark.read.format("lakeflow") \
        .option("connection", "paypal_v2") \
        .option("sourceTable", "subscriptions") \
        .option("subscription_ids", ",".join(subscription_ids)) \
        .load()
    
    print("✅ SUCCESS: subscription_ids option is ALLOWED")
    print("   (Connection has correct externalOptionsAllowList)")
    
    # Count records
    count = df.count()
    print(f"\n[TEST 2] Retrieved {count} subscription record(s)")
    
    if count > 0:
        print("\n✅ SUCCESS: Subscriptions data retrieved!")
        print("\nSubscription records:")
        df.select("id", "status", "plan_id").show(10, truncate=False)
        
        print("\n" + "="*70)
        print("✅✅✅ CONNECTOR IS WORKING CORRECTLY ✅✅✅")
        print("="*70)
        print("\nIf your destination table is empty, the issue is:")
        print("  1. Check your ingest.py has subscription_ids in table_configuration")
        print("  2. Make sure you're running the latest ingest.py code")
        print("  3. Verify the pipeline is actually ingesting subscriptions table")
        print("\nTry running JUST subscriptions table alone:")
        print("""
from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

register_lakeflow_source = get_register_function("paypal")
register_lakeflow_source(spark)

pipeline_spec = {
    "connection_name": "paypal_v2",
    "objects": [{
        "table": {
            "source_table": "subscriptions",
            "destination_catalog": "alex_owen_the_unity_catalog",
            "destination_schema": "paypal_comunity_connector_a0",
            "destination_table": "subscriptions",
            "table_configuration": {
                "scd_type": "SCD_TYPE_1",
                "subscription_ids": ["I-S45EDF98N3AV", "I-FWEC6DA9AKVJ", "I-D6JVS5Y2V12P", "I-GN66ML8DL6NC", "I-BJK104AU5722"]
            }
        }
    }]
}

ingest(spark, pipeline_spec)

# Check result
spark.sql("SELECT * FROM alex_owen_the_unity_catalog.paypal_comunity_connector_a0.subscriptions").show()
        """)
        
    else:
        print("\n❌ PROBLEM: 0 records returned")
        print("\nPossible causes:")
        print("  1. Connector code not updated in workspace (offset bug)")
        print("  2. Subscription IDs are invalid")
        print("  3. API authentication issue")
        print("\nFIX: Pull latest code to your Databricks workspace")
        print("     git pull origin master")
        print("     (Or re-upload sources/paypal/_generated_paypal_python_source.py)")
        
except Exception as e:
    error_msg = str(e)
    
    if "not allowed by connection" in error_msg or "OPTION_NOT_ALLOWED" in error_msg:
        print("\n❌ PROBLEM FOUND: subscription_ids is NOT ALLOWED by connection")
        print(f"\nError message: {error_msg}")
        print("\n" + "="*70)
        print("FIX: Update your connection with this SQL")
        print("="*70)
        print("""
-- Run this in a SQL cell or notebook:

DROP CONNECTION IF EXISTS paypal_v2;

CREATE CONNECTION paypal_v2 TYPE LAKEFLOW OPTIONS (
    client_id = 'Acpqp1DwjKoGnOxGJllN1BeS0PBc-thgcOMy2cgnSm0o67X8ReSGCSA2DBQ0_wstTV16AoolSc_l3Ja5',
    client_secret = 'EPJMwfXdAM-bUftXFuaLYziuTTWvwwzaC3ym4dCmBc4FWz7BMDvuyr3dyoFwZz8-PV7oh8WpfKnEDCF8',
    environment = 'sandbox',
    externalOptionsAllowList = 'start_date,end_date,page_size,subscription_ids,include_transactions,product_id,plan_ids'
);

-- Then re-run this diagnostic
        """)
    else:
        print(f"\n❌ UNEXPECTED ERROR: {error_msg}")
        print("\nFull traceback:")
        import traceback
        traceback.print_exc()

print("\n" + "="*70)
print("DIAGNOSTIC COMPLETE")
print("="*70)

