"""
DIRECT CONNECTOR TEST - NO SDP REQUIRED
This tests the PayPal connector directly without using Spark Declarative Pipelines
Copy and paste into Databricks notebook
"""

import sys
REPO_PATH = '/Workspace/Users/alex.owen@databricks.com/Artifacts_paypal_comunity_connector/paypal_hackathon_pipeline_a0'
sys.path.insert(0, REPO_PATH)

print("="*70)
print("DIRECT CONNECTOR TEST - SUBSCRIPTIONS FIX VERIFICATION")
print("="*70)

# Import the connector directly
from sources.paypal.paypal import LakeflowConnect

# Initialize connector with your credentials
connector = LakeflowConnect({
    "client_id": "Acpqp1DwjKoGnOxGJllN1BeS0PBc-thgcOMy2cgnSm0o67X8ReSGCSA2DBQ0_wstTV16AoolSc_l3Ja5",
    "client_secret": "EPJMwfXdAM-bUftXFuaLYziuTTWvwwzaC3ym4dCmBc4FWz7BMDvuyr3dyoFwZz8-PV7oh8WpfKnEDCF8",
    "environment": "sandbox"
})

print("✅ Connector initialized")

# Test subscriptions with all 5 IDs
subscription_ids = ["I-S45EDF98N3AV", "I-FWEC6DA9AKVJ", "I-D6JVS5Y2V12P", "I-GN66ML8DL6NC", "I-BJK104AU5722"]

table_options = {
    "subscription_ids": subscription_ids
}

print(f"\n📋 Testing with {len(subscription_ids)} subscription IDs")
print("-"*70)

# Call read_table method directly
print("\n[TEST 1] First call (should return all 5 subscriptions)")
try:
    records_iter, next_offset = connector.read_table("subscriptions", None, table_options)
    records = list(records_iter)
    
    print(f"✅ Returned {len(records)} records")
    print(f"✅ Next offset: {next_offset}")
    
    if len(records) == 5:
        print("\n" + "🎉"*20)
        print("✅✅✅ SUCCESS! NEW CODE IS WORKING!")
        print("🎉"*20)
        print("\nAll 5 subscriptions retrieved:")
        for i, rec in enumerate(records, 1):
            print(f"  {i}. {rec.get('id')}: {rec.get('status')} (plan: {rec.get('plan_id')})")
            
        print("\n✅ Fix is confirmed! Your subscriptions table should now work in production!")
        
    elif len(records) == 1:
        print("\n" + "="*70)
        print("❌ PROBLEM: OLD CODE STILL RUNNING!")
        print("="*70)
        print("\nOnly 1 record means the old pagination bug is still active.")
        print("\nFIX:")
        print("1. In Databricks: Compute → Your Cluster → Restart")
        print("2. After restart, re-run this test")
        print("3. If still failing, verify you pulled latest code:")
        print(f"   cd {REPO_PATH}")
        print("   git pull origin master")
        
    elif len(records) == 0:
        print("\n❌ 0 records returned")
        print("\nPossible causes:")
        print("1. API authentication issue")
        print("2. Subscription IDs are invalid or don't exist")
        print("3. Check error messages above")
        
    else:
        print(f"\n⚠️  Unexpected: {len(records)} records")
        print("Expected 5 or 1 (if old code)")
        
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()

# Test pagination completion
print("\n" + "-"*70)
print("[TEST 2] Second call with 'done' offset (should return 0 records)")
try:
    records_iter, next_offset = connector.read_table("subscriptions", {"done": True}, table_options)
    records = list(records_iter)
    
    print(f"✅ Returned {len(records)} records")
    print(f"✅ Next offset: {next_offset}")
    
    if len(records) == 0 and next_offset.get("done"):
        print("✅ Pagination logic correct!")
    else:
        print("⚠️  Pagination may have issues")
        
except Exception as e:
    print(f"❌ ERROR: {e}")

print("\n" + "="*70)
print("TEST COMPLETE")
print("="*70)

