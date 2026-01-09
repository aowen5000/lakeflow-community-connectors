"""
Test suite for PayPal LakeflowConnect connector
"""
import sys
import os
import json
from pathlib import Path

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from sources.paypal.paypal import LakeflowConnect  # noqa: E402
from tests import test_suite  # noqa: E402
from tests.test_suite import LakeflowConnectTester  # noqa: E402


def load_config(config_path):
    """Load configuration from JSON file"""
    with open(config_path, 'r') as f:
        return json.load(f)


if __name__ == "__main__":
    # Inject the LakeflowConnect class into test_suite module's namespace
    # This is required because test_suite.py expects LakeflowConnect to be available
    test_suite.LakeflowConnect = LakeflowConnect
    
    # Load configurations
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    table_config_path = parent_dir / "configs" / "dev_table_config.json"
    
    config = load_config(config_path)
    table_config = load_config(table_config_path)
    
    # Create tester
    tester = LakeflowConnectTester(init_options=config, table_configs=table_config)
    
    # Run all tests
    report = tester.run_all_tests()
    
    # Print results
    tester.print_report(report, show_details=True)
