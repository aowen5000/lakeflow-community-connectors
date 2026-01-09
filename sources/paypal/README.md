# Lakeflow PayPal Community Connector

This documentation provides setup instructions and reference information for the PayPal source connector.

## Prerequisites

To use this connector, you need:

- A PayPal Business account with API access
- API credentials (Client ID and Client Secret) from the PayPal Developer Dashboard
- Access to the PayPal REST API (Transaction Search API v1)
- For production use: A verified PayPal Business account with appropriate API permissions
- For testing: Access to PayPal Sandbox environment

## Setup

### Required Connection Parameters

To configure the connector, provide the following parameters in your connection options:

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `client_id` | string | Yes | OAuth 2.0 Client ID from PayPal Developer Dashboard | `"AYourClientIdHere..."` |
| `client_secret` | string | Yes | OAuth 2.0 Client Secret from PayPal Developer Dashboard | `"EYourClientSecretHere..."` |
| `environment` | string | No | API environment: `"sandbox"` or `"production"`. Defaults to `"sandbox"` | `"sandbox"` |
| `externalOptionsAllowList` | string | Yes | Comma-separated list of table-specific options that can be configured per table | `"start_date,end_date,page_size,plan_id,start_time,end_time,subscription_ids,product_id,plan_ids,event_type,transaction_id,tracking_number"` |

**Note**: The `externalOptionsAllowList` parameter is **required** and must include all table-specific options you plan to use:
- **Core date/pagination**: `start_date`, `end_date`, `page_size`
- **Subscription filtering**: `subscription_ids`, `plan_id`, `product_id`, `plan_ids`
- **Event filtering**: `start_time`, `end_time`, `event_type`
- **Transaction filtering**: `transaction_id`, `tracking_number`, `disputed_transaction_id`

**Recommended full list**: `"start_date,end_date,page_size,plan_id,start_time,end_time,subscription_ids,include_transactions,product_id,plan_ids,event_type,transaction_id,tracking_number,disputed_transaction_id"`

### Obtaining PayPal API Credentials

Follow these steps to obtain your PayPal API credentials:

1. **Log in to PayPal Developer Dashboard**
   - Go to [https://developer.paypal.com/dashboard/](https://developer.paypal.com/dashboard/)
   - Sign in with your PayPal Business account credentials

2. **Navigate to Apps & Credentials**
   - Select the **Apps & Credentials** section
   - Choose **Sandbox** tab for testing or **Live** tab for production

3. **Create or Select an App**
   - Click **Create App** to create a new app, or select an existing one
   - Give your app a descriptive name (e.g., "Databricks Lakeflow Connector")

4. **Copy Credentials**
   - **Client ID**: Displayed at the top of the app details page
   - **Secret**: Click **Show** under "Secret" to reveal, then copy it
   - **Important**: Keep your Client Secret secure and never share it publicly

5. **API Permissions**
   - Ensure your app has access to the **Transaction Search API v1**
   - No additional permissions or scopes are required for basic transaction data access

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways:

1. **Via Lakeflow Community Connector UI**:
   - Navigate to the "Add Data" page in your Databricks workspace
   - Follow the Lakeflow Community Connector UI flow
   - Select PayPal as your source connector
   - Provide your `client_id`, `client_secret`, and `environment`
   - Set `externalOptionsAllowList` to: `"start_date,end_date,page_size,plan_id,start_time,end_time"`

2. **Via Unity Catalog API**:
   ```python
   # Example using Databricks SDK
   from databricks.sdk import WorkspaceClient
   
   w = WorkspaceClient()
   
   connection = w.connections.create(
       name="paypal_connection",
       connection_type="LAKEFLOW",
       options={
           "client_id": "YOUR_CLIENT_ID",
           "client_secret": "YOUR_CLIENT_SECRET",
           "environment": "sandbox",
           "externalOptionsAllowList": "start_date,end_date,page_size,plan_id,start_time,end_time,subscription_ids,include_transactions,product_id,plan_ids,event_type,transaction_id,tracking_number,disputed_transaction_id"
       }
   )
   ```

## Supported Objects

The PayPal connector supports **14 comprehensive data tables** covering all PayPal data APIs:

### Core Payment Tables
1. **`transactions`** - 💰 Transaction history and search (most comprehensive)
2. **`payment_captures`** - 💳 Payment capture records
3. **`refunds`** - 🔄 Refund transactions
4. **`payment_authorizations`** - 🔐 Payment authorizations

### Subscription & Product Tables
5. **`subscriptions`** - 📅 Subscription billing data
6. **`products`** - 📦 Catalog products
7. **`plans`** - 📋 Billing plans

### Dispute & Risk Tables
8. **`disputes`** - ⚠️  Customer disputes and chargebacks

### Operational Tables
9. **`payouts`** - 💸 Payout batches
10. **`webhooks_events`** - 🔔 Webhook event history
11. **`tracking`** - 📍 Shipment tracking information
12. **`payment_experiences`** - 🎨 Web payment profiles

### Limited Availability Tables
13. **`invoices`** - 🧾 Invoicing data (⚠️ Requires special permissions)
14. **`orders`** - 🛒 Order data (⚠️ No bulk listing available)

**All 14 Tables Available**: All tables are implemented and tested. Some require specific API permissions or table_options (see below).

### `transactions` Table

The **`transactions`** table provides transaction history data from your PayPal account.

**Primary Key**: `transaction_id`

**Incremental Ingestion**: 
- **Strategy**: Snapshot-based ingestion with date range filtering
- **Cursor Field**: `transaction_initiation_date`
- **Ingestion Type**: `snapshot`

**Required Table Options**:

| Option | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `start_date` | string | Yes | Start of date range in ISO 8601 format (UTC). Must be within the last 3 years. | `"2024-01-01T00:00:00Z"` |
| `end_date` | string | Yes | End of date range in ISO 8601 format (UTC). Maximum 31-day range from start_date. | `"2024-01-31T23:59:59Z"` |
| `page_size` | integer | No | Number of transactions per page (default: 100, max: 500) | `100` |

**Schema Structure**:

The `transactions` table has a **flattened schema** with all fields at the top level for easier querying:

**Transaction Fields** (from PayPal `transaction_info`):
- `transaction_id` (string, not null): Unique transaction identifier
- `paypal_account_id` (string): PayPal account ID associated with transaction
- `transaction_event_code` (string): Event code (e.g., T0000, T0001)
- `transaction_initiation_date` (string): Transaction creation timestamp (ISO 8601)
- `transaction_updated_date` (string): Last update timestamp (ISO 8601)
- `transaction_amount` (struct): Amount with `currency_code` and `value` fields
- `fee_amount` (struct): Fee amount with `currency_code` and `value` fields
- `transaction_status` (string): Status code (S=Success, P=Pending, D=Denied, V=Voided, F=Failed)
- `transaction_subject` (string): Transaction description
- `ending_balance` (struct): Account balance after transaction
- `available_balance` (struct): Available balance after transaction
- `invoice_id` (string): Associated invoice ID (if applicable)
- `custom_field` (string): Custom data field
- `protection_eligibility` (string): Buyer/seller protection status

**Payer Fields** (from PayPal `payer_info`):
- `payer_account_id` (string): Payer's PayPal account ID
- `payer_email_address` (string): Payer's email address
- `payer_address_status` (string): Address verification status
- `payer_status` (string): Payer account status
- `payer_name` (struct): Payer name with `given_name` and `surname` fields
- `payer_country_code` (string): Payer's country code

**Shipping Fields** (from PayPal `shipping_info`):
- `shipping_name` (string): Recipient name
- `shipping_address` (struct): Address with `line1`, `city`, `country_code`, `postal_code` fields

**Cart Fields** (from PayPal `cart_info`):
- `item_details` (array<struct>): Array of purchased items, each with:
  - `item_code` (string): Item SKU/code
  - `item_name` (string): Item name
  - `item_description` (string): Item description
  - `item_quantity` (string): Quantity purchased
  - `item_unit_price` (struct): Unit price with `currency_code` and `value`
  - `item_amount` (struct): Total item amount with `currency_code` and `value`

**Delete Synchronization**: Not supported. PayPal transactions are immutable once created.

**Important Limitations**:
- **31-Day Maximum**: PayPal enforces a strict 31-day maximum date range per query. For larger ranges, split into multiple 31-day windows.
- **3-Year Historical Limit**: Transaction data is only available for the last 3 years from the current date.
- **Immutable Transactions**: Transactions don't change after creation. Refunds and reversals appear as new transactions.

### ❌ `invoices` Table - Not Included

**Status**: Not available in this connector

**Reason**: The PayPal Invoicing API v2 requires special permissions that are **not available in the PayPal Sandbox environment**. Testing and development with Sandbox credentials consistently returns 403 "NOT_AUTHORIZED" errors, making it impossible to validate the implementation.

**Impact**: The `invoices` table has been removed from the connector and will not appear in `list_tables()`.

**Alternative**: If you need invoice data and have production credentials with Invoicing API access enabled, please contact the maintainers to discuss adding this table back as a production-only feature.

### `subscriptions` Table

The **`subscriptions`** table provides subscription data from your PayPal account using the Subscriptions API v1.

**Primary Key**: `id`

**Incremental Ingestion**:
- **Strategy**: Change Data Capture (CDC) with update tracking
- **Cursor Field**: `update_time`
- **Ingestion Type**: `cdc`

**Optional Table Options**:

| Option | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `plan_id` | string | Recommended | Filter by subscription plan ID | `"P-12345..."` |
| `start_time` | string | No | Filter by start time (ISO 8601) | `"2024-01-01T00:00:00Z"` |
| `end_time` | string | No | Filter by end time (ISO 8601) | `"2024-12-31T23:59:59Z"` |

**Schema Highlights**:

- **`id`**: Unique subscription identifier
- **`plan_id`**: Associated subscription plan ID
- **`status`**: Subscription status (ACTIVE, SUSPENDED, CANCELLED, EXPIRED)
- **`subscriber`**: Subscriber information (email, payer_id, name, shipping address)
- **`billing_info`**: Billing details including cycle executions, last/next payment times, failed payment count
- **`start_time`**: Subscription start timestamp
- **`create_time`**: Subscription creation timestamp
- **`update_time`**: Last update timestamp
- **`status_update_time`**: Status change timestamp

**Key Fields**:
- `id` (string, not null): Unique subscription identifier
- `plan_id` (string): Plan this subscription is based on
- `status` (string): Current subscription status
- `update_time` (string): Last modification time (ISO 8601)
- `billing_info.next_billing_time` (string): Next scheduled billing date

**Important Notes**:
- **Limited Bulk Listing**: The PayPal Subscriptions API has limited bulk listing capability. You may need to provide a `plan_id` to filter results.
- **Plan-Based Access**: If you encounter errors, ensure you're filtering by a valid `plan_id`.

### `orders` Table

The **`orders`** table is **not functional** due to PayPal API limitations.

**Status**: ❌ Not Available

**Reason**: PayPal Orders API v2 does not provide a bulk "list orders" endpoint. Orders are created and retrieved individually by ID only.

**Alternative**: Use the **`transactions`** table instead, which provides comprehensive transaction data including order information, payments, captures, and refunds.

**Schema Defined**: While the schema is defined in the connector for potential future use, attempting to read from this table will return an informative error directing you to use the `transactions` table.

### `products` Table

The **`products`** table provides catalog product data from your PayPal account.

**Primary Key**: `id`

**Incremental Ingestion**:
- **Strategy**: Change Data Capture (CDC) with update tracking
- **Cursor Field**: `update_time`
- **Ingestion Type**: `cdc`

**Optional Table Options**:

| Option | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `page_size` | integer | No | Number of products per page (default: 20, max: 20) | `20` |

**Schema Highlights**:

- **`id`** (string, not null): Unique product identifier
- **`name`** (string): Product name
- **`description`** (string): Product description
- **`type`** (string): Product type (e.g., PHYSICAL, DIGITAL, SERVICE)
- **`category`** (string): Product category
- **`image_url`** (string): Product image URL
- **`home_url`** (string): Product home page URL
- **`create_time`** (string): Product creation timestamp (ISO 8601)
- **`update_time`** (string): Last update timestamp (ISO 8601)
- **`links`** (array): HATEOAS links

**Use Case**: Product catalog management, inventory tracking, subscription product setup.

### `plans` Table

The **`plans`** table provides billing plan data for subscriptions.

**Primary Key**: `id`

**Incremental Ingestion**:
- **Strategy**: Change Data Capture (CDC) with update tracking
- **Cursor Field**: `update_time`
- **Ingestion Type**: `cdc`

**Optional Table Options**:

| Option | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `product_id` | string | No | Filter by product ID | `"PROD-12345..."` |
| `plan_ids` | string | No | Comma-separated plan IDs to filter | `"P-123,P-456"` |
| `page_size` | integer | No | Number of plans per page (default: 20, max: 20) | `20` |

**Schema Highlights**:

- **`id`** (string, not null): Unique plan identifier
- **`product_id`** (string): Associated product ID
- **`name`** (string): Plan name
- **`status`** (string): Plan status (ACTIVE, INACTIVE, CREATED)
- **`description`** (string): Plan description
- **`billing_cycles`** (array<struct>): Billing cycle definitions with pricing and frequency
- **`payment_preferences`** (struct): Payment setup including auto-billing and failure actions
- **`taxes`** (struct): Tax configuration
- **`create_time`** (string): Plan creation timestamp (ISO 8601)
- **`update_time`** (string): Last update timestamp (ISO 8601)

**Use Case**: Subscription plan management, pricing analytics, recurring revenue tracking.

### `payment_captures` Table

The **`payment_captures`** table provides payment capture transaction records.

**Primary Key**: `id`

**Incremental Ingestion**:
- **Strategy**: Change Data Capture (CDC) with update tracking
- **Cursor Field**: `update_time`
- **Ingestion Type**: `cdc`

**Required Table Options**:

| Option | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `start_date` | string | Yes | Start of date range in ISO 8601 format (UTC). Must be within the last 3 years. | `"2024-01-01T00:00:00Z"` |
| `end_date` | string | Yes | End of date range in ISO 8601 format (UTC). Maximum 31-day range from start_date. | `"2024-01-31T23:59:59Z"` |
| `page_size` | integer | No | Number of captures per page (default: 100, max: 500) | `100` |

**Schema Highlights**:

- **`id`** (string, not null): Unique capture identifier (transaction ID)
- **`status`** (string): Capture status (COMPLETED, PENDING, DECLINED, etc.)
- **`status_details`** (struct): Additional status information
- **`amount`** (struct): Capture amount with currency_code and value
- **`invoice_id`** (string): Associated invoice ID
- **`custom_id`** (string): Custom identifier
- **`seller_protection`** (struct): Seller protection status and dispute categories
- **`final_capture`** (boolean): Whether this is the final capture for the transaction
- **`seller_receivable_breakdown`** (struct): Detailed breakdown including gross amount, fees, and net amount
- **`disbursement_mode`** (string): Disbursement mode (INSTANT, DELAYED)
- **`create_time`** (string): Capture creation timestamp (ISO 8601)
- **`update_time`** (string): Last update timestamp (ISO 8601)

**Important Notes**:
- Uses Transaction Search API to retrieve capture transactions
- Subject to same 31-day maximum date range and 3-year historical limit as `transactions` table
- Filters specifically for capture transaction types (T0106)

**Use Case**: Payment reconciliation, revenue tracking, fee analysis, financial reporting.

### `disputes` Table

The **`disputes`** table provides customer dispute and chargeback data.

**Primary Key**: `dispute_id`

**Incremental Ingestion**:
- **Strategy**: Change Data Capture (CDC) with update tracking
- **Cursor Field**: `update_time`
- **Ingestion Type**: `cdc`

**Optional Table Options**:

| Option | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `start_time` | string | No | Filter disputes created after this time (ISO 8601) | `"2024-01-01T00:00:00Z"` |
| `disputed_transaction_id` | string | No | Filter by specific transaction ID | `"1AB23456CD789012E"` |
| `page_size` | integer | No | Number of disputes per page (default: 10, max: 50) | `10` |

**Schema Highlights**:

- **`dispute_id`** (string, not null): Unique dispute identifier
- **`create_time`** (string): Dispute creation timestamp
- **`update_time`** (string): Last update timestamp
- **`reason`** (string): Dispute reason code
- **`status`** (string): Current dispute status
- **`dispute_state`** (string): Detailed state of the dispute
- **`dispute_amount`** (struct): Amount in dispute
- **`seller_transaction_id`** (string): Associated transaction ID
- **`buyer_user_id`** (string): Buyer's PayPal user ID
- **`seller_user_id`** (string): Seller's PayPal user ID
- **`offer`** (struct): Settlement offer details
- **`messages`** (array<struct>): Communication thread

**Use Case**: Fraud analysis, customer service, dispute resolution tracking, chargeback management.

### `payouts` Table

The **`payouts`** table provides payout batch data for mass payment operations.

**Primary Key**: `batch_id`

**Incremental Ingestion**:
- **Strategy**: Change Data Capture (CDC) with creation time tracking
- **Cursor Field**: `time_created`
- **Ingestion Type**: `cdc`

**Optional Table Options**:

| Option | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `start_date` | string | No | Filter by creation date (YYYY-MM-DD) | `"2024-01-01"` |
| `end_date` | string | No | Filter by creation date (YYYY-MM-DD) | `"2024-01-31"` |
| `page_size` | integer | No | Number of payouts per page (default: 10, max: 100) | `10` |

**Schema Highlights**:

- **`batch_id`** (string, not null): Unique payout batch identifier
- **`payout_batch_id`** (string): PayPal payout batch ID
- **`batch_status`** (string): Batch processing status
- **`time_created`** (string): Batch creation timestamp
- **`time_completed`** (string): Batch completion timestamp
- **`sender_batch_header`** (struct): Batch metadata including email subject/message
- **`amount`** (struct): Total payout amount
- **`fees`** (struct): Total fees charged

**Use Case**: Mass payment tracking, affiliate payouts, vendor payments, commission distribution.

**Note**: Payouts may not be available in Sandbox without creating payout batches first.

### `refunds` Table

The **`refunds`** table provides refund transaction data.

**Primary Key**: `id`

**Incremental Ingestion**:
- **Strategy**: Change Data Capture (CDC) with update tracking
- **Cursor Field**: `update_time`
- **Ingestion Type**: `cdc`

**Required Table Options**:

| Option | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `start_date` | string | Yes | Start of date range in ISO 8601 format (UTC) | `"2024-01-01T00:00:00Z"` |
| `end_date` | string | Yes | End of date range in ISO 8601 format (UTC). Max 31-day range | `"2024-01-31T23:59:59Z"` |

**Schema Highlights**:

- **`id`** (string, not null): Unique refund transaction identifier
- **`status`** (string): Refund status (COMPLETED, PENDING, CANCELLED)
- **`status_details`** (struct): Additional status information
- **`amount`** (struct): Refund amount with currency and value
- **`invoice_id`** (string): Associated invoice ID
- **`custom_id`** (string): Custom identifier
- **`note_to_payer`** (string): Refund note sent to customer
- **`seller_payable_breakdown`** (struct): Refund breakdown including fees
- **`create_time`** (string): Refund creation timestamp
- **`update_time`** (string): Last update timestamp

**Use Case**: Refund analysis, return tracking, revenue adjustments, customer satisfaction metrics.

**Note**: Uses Transaction Search API filtered for refund transaction types (T1106).

### `payment_authorizations` Table

The **`payment_authorizations`** table provides payment authorization data.

**Primary Key**: `id`

**Incremental Ingestion**:
- **Strategy**: Change Data Capture (CDC) with update tracking
- **Cursor Field**: `update_time`
- **Ingestion Type**: `cdc`

**Required Table Options**:

| Option | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `start_date` | string | Yes | Start of date range in ISO 8601 format (UTC) | `"2024-01-01T00:00:00Z"` |
| `end_date` | string | Yes | End of date range in ISO 8601 format (UTC). Max 31-day range | `"2024-01-31T23:59:59Z"` |

**Schema Highlights**:

- **`id`** (string, not null): Unique authorization identifier
- **`status`** (string): Authorization status (CREATED, CAPTURED, VOIDED, EXPIRED)
- **`amount`** (struct): Authorized amount
- **`invoice_id`** (string): Associated invoice ID
- **`custom_id`** (string): Custom identifier
- **`seller_protection`** (struct): Protection status
- **`expiration_time`** (string): Authorization expiration timestamp
- **`create_time`** (string): Authorization creation timestamp
- **`update_time`** (string): Last update timestamp

**Use Case**: Authorization tracking, payment holds, fraud prevention, order fulfillment workflow.

**Note**: Uses Transaction Search API filtered for authorization transaction types (T0001).

### `webhooks_events` Table

The **`webhooks_events`** table provides webhook event history.

**Primary Key**: `id`

**Incremental Ingestion**:
- **Strategy**: Change Data Capture (CDC) with creation time tracking
- **Cursor Field**: `create_time`
- **Ingestion Type**: `cdc`

**Optional Table Options**:

| Option | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `start_time` | string | No | Filter events after this time (ISO 8601) | `"2024-01-01T00:00:00Z"` |
| `end_time` | string | No | Filter events before this time (ISO 8601) | `"2024-01-31T23:59:59Z"` |
| `event_type` | string | No | Filter by specific event type | `"PAYMENT.CAPTURE.COMPLETED"` |
| `page_size` | integer | No | Number of events per page (default: 10, max: 300) | `10` |

**Schema Highlights**:

- **`id`** (string, not null): Unique event identifier
- **`create_time`** (string): Event creation timestamp
- **`resource_type`** (string): Type of resource that triggered the event
- **`event_version`** (string): Event schema version
- **`event_type`** (string): Event type (e.g., PAYMENT.CAPTURE.COMPLETED)
- **`summary`** (string): Event summary description
- **`resource`** (string): JSON string of the event resource data
- **`links`** (array<struct>): Related API resource links

**Use Case**: Event monitoring, integration debugging, audit logging, real-time notifications.

### `payment_experiences` Table

The **`payment_experiences`** table provides web payment profile/experience configurations.

**Primary Key**: `id`

**Incremental Ingestion**:
- **Strategy**: Snapshot (no timestamp-based incremental)
- **Cursor Field**: `id`
- **Ingestion Type**: `snapshot`

**No Table Options Required**: This API returns all profiles in a single call.

**Schema Highlights**:

- **`id`** (string, not null): Unique payment experience ID
- **`name`** (string): Experience profile name
- **`temporary`** (boolean): Whether this is a temporary profile
- **`flow_config`** (struct): Payment flow configuration (landing page, user action)
- **`input_fields`** (struct): Input field settings (notes, shipping)
- **`presentation`** (struct): Presentation settings (brand name, logo, locale)

**Use Case**: Payment UX customization, branding management, checkout flow configuration.

### `tracking` Table

The **`tracking`** table provides shipment tracking information.

**Primary Key**: `transaction_id`, `tracking_number`

**Incremental Ingestion**:
- **Strategy**: Change Data Capture (CDC) with update tracking
- **Cursor Field**: `last_updated_time`
- **Ingestion Type**: `cdc`

**Required Table Options** (at least one):

| Option | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `transaction_id` | string | Conditional | Filter by transaction ID | `"1AB23456CD789012E"` |
| `tracking_number` | string | Conditional | Filter by tracking number | `"1Z999AA10123456784"` |
| `start_date` | string | Conditional | Filter by shipment date (YYYY-MM-DD) | `"2024-01-01"` |
| `end_date` | string | Conditional | Filter by shipment date (YYYY-MM-DD) | `"2024-01-31"` |
| `page_size` | integer | No | Number of trackers per page (default: 10, max: 20) | `10` |

**Schema Highlights**:

- **`transaction_id`** (string, not null): Associated transaction ID
- **`tracking_number`** (string): Shipment tracking number
- **`status`** (string): Tracking status
- **`carrier`** (string): Shipping carrier code
- **`carrier_name_other`** (string): Custom carrier name
- **`shipment_date`** (string): Date shipment was sent
- **`last_updated_time`** (string): Last tracking update timestamp
- **`quantity`** (long): Number of items shipped
- **`notify_buyer`** (boolean): Whether buyer was notified

**Use Case**: Order fulfillment tracking, shipping analytics, delivery monitoring.

**Important**: At least one filter (transaction_id, tracking_number, or date range) must be provided.

## Data Type Mapping

| PayPal API Type | Example Fields | Databricks Type | Notes |
|-----------------|----------------|-----------------|-------|
| string | `transaction_id`, `email_address`, `transaction_status` | STRING | Text identifiers, codes, and general strings |
| string (amount) | `value` in amount objects | STRING | Decimal values as strings to preserve precision. Can be cast to DECIMAL in downstream processing. |
| string (ISO 8601) | `transaction_initiation_date`, `transaction_updated_date` | STRING | ISO 8601 UTC timestamps (e.g., "2024-01-15T10:30:00Z"). Can be cast to TIMESTAMP in processing. |
| object | `transaction_amount`, `payer_info`, `shipping_info` | STRUCT | Nested records with named fields |
| array | `item_details` | ARRAY<STRUCT> | Arrays of nested objects |

**Type Conversion Notes**:
- **Currency Amounts**: Stored as STRING to avoid floating-point precision loss. Convert to DECIMAL(19,4) or similar in your queries.
- **Timestamps**: Stored as STRING in ISO 8601 format with 'Z' timezone. Convert using `to_timestamp()` function.
- **Status Codes**: Single-letter codes (S, P, D, V, F) - consider creating a lookup table for human-readable descriptions.

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the PayPal source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in your main pipeline file (e.g., `ingest.py`):

```python
pipeline_spec = {
    "connection_name": "paypal_connection",
    "object": [
        {
            "table": {
                "source_table": "transactions",
                "start_date": "2024-01-01T00:00:00Z",
                "end_date": "2024-01-31T23:59:59Z",
                "page_size": 100
            }
        },
        {
            "table": {
                "source_table": "subscriptions",
                "plan_id": "P-12345ABCDE"  # Optional but recommended
            }
        }
    ]
}
```

2. **Configure Date Ranges**:
   - Set `start_date` and `end_date` for each ingestion window
   - Ensure the range doesn't exceed 31 days
   - Use ISO 8601 format with UTC timezone (ending in 'Z')
   - For ongoing syncs, implement a rolling window strategy

3. **Example: Rolling Window for Incremental Sync**:

```python
from datetime import datetime, timedelta

# Sync the last 30 days of transactions
end_date = datetime.utcnow()
start_date = end_date - timedelta(days=30)

pipeline_spec = {
    "connection_name": "paypal_connection",
    "object": [
        {
            "table": {
                "source_table": "transactions",
                "start_date": start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_date": end_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "page_size": 500  # Maximum page size for better performance
            }
        }
    ]
}
```

4. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start Small**: Begin by syncing a short date range (e.g., 1-7 days) to validate your pipeline
- **Respect API Limits**: PayPal enforces a 31-day maximum date range - plan your queries accordingly
- **Use Maximum Page Size**: Set `page_size` to 500 (the maximum) to reduce API calls
- **Schedule Appropriately**: 
  - For recent transactions: Run daily with a 1-day window
  - For historical backfills: Run with 31-day windows sequentially
  - Avoid overlapping date ranges to prevent duplicate data
- **Monitor Rate Limits**: PayPal allows **50 requests per 10 seconds** - if you hit rate limits, the connector will raise an error with retry guidance
- **Timezone Awareness**: Always use UTC timestamps (ending in 'Z') to avoid timezone conversion issues

#### Troubleshooting

**Common Issues:**

1. **Authentication Errors (401 Unauthorized)**
   - **Cause**: Invalid or expired credentials
   - **Solution**: Verify your `client_id` and `client_secret` are correct. Ensure your PayPal app is active in the Developer Dashboard.

2. **Rate Limiting (429 Too Many Requests)**
   - **Cause**: Exceeded 50 requests per 10 seconds
   - **Solution**: The connector will report the retry-after time. Reduce pipeline frequency or implement exponential backoff in your scheduling.

3. **Date Range Error**
   - **Cause**: Date range exceeds 31 days or dates are outside the 3-year historical window
   - **Solution**: Split queries into 31-day windows. Verify dates are within the last 3 years.

4. **Empty Results**
   - **Cause**: No transactions in the specified date range, or dates in the future
   - **Solution**: Verify your account had activity during the specified period. Check that `start_date` is before `end_date` and both are in the past.

5. **Invalid Date Format**
   - **Cause**: Date strings not in ISO 8601 format
   - **Solution**: Use format `"YYYY-MM-DDTHH:MM:SSZ"` (e.g., `"2024-01-15T00:00:00Z"`)

6. **Missing Nested Fields**
   - **Cause**: Some transactions may not have all nested objects (e.g., no shipping info for certain transaction types)
   - **Solution**: This is expected behavior. The connector sets missing nested objects to `null`. Handle nulls in your downstream queries.

7. **Subscriptions API Error (404 or requires plan_id)**
   - **Cause**: PayPal Subscriptions API has limited bulk listing capability
   - **Solution**: Provide a `plan_id` in table_options to filter subscriptions by plan. Example: `"plan_id": "P-12345ABCDE"`

8. **Orders Table Not Available**
   - **Cause**: PayPal Orders API v2 doesn't support bulk order listing
   - **Solution**: Use the `transactions` table instead, which includes order and payment information

## Example Queries

Once your transactions data is ingested into Databricks, you can query it using SQL. The flattened schema makes querying straightforward:

### Basic Transaction Query

```sql
SELECT 
    transaction_id,
    transaction_initiation_date,
    transaction_status,
    transaction_amount.value as amount,
    transaction_amount.currency_code as currency,
    payer_email_address,
    payer_name.given_name as payer_first_name,
    payer_name.surname as payer_last_name
FROM main.paypal_data.transactions
WHERE transaction_status = 'S'  -- Success
ORDER BY transaction_initiation_date DESC
LIMIT 100;
```

### Calculate Total Revenue by Date

```sql
SELECT 
    DATE(transaction_initiation_date) as transaction_date,
    transaction_amount.currency_code as currency,
    SUM(CAST(transaction_amount.value AS DECIMAL(19,4))) as total_revenue,
    SUM(CAST(fee_amount.value AS DECIMAL(19,4))) as total_fees,
    COUNT(*) as transaction_count
FROM main.paypal_data.transactions
WHERE transaction_status = 'S'
GROUP BY DATE(transaction_initiation_date), transaction_amount.currency_code
ORDER BY transaction_date DESC;
```

### Analyze Transactions by Country

```sql
SELECT 
    payer_country_code,
    COUNT(*) as transaction_count,
    SUM(CAST(transaction_amount.value AS DECIMAL(19,4))) as total_amount,
    transaction_amount.currency_code as currency
FROM main.paypal_data.transactions
WHERE transaction_status = 'S'
    AND payer_country_code IS NOT NULL
GROUP BY payer_country_code, transaction_amount.currency_code
ORDER BY total_amount DESC;
```

### Transactions with Shipping Information

```sql
SELECT 
    transaction_id,
    transaction_initiation_date,
    payer_email_address,
    shipping_name,
    shipping_address.city,
    shipping_address.country_code,
    shipping_address.postal_code,
    transaction_amount.value as amount
FROM main.paypal_data.transactions
WHERE shipping_name IS NOT NULL
    AND transaction_status = 'S'
ORDER BY transaction_initiation_date DESC;
```

### Analyze Cart Items (Exploded)

```sql
SELECT 
    transaction_id,
    transaction_initiation_date,
    item.item_name,
    item.item_quantity,
    item.item_unit_price.value as unit_price,
    item.item_amount.value as item_total,
    item.item_unit_price.currency_code as currency
FROM main.paypal_data.transactions
LATERAL VIEW EXPLODE(item_details) AS item
WHERE item_details IS NOT NULL
    AND transaction_status = 'S'
ORDER BY transaction_initiation_date DESC;
```

### Top Products by Revenue

```sql
SELECT 
    item.item_name,
    item.item_code,
    COUNT(*) as times_sold,
    SUM(CAST(item.item_quantity AS INT)) as total_quantity,
    SUM(CAST(item.item_amount.value AS DECIMAL(19,4))) as total_revenue,
    item.item_amount.currency_code as currency
FROM main.paypal_data.transactions
LATERAL VIEW EXPLODE(item_details) AS item
WHERE item_details IS NOT NULL
    AND transaction_status = 'S'
GROUP BY item.item_name, item.item_code, item.item_amount.currency_code
ORDER BY total_revenue DESC
LIMIT 20;
```

### Transaction Status Distribution

```sql
SELECT 
    CASE transaction_status
        WHEN 'S' THEN 'Success'
        WHEN 'P' THEN 'Pending'
        WHEN 'D' THEN 'Denied'
        WHEN 'V' THEN 'Voided'
        WHEN 'F' THEN 'Failed'
        ELSE 'Unknown'
    END as status_label,
    transaction_status,
    COUNT(*) as count,
    SUM(CAST(transaction_amount.value AS DECIMAL(19,4))) as total_amount
FROM main.paypal_data.transactions
GROUP BY transaction_status
ORDER BY count DESC;
```

## References

- [PayPal REST API Documentation](https://developer.paypal.com/api/rest/)
- [PayPal Transaction Search API v1](https://developer.paypal.com/docs/api/transaction-search/v1/)
- [PayPal Subscriptions API v1](https://developer.paypal.com/docs/api/subscriptions/v1/)
- [PayPal Developer Dashboard](https://developer.paypal.com/dashboard/)
- [PayPal REST API Current Resources](https://developer.paypal.com/api/rest/current-resources/)
- [OAuth 2.0 Client Credentials Flow](https://developer.paypal.com/api/rest/authentication/)
- [PayPal API Rate Limits](https://developer.paypal.com/api/rest/rate-limiting/)

