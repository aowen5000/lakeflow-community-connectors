import requests
import base64
from datetime import datetime, timedelta
from typing import Iterator, Any

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    BooleanType,
    ArrayType,
)


class LakeflowConnect:
    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the PayPal connector with connection-level options.

        Expected options:
            - client_id: OAuth 2.0 client ID from PayPal Developer Dashboard
            - client_secret: OAuth 2.0 client secret from PayPal Developer Dashboard
            - environment (optional): 'sandbox' or 'production'. Defaults to 'sandbox'.
        """
        self.client_id = options.get("client_id")
        self.client_secret = options.get("client_secret")
        
        if not self.client_id or not self.client_secret:
            raise ValueError(
                "PayPal connector requires 'client_id' and 'client_secret' in options"
            )

        # Determine base URL based on environment
        environment = options.get("environment", "sandbox").lower()
        if environment == "production":
            self.base_url = "https://api-m.paypal.com"
        else:
            self.base_url = "https://api-m.sandbox.paypal.com"

        # Configure session for API requests
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

        # Token caching
        self._access_token = None
        self._token_expires_at = None

    def _get_access_token(self) -> str:
        """
        Obtain or refresh OAuth 2.0 access token using client credentials flow.
        
        Tokens are cached and refreshed 5 minutes before expiration (9-hour lifetime).
        """
        # Check if cached token is still valid (with 5-minute buffer)
        if self._access_token and self._token_expires_at:
            buffer = timedelta(minutes=5)
            if datetime.now() + buffer < self._token_expires_at:
                return self._access_token

        # Request new token
        token_url = f"{self.base_url}/v1/oauth2/token"
        
        # Create Basic auth header with Base64-encoded client_id:client_secret
        credentials = f"{self.client_id}:{self.client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        headers = {
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        
        data = {"grant_type": "client_credentials"}
        
        response = requests.post(token_url, headers=headers, data=data, timeout=30)
        
        if response.status_code != 200:
            raise RuntimeError(
                f"PayPal OAuth token request failed: {response.status_code} {response.text}"
            )
        
        token_data = response.json()
        self._access_token = token_data.get("access_token")
        expires_in = token_data.get("expires_in", 32400)  # Default 9 hours
        
        self._token_expires_at = datetime.now() + timedelta(seconds=expires_in)
        
        return self._access_token

    def _make_request(
        self, method: str, endpoint: str, params: dict = None
    ) -> requests.Response:
        """
        Make an authenticated API request to PayPal.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path (e.g., '/v1/reporting/transactions')
            params: Query parameters
            
        Returns:
            Response object
        """
        access_token = self._get_access_token()
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        
        url = f"{self.base_url}{endpoint}"
        
        response = self._session.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            timeout=30
        )
        
        # Handle common error cases
        if response.status_code == 401:
            # Token may have expired, clear cache and retry once
            self._access_token = None
            self._token_expires_at = None
            access_token = self._get_access_token()
            headers["Authorization"] = f"Bearer {access_token}"
            response = self._session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                timeout=30
            )
        
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After", "60")
            raise RuntimeError(
                f"PayPal API rate limit exceeded. Retry after {retry_after} seconds."
            )
        
        if response.status_code not in [200, 201]:
            raise RuntimeError(
                f"PayPal API error: {response.status_code} {response.text}"
            )
        
        return response

    def list_tables(self) -> list[str]:
        """
        List names of all tables supported by this connector.
        
        Currently supports only the 'transactions' table.
        """
        return ["transactions"]

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a table.
        
        Args:
            table_name: The name of the table to fetch the schema for.
            table_options: Additional options (not required for PayPal connector).
            
        Returns:
            A StructType object representing the schema of the table.
        """
        if table_name not in self.list_tables():
            raise ValueError(f"Unsupported table: {table_name!r}")

        if table_name == "transactions":
            # Define nested struct types
            amount_struct = StructType([
                StructField("currency_code", StringType(), True),
                StructField("value", StringType(), True),
            ])
            
            payer_name_struct = StructType([
                StructField("given_name", StringType(), True),
                StructField("surname", StringType(), True),
            ])
            
            payer_info_struct = StructType([
                StructField("account_id", StringType(), True),
                StructField("email_address", StringType(), True),
                StructField("address_status", StringType(), True),
                StructField("payer_status", StringType(), True),
                StructField("payer_name", payer_name_struct, True),
                StructField("country_code", StringType(), True),
            ])
            
            address_struct = StructType([
                StructField("line1", StringType(), True),
                StructField("city", StringType(), True),
                StructField("country_code", StringType(), True),
                StructField("postal_code", StringType(), True),
            ])
            
            shipping_info_struct = StructType([
                StructField("name", StringType(), True),
                StructField("address", address_struct, True),
            ])
            
            transaction_info_struct = StructType([
                StructField("transaction_id", StringType(), False),
                StructField("paypal_account_id", StringType(), True),
                StructField("transaction_event_code", StringType(), True),
                StructField("transaction_initiation_date", StringType(), True),
                StructField("transaction_updated_date", StringType(), True),
                StructField("transaction_amount", amount_struct, True),
                StructField("fee_amount", amount_struct, True),
                StructField("transaction_status", StringType(), True),
                StructField("transaction_subject", StringType(), True),
                StructField("ending_balance", amount_struct, True),
                StructField("available_balance", amount_struct, True),
                StructField("invoice_id", StringType(), True),
                StructField("custom_field", StringType(), True),
                StructField("protection_eligibility", StringType(), True),
            ])
            
            # Item details for cart info
            item_details_struct = StructType([
                StructField("item_code", StringType(), True),
                StructField("item_name", StringType(), True),
                StructField("item_description", StringType(), True),
                StructField("item_quantity", StringType(), True),
                StructField("item_unit_price", amount_struct, True),
                StructField("item_amount", amount_struct, True),
            ])
            
            cart_info_struct = StructType([
                StructField("item_details", ArrayType(item_details_struct, True), True),
            ])
            
            # Main transactions table schema
            transactions_schema = StructType([
                StructField("transaction_info", transaction_info_struct, True),
                StructField("payer_info", payer_info_struct, True),
                StructField("shipping_info", shipping_info_struct, True),
                StructField("cart_info", cart_info_struct, True),
            ])
            
            return transactions_schema

        raise ValueError(f"Unsupported table: {table_name!r}")

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        """
        Fetch metadata for the given table.
        
        Args:
            table_name: The name of the table to fetch metadata for.
            table_options: Additional options (not required for PayPal connector).
            
        Returns:
            A dictionary containing primary_keys, cursor_field, and ingestion_type.
        """
        if table_name not in self.list_tables():
            raise ValueError(f"Unsupported table: {table_name!r}")

        if table_name == "transactions":
            return {
                "primary_keys": ["transaction_info.transaction_id"],
                "cursor_field": "transaction_info.transaction_initiation_date",
                "ingestion_type": "snapshot",
            }

        raise ValueError(f"Unsupported table: {table_name!r}")

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read records from a table and return raw JSON-like dictionaries.
        
        Args:
            table_name: The name of the table to read.
            start_offset: The offset to start reading from.
            table_options: Additional options including start_date and end_date.
            
        Returns:
            An iterator of records in JSON format and an offset.
        """
        if table_name not in self.list_tables():
            raise ValueError(f"Unsupported table: {table_name!r}")

        if table_name == "transactions":
            return self._read_transactions(start_offset, table_options)

        raise ValueError(f"Unsupported table: {table_name!r}")

    def _read_transactions(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Internal implementation for reading the 'transactions' table.
        
        Required table_options:
            - start_date: ISO 8601 date string (e.g., '2024-01-01T00:00:00Z')
            - end_date: ISO 8601 date string (e.g., '2024-01-31T23:59:59Z')
            
        Optional table_options:
            - page_size: Number of transactions per page (default: 100, max: 500)
            
        The PayPal API enforces a maximum 31-day date range per request.
        """
        start_date = table_options.get("start_date")
        end_date = table_options.get("end_date")
        
        if not start_date or not end_date:
            raise ValueError(
                "table_options for 'transactions' must include 'start_date' and 'end_date' "
                "in ISO 8601 format (e.g., '2024-01-01T00:00:00Z')"
            )
        
        # Validate date range (PayPal enforces 31-day maximum)
        try:
            start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            date_range = (end_dt - start_dt).days
            
            if date_range > 31:
                raise ValueError(
                    f"Date range exceeds PayPal's 31-day maximum. "
                    f"Requested range: {date_range} days. "
                    f"Please split the date range into smaller windows."
                )
        except ValueError as e:
            if "31-day" in str(e):
                raise
            raise ValueError(
                f"Invalid date format. Expected ISO 8601 format "
                f"(e.g., '2024-01-01T00:00:00Z'): {e}"
            )
        
        # Get page size from options (default 100, max 500)
        try:
            page_size = int(table_options.get("page_size", 100))
        except (TypeError, ValueError):
            page_size = 100
        page_size = max(1, min(page_size, 500))
        
        # Get starting page from offset (default 1)
        if start_offset and isinstance(start_offset, dict):
            page = start_offset.get("page", 1)
        else:
            page = 1
        
        # Build query parameters
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "page_size": page_size,
            "page": page,
        }
        
        # Make API request
        response = self._make_request("GET", "/v1/reporting/transactions", params)
        
        if response.status_code != 200:
            raise RuntimeError(
                f"PayPal API error for transactions: {response.status_code} {response.text}"
            )
        
        data = response.json()
        
        # Extract transaction details array
        transaction_details = data.get("transaction_details", [])
        if not isinstance(transaction_details, list):
            raise ValueError(
                f"Unexpected response format for transaction_details: "
                f"{type(transaction_details).__name__}"
            )
        
        # Process records - keep nested structure, set missing nested objects to None
        records: list[dict[str, Any]] = []
        for txn in transaction_details:
            # Preserve nested structure as returned by API
            record: dict[str, Any] = {
                "transaction_info": txn.get("transaction_info"),
                "payer_info": txn.get("payer_info"),
                "shipping_info": txn.get("shipping_info"),
                "cart_info": txn.get("cart_info"),
            }
            records.append(record)
        
        # Determine next offset based on pagination metadata
        total_pages = data.get("total_pages", 1)
        current_page = data.get("page", page)
        
        # If there are more pages, increment page number
        if current_page < total_pages:
            next_offset = {"page": current_page + 1}
        else:
            # No more pages - return same offset to indicate end of data
            next_offset = start_offset if start_offset else {"page": page}
        
        return iter(records), next_offset

