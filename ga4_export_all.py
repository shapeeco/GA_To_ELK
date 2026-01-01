"""
GA4 to Elasticsearch Exporter - Export all GA4 properties to Elasticsearch data streams

This script automatically discovers all accessible Google Analytics 4 properties and exports
their metrics to Elasticsearch data streams for visualization in Kibana.

Functions:
---------
validate_config()
    Validates required environment variables for GA4 and Elasticsearch connections.
    Exits the program if configuration is invalid.

create_datastream_if_not_exists(es, datastream_name)
    Creates an Elasticsearch data stream if it doesn't already exist.
    Data streams are optimized for time-series data like GA4 metrics.

    Args:
        es: Elasticsearch client instance
        datastream_name: Name of the data stream to create (e.g., 'ga4metrics-property-123456')

export_property_data(ga_client, es, property_id, property_name)
    Exports GA4 metrics for a single property to its dedicated Elasticsearch data stream.

    Fetches the following data:
    - Dimensions: pageTitle, pagePath, sessionSource, sessionMedium, country, city, date
    - Metrics: screenPageViews, scrolledUsers, activeUsers, userEngagementDuration, eventCount,
               sessions, totalUsers, engagedSessions

    Args:
        ga_client: Google Analytics Data API client
        es: Elasticsearch client instance
        property_id: GA4 property ID (numeric)
        property_name: Display name of the property

    Returns:
        int: Number of documents successfully exported

main()
    Main entry point that orchestrates the export process:
    1. Authenticates to Google Analytics and Elasticsearch
    2. Discovers all accessible GA4 properties via account summaries
    3. Exports data from each property to a separate data stream
    4. Prints summary statistics

Environment Variables Required:
------------------------------
- GA_CREDENTIALS_PATH: Path to Google service account JSON credentials
- GA_ACCOUNT_ID or GA_PROPERTY_ID: GA4 account/property identifier
- GA_DAYS_TO_PULL: Number of days of historical data to fetch (default: 30)
- GA_REPORT_LIMIT: Maximum number of rows per report (default: 100000)
- ELASTICSEARCH_HOST: Elasticsearch server URL
- ELASTICSEARCH_API_KEY: Elasticsearch API key for authentication
- LOG_LEVEL: Logging level (default: INFO)

Usage:
------
    python ga4_export_all.py

Output:
-------
Creates Elasticsearch data streams named: ga4metrics-{property-name}-{property-id}
Each data stream contains time-series documents with GA4 metrics and dimensions.
"""

import logging
import warnings
import hashlib
from datetime import datetime, timedelta, timezone

from elasticsearch import Elasticsearch
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
from google.analytics.admin import AnalyticsAdminServiceClient

from ga_auth import GAConfig

# Suppress SSL warnings when verify_certs=False
warnings.filterwarnings('ignore', message='Unverified HTTPS request')
warnings.filterwarnings('ignore', message='Connecting to .* using TLS with verify_certs=False is insecure')

# Initialize configuration
config = GAConfig()

# Logging Setup
logging.basicConfig(
    level=config.log_level,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def validate_config():
    """Validate required configuration for GA4 export."""
    try:
        config.validate_ga_config()
        config.validate_elasticsearch_config()
    except (ValueError, FileNotFoundError) as e:
        logger.error(f"Configuration error: {e}")
        exit(1)

def create_datastream_if_not_exists(es, datastream_name):
    """Create Elasticsearch data stream if it doesn't exist."""
    try:
        # Check if data stream exists
        if not es.indices.exists(index=datastream_name):
            # Create data stream
            es.indices.create_data_stream(name=datastream_name)
            logger.info(f"Created new Elasticsearch data stream: {datastream_name}")
        else:
            logger.info(f"Using existing Elasticsearch data stream: {datastream_name}")
    except Exception as e:
        # If data stream creation fails, log warning but continue
        logger.warning(f"Could not create data stream {datastream_name}: {e}. Will attempt to insert documents anyway.")
        pass

def export_property_data(ga_client, es, property_id, property_name):
    """Export data for a single property to Elasticsearch."""
    try:
        # Create data stream name from property name (sanitize for Elasticsearch)
        # Data streams work better for time-series data like GA4 metrics
        sanitized_name = property_name.lower().replace(' ', '-').replace('_', '-').replace('.', '-')
        datastream_name = f"ga4metrics-{sanitized_name}-{property_id}"
        logger.info(f"Exporting data for property: {property_name} ({property_id}) to data stream: {datastream_name}")

        # Create data stream if it doesn't exist
        create_datastream_if_not_exists(es, datastream_name)

        # Prepare GA4 report request with comprehensive dimensions and metrics
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[
                Dimension(name="pageTitle"),
                Dimension(name="pagePath"),
                Dimension(name="sessionSource"),
                Dimension(name="sessionMedium"),
                Dimension(name="country"),
                Dimension(name="city"),
                Dimension(name="date"),
            ],
            metrics=[
                Metric(name="screenPageViews"),
                Metric(name="scrolledUsers"),
                Metric(name="activeUsers"),
                Metric(name="userEngagementDuration"),
                Metric(name="eventCount"),
                Metric(name="sessions"),
                Metric(name="totalUsers"),
                Metric(name="engagedSessions"),
            ],
            date_ranges=[DateRange(start_date=f"{config.days_to_pull}daysAgo", end_date="today")],
            limit=config.report_limit
        )

        # Fetch report data
        response = ga_client.run_report(request)
        logger.info(f"Pulled {len(response.rows)} rows for property {property_id}")

        if len(response.rows) == 0:
            logger.warning(f"No data found for property {property_id}")
            return 0

        # Send data to Elasticsearch
        doc_count = 0
        for row in response.rows:
            # Build document with dimensions
            doc = {
                "property_id": property_id,
                "property_name": property_name,
                "pageTitle": row.dimension_values[0].value,
                "pagePath": row.dimension_values[1].value,
                "sessionSource": row.dimension_values[2].value,
                "sessionMedium": row.dimension_values[3].value,
                "country": row.dimension_values[4].value,
                "city": row.dimension_values[5].value,
                "date": row.dimension_values[6].value,

                # Add metrics
                "screenPageViews": int(row.metric_values[0].value or 0),
                "scrolledUsers": int(row.metric_values[1].value or 0),
                "activeUsers": int(row.metric_values[2].value or 0),
                "userEngagementDuration": float(row.metric_values[3].value or 0),
                "eventCount": int(row.metric_values[4].value or 0),
                "sessions": int(row.metric_values[5].value or 0),
                "totalUsers": int(row.metric_values[6].value or 0),
                "engagedSessions": int(row.metric_values[7].value or 0),

                # Add timestamp
                "@timestamp": datetime.now(timezone.utc).isoformat()
            }

            # Generate unique document ID based on property, date, and key dimensions
            # This prevents duplicate data if the script is run multiple times
            doc_id_string = f"{property_id}-{doc['date']}-{doc['pagePath']}-{doc['sessionSource']}-{doc['sessionMedium']}-{doc['country']}-{doc['city']}"
            doc_id = hashlib.md5(doc_id_string.encode()).hexdigest()

            es.index(index=datastream_name, document=doc, id=doc_id)
            doc_count += 1

        logger.info(f"Successfully sent {doc_count} documents to Elasticsearch data stream: {datastream_name}")
        return doc_count

    except Exception as e:
        logger.error(f"Failed to export data for property {property_id}: {e}")
        logger.exception("Full error details:")
        return 0

def main():
    """Main function to export GA4 data from all properties to Elasticsearch."""
    validate_config()

    # Authenticate Google Analytics clients
    try:
        credentials = config.get_credentials()
        admin_client = AnalyticsAdminServiceClient(credentials=credentials)
        ga_client = BetaAnalyticsDataClient(credentials=credentials)
        logger.info("Authenticated to Google Analytics.")
    except Exception as e:
        logger.error(f"Failed to authenticate to Google Analytics: {e}")
        exit(1)

    # Connect to Elasticsearch
    try:
        es = Elasticsearch(
            config.elasticsearch_host,
            api_key=config.elasticsearch_api_key,
            verify_certs=False  # Use only if self hosting ssl certificates
        )
        if not es.ping():
            raise Exception("Elasticsearch server not responding")
        logger.info("Connected to Elasticsearch.")
    except Exception as e:
        logger.error(f"Failed to connect to Elasticsearch: {e}")
        exit(1)

    # Get all properties using account summaries
    try:
        logger.info("Fetching all accessible properties via account summaries")
        account_summaries = admin_client.list_account_summaries()

        total_properties = 0
        total_documents = 0

        # Iterate through account summaries and their property summaries
        for summary in account_summaries:
            account_name = summary.account
            account_display_name = summary.display_name

            logger.info(f"Processing account: {account_display_name} ({account_name})")

            for prop_summary in summary.property_summaries:
                total_properties += 1
                property_resource_name = prop_summary.property
                property_id = property_resource_name.split('/')[-1]
                property_display_name = prop_summary.display_name

                logger.info(f"Processing property #{total_properties}: {property_display_name} (ID: {property_id})")

                # Export data for this property
                doc_count = export_property_data(ga_client, es, property_id, property_display_name)
                total_documents += doc_count

        # Summary
        print("\n" + "="*100)
        print("Export Summary")
        print("="*100)
        print(f"Total Properties Processed: {total_properties}")
        print(f"Total Documents Exported: {total_documents}")
        print(f"Date Range: Last {config.days_to_pull} days")
        print("="*100 + "\n")

        logger.info(f"Export complete. Processed {total_properties} properties, exported {total_documents} documents.")

    except Exception as e:
        logger.error(f"Failed to export data: {e}")
        logger.exception("Full error details:")
        exit(1)

if __name__ == "__main__":
    main()
