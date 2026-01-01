"""
GA4 to Elasticsearch Exporter - Single Property Export

Exports GA4 metrics from a single property to an Elasticsearch index.
Fetches basic dimensions (date, pagePath) and metrics (screenPageViews, sessions).
"""

import logging
import hashlib
from datetime import datetime, timedelta, timezone

from elasticsearch import Elasticsearch
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric

from ga_auth import GAConfig

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

def main():
    validate_config()

    # Authenticate Google Analytics client
    try:
        credentials = config.get_credentials()
        ga_client = BetaAnalyticsDataClient(credentials=credentials)
        logger.info("Authenticated to Google Analytics.")
    except Exception as e:
        logger.error(f"Failed to authenticate to Google Analytics: {e}")
        exit(1)

    # Prepare GA4 report request
    end_date = datetime.today()
    start_date = end_date - timedelta(days=config.days_to_pull)

    request = RunReportRequest(
        property=f"properties/{config.property_id}",
        dimensions=[Dimension(name="date"), Dimension(name="pagePath")],
        metrics=[Metric(name="screenPageViews"), Metric(name="sessions")],
        date_ranges=[DateRange(start_date=start_date.strftime("%Y-%m-%d"), end_date=end_date.strftime("%Y-%m-%d"))],
        limit=config.report_limit
    )

    # Fetch report data
    try:
        response = ga_client.run_report(request)
        logger.info(f"Pulled {len(response.rows)} GA rows from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}.")
    except Exception as e:
        logger.error(f"Failed to fetch GA report: {e}")
        exit(1)

    # Connect Elasticsearch
    try:
        es = Elasticsearch(
            config.elasticsearch_host,
            api_key=config.elasticsearch_api_key,
            verify_certs=False  # Use only if self hosting ssl certificates as it disables SSL cert validation. If using normal SSL, then change to True
        )
        if not es.ping():
            raise Exception("Elasticsearch server not responding")
        logger.info("Connected to Elasticsearch.")
    except Exception as e:
        logger.error(f"Failed to connect to Elasticsearch: {e}")
        exit(1)

    # Send GA data to Elasticsearch
    try:
        for row in response.rows:
            doc = {dim.name: val.value for dim, val in zip(response.dimension_headers, row.dimension_values)}
            doc.update({metric.name: float(val.value or 0) for metric, val in zip(response.metric_headers, row.metric_values)})
            doc['@timestamp'] = datetime.now(timezone.utc).isoformat()

            # Generate unique document ID to prevent duplicates
            doc_id_string = f"{config.property_id}-{doc.get('date', '')}-{doc.get('pagePath', '')}"
            doc_id = hashlib.md5(doc_id_string.encode()).hexdigest()

            es.index(index="ga4-data", document=doc, id=doc_id)

        logger.info("Successfully sent GA data to Elasticsearch index: ga4-data")
    except Exception as e:
        logger.error(f"Failed to send data to Elasticsearch: {e}")
        exit(1)

if __name__ == "__main__":
    main()
