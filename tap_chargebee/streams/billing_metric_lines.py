from tap_chargebee.streams.base import BaseChargebeeStream


class BillingMetricLinesStream(BaseChargebeeStream):
    TABLE = 'billing_metric_lines'
    ENTITY = 'billing_metric_line'
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEY = 'updated_at'
    # Live API has no id; lineage_id+version is unique across fetched pages.
    KEY_PROPERTIES = ['lineage_id', 'version']
    BOOKMARK_PROPERTIES = ['updated_at']
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = ['updated_at']
    INCLUSION = 'available'
    API_METHOD = 'GET'
    SORT_BY = 'updated_at'

    def get_url(self):
        return 'https://{}/api/v2/billing_metric_lines'.format(self.config.get('full_site'))

    def get_stream_data(self, data):
        # List items are flat records, not wrapped under the entity key.
        return [self.transform_record(item) for item in data]
