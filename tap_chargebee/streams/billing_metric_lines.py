from tap_chargebee.streams.base import BaseChargebeeStream


class BillingMetricLinesStream(BaseChargebeeStream):
    TABLE = 'billing_metric_lines'
    ENTITY = 'billing_metric_line'
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEY = 'occurred_at'
    KEY_PROPERTIES = ['id']
    BOOKMARK_PROPERTIES = ['occurred_at']
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = ['occurred_at']
    INCLUSION = 'available'
    API_METHOD = 'GET'
    SORT_BY = 'occurred_at'

    def get_url(self):
        return 'https://{}/api/v2/billing_metric_lines'.format(self.config.get('full_site'))

    def get_stream_data(self, data):
        # Sample list items are flat records, not wrapped under the entity key.
        return [self.transform_record(item) for item in data]
