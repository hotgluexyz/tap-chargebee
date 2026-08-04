from tap_chargebee.streams.base import BaseChargebeeStream


class BillingMetricBreakdownsStream(BaseChargebeeStream):
    TABLE = 'billing_metric_breakdowns'
    ENTITY = 'billing_metric_breakdown'
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEY = 'updated_at'
    # line_item_id alone is not unique; lineage_id+version+line_item_id+date_from+date_to is.
    KEY_PROPERTIES = ['lineage_id', 'version', 'line_item_id', 'date_from', 'date_to']
    BOOKMARK_PROPERTIES = ['updated_at']
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = ['updated_at']
    INCLUSION = 'available'
    API_METHOD = 'GET'
    SORT_BY = 'updated_at'

    def get_url(self):
        return 'https://{}/api/v2/billing_metric_breakdowns'.format(self.config.get('full_site'))

    def get_stream_data(self, data):
        # List items are flat records, not wrapped under the entity key.
        return [self.transform_record(item) for item in data]
