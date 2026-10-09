from tap_chargebee.streams.base import BaseChargebeeStream


class BillingMetricEntitlementsStream(BaseChargebeeStream):
    TABLE = 'billing_metric_entitlements'
    ENTITY = 'billing_metric_entitlement'
    REPLICATION_METHOD = 'INCREMENTAL'
    # API ignores updated_at[after/before] and rejects sort_by updated_at; occurred_at is supported.
    REPLICATION_KEY = 'occurred_at'
    # Live API has no id; entitlement_lineage_id+version is unique across fetched pages.
    KEY_PROPERTIES = ['entitlement_lineage_id', 'version']
    BOOKMARK_PROPERTIES = ['occurred_at']
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = ['occurred_at']
    INCLUSION = 'available'
    API_METHOD = 'GET'
    SORT_BY = 'occurred_at'

    def get_url(self):
        return 'https://{}/api/v2/billing_metric_entitlements'.format(self.config.get('full_site'))

    def get_stream_data(self, data):
        # List items are flat records, not wrapped under the entity key.
        return [self.transform_record(item) for item in data]
