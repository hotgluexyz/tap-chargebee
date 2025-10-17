from tap_chargebee.streams.base import BaseChargebeeStream


class ItemBillingMetricsStream(BaseChargebeeStream):
    TABLE = 'item_billing_metrics'
    ENTITY = 'item_billing_metric'
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEY = 'modified_at'
    KEY_PROPERTIES = ['id']
    BOOKMARK_PROPERTIES = ['modified_at']
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = ['modified_at']
    INCLUSION = 'available'
    API_METHOD = 'GET'

    def get_url(self):
        return 'https://{}/api/v2/item_billing_metrics'.format(self.config.get('full_site'))
