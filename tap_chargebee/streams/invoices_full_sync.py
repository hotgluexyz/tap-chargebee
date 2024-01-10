from tap_chargebee.streams.base import BaseChargebeeStream


class InvoicesFullSyncStream(BaseChargebeeStream):
    TABLE = 'invoices_full_sync'
    ENTITY = 'invoices_full_sync'
    REPLICATION_METHOD = 'FULL_TABLE'
    REPLICATION_KEY = None
    KEY_PROPERTIES = ['id']
    BOOKMARK_PROPERTIES = ['date']
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = []
    INCLUSION = 'available'
    API_METHOD = 'GET'

    def get_url(self):
        return 'https://{}/api/v2/invoices'.format(self.config.get('full_site'))
    
    def get_stream_data(self, data):
        entity = "invoice"
        return [self.transform_record(item.get(entity)) for item in data]
