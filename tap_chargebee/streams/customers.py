from tap_chargebee.streams.base import BaseChargebeeStream


class CustomersStream(BaseChargebeeStream):
    TABLE = 'customers'
    ENTITY = 'customer'
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEY = 'updated_at'
    KEY_PROPERTIES = ['id']
    BOOKMARK_PROPERTIES = ['updated_at']
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = ['updated_at']
    INCLUSION = 'available'
    API_METHOD = 'GET'

    def get_url(self):
        return 'https://{}/api/v2/customers'.format(self.config.get('full_site'))

    def get_params(self, params):
        params = super().get_params(params)
        if self.config.get('include_deprecated') is True:
            params["include_deprecated"] = "true"
        return params