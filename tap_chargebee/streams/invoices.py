from tap_chargebee.streams.base import BaseChargebeeStream
import singer

LOGGER = singer.get_logger()

class InvoicesStream(BaseChargebeeStream):
    TABLE = 'invoices'
    ENTITY = 'invoice'
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEY = 'updated_at'
    KEY_PROPERTIES = ['id']
    BOOKMARK_PROPERTIES = ['updated_at']
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = ['updated_at']
    INCLUSION = 'available'
    API_METHOD = 'GET'

    def get_url(self):
        return 'https://{}/api/v2/invoices'.format(self.config.get('full_site'))

    def get_stream_data(self, records):
        invoice_dict = {}  

        for record in records:
            invoice = record.get(self.ENTITY, {})
            if invoice:
                invoice_id = invoice.get('id', None)
                
                if invoice_id in invoice_dict:
                    existing_invoice = invoice_dict[invoice_id]
                    existing_updated_at = existing_invoice.get('updated_at')
                    new_updated_at = invoice.get('updated_at')
                    
                    if new_updated_at and existing_updated_at and new_updated_at > existing_updated_at:
                        LOGGER.info(f"Updating invoice {invoice_id} with newer version from {new_updated_at}")
                        invoice_dict[invoice_id] = invoice
                    else:
                        LOGGER.debug(f"Skipping older version of invoice {invoice_id}")
                else:
                    invoice_dict[invoice_id] = invoice

        return list(invoice_dict.values())
