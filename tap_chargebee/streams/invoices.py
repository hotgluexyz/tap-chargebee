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
    SORT_BY = 'updated_at'
    # Chargebee paginates these together under line_items_next_offset (Enterprise-scale Invoicing).
    _LINE_ITEM_KEYS = (
        'line_items',
        'line_item_discounts',
        'line_item_taxes',
        'line_item_tiers',
    )

    def get_url(self):
        return 'https://{}/api/v2/invoices'.format(self.config.get('full_site'))
     
    def get_stream_data(self, data):
        entity = self.ENTITY
        records = []
        
        for item in data:
            record = item.get(entity)
            
            # Check if line items are empty but there's a line_items_next_offset
            if 'line_items_next_offset' in record and not record.get('line_items', []):
                LOGGER.info(f"Invoice {record['id']} has empty line items but has line_items_next_offset: {record['line_items_next_offset']}. Retrieving line items through retrieve API.")
                
                # Get all line items recursively
                record = self._get_all_line_items(record)
            
            records.append(self.transform_record(record))
            
        return records
        
    def _get_all_line_items(self, record):
        """Fetch all paginated line-item resources for an invoice using line_items_offset."""
        collected = {key: list(record.get(key) or []) for key in self._LINE_ITEM_KEYS}
        current_offset = record.get('line_items_next_offset')

        while current_offset:
            try:
                #to test with a smaller limit
                params = {"line_items_offset": current_offset, "line_items_limit": 300}
                retrieve_response = self.client.make_request(
                    url=f"{self.get_url()}/{record['id']}",
                    method=self.API_METHOD,
                    params=params
                )

                if retrieve_response and 'invoice' in retrieve_response:
                    invoice_data = retrieve_response['invoice']

                    for key in self._LINE_ITEM_KEYS:
                        collected[key].extend(invoice_data.get(key) or [])

                    current_offset = invoice_data.get('line_items_next_offset')
                    new_line_items = invoice_data.get('line_items') or []

                    LOGGER.info(f"Retrieved {len(new_line_items)} additional line items for invoice {record['id']}. " +
                               (f"Continuing with offset {current_offset}" if current_offset else "All line items retrieved."))
                else:
                    LOGGER.warning(f"Invalid response while retrieving line items for invoice {record['id']}")
                    break

            except Exception as e:
                LOGGER.error(f"Error retrieving line items for invoice {record['id']}: {str(e)}")
                break

        record.update(collected)
        LOGGER.info(
            f"Successfully retrieved all {len(collected['line_items'])} line items, "
            f"{len(collected['line_item_discounts'])} line_item_discounts, "
            f"{len(collected['line_item_taxes'])} line_item_taxes, "
            f"{len(collected['line_item_tiers'])} line_item_tiers "
            f"for invoice {record['id']}"
        )

        return record
