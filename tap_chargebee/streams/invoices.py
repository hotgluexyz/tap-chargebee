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
        """Recursively fetch all line items for an invoice using line_items_offset."""
        all_line_items = record.get('line_items', [])
        current_offset = record.get('line_items_next_offset')
        
        while current_offset:
            try:
                params = {"line_items_offset": current_offset, "line_items_limit": 300}
                retrieve_response = self.client.make_request(
                    url=f"{self.get_url()}/{record['id']}",
                    method=self.API_METHOD,
                    params=params
                )
                
                if retrieve_response and 'invoice' in retrieve_response:
                    invoice_data = retrieve_response['invoice']
                    
                    # Add newly retrieved line items to our collection
                    new_line_items = invoice_data.get('line_items', [])
                    all_line_items.extend(new_line_items)
                    
                    # Update offset for next iteration or exit loop if done
                    current_offset = invoice_data.get('line_items_next_offset')
                    
                    LOGGER.info(f"Retrieved {len(new_line_items)} additional line items for invoice {record['id']}. " +
                               (f"Continuing with offset {current_offset}" if current_offset else "All line items retrieved."))
                else:
                    LOGGER.warning(f"Invalid response while retrieving line items for invoice {record['id']}")
                    break
                    
            except Exception as e:
                LOGGER.error(f"Error retrieving line items for invoice {record['id']}: {str(e)}")
                break
        
        # Update the record with all line items
        record['line_items'] = all_line_items
        LOGGER.info(f"Successfully retrieved all {len(all_line_items)} line items for invoice {record['id']}")
        
        return record
