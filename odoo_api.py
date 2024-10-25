import requests
import logging
import json
from utils import safe_get, format_timestamp

class OdooAPI:
    def __init__(self, config):
        self.base_url = config['base_url']
        self.api_key = config['api_key']
        self.login = config['login']
        self.password = config['password']
        self.db_name = config['db_name']

    def _make_request(self, model, fields):
        """Private method to make the API request to Odoo."""
        url = (f"{self.base_url}/send_request?model={model}"
               f"&login={self.login}&password={self.password}&api-key={self.api_key}&db={self.db_name}"
               f"&Content-Type=application/json")
        
        logging.info(f"Constructed URL: {url}")

        headers = {
            "login": self.login,
            "password": self.password,
            "api-key": self.api_key,
            "db": self.db_name,
            "Content-Type": "application/json"
        }

        payload = {"fields": fields}
        logging.info(f"Payload: {json.dumps(payload, indent=2)}")

        try:
            response = requests.get(url, headers=headers, data=json.dumps(payload), timeout=30)
            response.raise_for_status()
            return response.json().get('records', [])

        except requests.exceptions.RequestException as e:
            logging.error(f"Error making request: {str(e)}")
            return []

    def fetch_sales_orders(self):
        """Fetch Sales Orders from Odoo API."""
        fields = [
            "name", "date_order", "expected_date", "partner_id", "user_id", "team_id", 
            "amount_untaxed", "amount_tax", "amount_total", "write_date", "create_date", 
            "warehouse_id", "amount_to_invoice", "client_order_ref", "invoice_status", 
            "delivery_status", "state"
        ]
        orders = self._make_request('sale.order', fields)
        
        if not orders:
            logging.info("No sales orders found.")
            return []
        
        processed_orders = []
        for order in orders:
            processed_order = {
                'id': str(safe_get(order, 'id')),
                'name': str(safe_get(order, 'name')),
                'date_order': str(format_timestamp(order['date_order'])),
                'expected_date': str(format_timestamp(order['expected_date'])),
                'partner_id': str(order['partner_id'][1] if order['partner_id'] else None),
                'user_id': str(order['user_id'][1] if order['user_id'] else None),
                'team_id': str(order['team_id'][1] if order['team_id'] else None),
                'amount_untaxed': str(safe_get(order, 'amount_untaxed')),
                'amount_tax': str(safe_get(order, 'amount_tax')),
                'amount_total': str(safe_get(order, 'amount_total')),
                'write_date': str(format_timestamp(order['write_date'])),
                'create_date': str(format_timestamp(order['create_date'])),
                'warehouse_id': str(order['warehouse_id'][1] if order['warehouse_id'] else None),
                'amount_to_invoice': str(safe_get(order, 'amount_to_invoice')),
                'client_order_ref': str(safe_get(order, 'client_order_ref')),
                'invoice_status': str(safe_get(order, 'invoice_status')),
                'delivery_status': str(safe_get(order, 'delivery_status')),
                'state': str(safe_get(order, 'state'))
            }
            processed_orders.append(processed_order)

        return processed_orders

        
        """Fetch Sales Orders Lines from Odoo API."""
    def fetch_sales_order_line(self):
        fields = [
            "product_id","product_template_id", "name", "stock_item_note", "warehouses_id", 
            "free_qty_today", "route_id", "product_uom_qty", "qty_delivered", "qty_invoiced", 
            "product_uom", "customer_lead", "product_packaging_qty", "product_packaging_id", 
            "price_unit", "tax_id", "price_subtotal", "price_total"
        ]
        order_line = self._make_request('sale.order.line', fields)
        
        if not order_line:
            logging.info("No sales order line found.")
            return []
        
        processed_records = []
        for record in order_line:
            processed_record = {
                'id': str(safe_get(record, 'id')),
                
                # Safely handling list fields: product_id and product_template_id
                'product_id_id': str(record['product_id'][0]) if isinstance(record['product_id'], list) and len(record['product_id']) > 0 else str(record['product_id']) if not isinstance(record['product_id'], list) else None,
                'product_id_name': str(record['product_id'][1]) if isinstance(record['product_id'], list) and len(record['product_id']) > 1 else None,

                'product_template_id_id': str(record['product_template_id'][0]) if isinstance(record['product_template_id'], list) and len(record['product_template_id']) > 0 else str(record['product_template_id']) if not isinstance(record['product_template_id'], list) else None,
                'product_template_id_name': str(record['product_template_id'][1]) if isinstance(record['product_template_id'], list) and len(record['product_template_id']) > 1 else None,

                # Regular string fields
                'name': str(safe_get(record, 'name')),
                'stock_item_note': str(safe_get(record, 'stock_item_note')),
                
                # Safely handling list fields: warehouses_id
                'warehouses_id_id': str(record['warehouses_id'][0]) if isinstance(record['warehouses_id'], list) and len(record['warehouses_id']) > 0 else str(record['warehouses_id']) if not isinstance(record['warehouses_id'], list) else None,
                'warehouses_id_name': str(record['warehouses_id'][1]) if isinstance(record['warehouses_id'], list) and len(record['warehouses_id']) > 1 else None,

                'free_qty_today': str(safe_get(record, 'free_qty_today')),
                'route_id': str(record['route_id'][0]) if isinstance(record['route_id'], list) and len(record['route_id']) > 0 else str(record['route_id']) if not isinstance(record['route_id'], list) else None,
                'product_uom_qty': str(safe_get(record, 'product_uom_qty')),
                'qty_delivered': str(safe_get(record, 'qty_delivered')),
                'qty_invoiced': str(safe_get(record, 'qty_invoiced')),

                # Safely handling list fields: product_uom
                'product_uom_id': str(record['product_uom'][0]) if isinstance(record['product_uom'], list) and len(record['product_uom']) > 0 else str(record['product_uom']) if not isinstance(record['product_uom'], list) else None,
                'product_uom_name': str(record['product_uom'][1]) if isinstance(record['product_uom'], list) and len(record['product_uom']) > 1 else None,

                'customer_lead': str(safe_get(record, 'customer_lead')),
                'product_packaging_qty': str(safe_get(record, 'product_packaging_qty')),

                # Safely handling list fields: product_packaging_id
                'product_packaging_id': str(record['product_packaging_id'][0]) if isinstance(record['product_packaging_id'], list) and len(record['product_packaging_id']) > 0 else str(record['product_packaging_id']) if not isinstance(record['product_packaging_id'], list) else None,

                'price_unit': str(safe_get(record, 'price_unit')),

                # Safely handling list fields: tax_id
                'tax_id': str(record['tax_id'][0]) if isinstance(record['tax_id'], list) and len(record['tax_id']) > 0 else str(record['tax_id']) if not isinstance(record['tax_id'], list) else None,

                'price_subtotal': str(safe_get(record, 'price_subtotal')),
                'price_total': str(safe_get(record, 'price_total'))
            }

            processed_records.append(processed_record)

        return processed_records

    """Fetch Purchase Orders from Odoo API."""
    def fetch_purchase_orders(self):
        fields = [
            "name", "partner_id", "partner_ref", "user_id", "date_order", "origin", 
            "amount_untaxed", "amount_total", "state", "invoice_status", 
            "write_date", "create_date"
        ]
        
        # Fetch data from Odoo (assuming the _make_request method is available)
        purchase_orders = self._make_request('purchase.order', fields)

        if not purchase_orders:
            logging.info("No purchase orders found.")
            return []

        processed_records = []
        for record in purchase_orders:
            processed_record = {
                'id': str(safe_get(record, 'id')),

                # Safely handling list fields: partner_id and user_id
                'partner_id_id': str(record['partner_id'][0]) if isinstance(record['partner_id'], list) and len(record['partner_id']) > 0 else str(record['partner_id']) if not isinstance(record['partner_id'], list) else None,
                'partner_id_name': str(record['partner_id'][1]) if isinstance(record['partner_id'], list) and len(record['partner_id']) > 1 else None,
                
                'user_id_id': str(record['user_id'][0]) if isinstance(record['user_id'], list) and len(record['user_id']) > 0 else str(record['user_id']) if not isinstance(record['user_id'], list) else None,
                'user_id_name': str(record['user_id'][1]) if isinstance(record['user_id'], list) and len(record['user_id']) > 1 else None,

                # Regular string fields
                'name': str(safe_get(record, 'name')),
                'partner_ref': str(safe_get(record, 'partner_ref')),
                'origin': str(safe_get(record, 'origin')),
                
                # Handling numeric fields
                'amount_untaxed': str(safe_get(record, 'amount_untaxed')),
                'amount_total': str(safe_get(record, 'amount_total')),
                
                # Handling state and status fields
                'state': str(safe_get(record, 'state')),
                'invoice_status': str(safe_get(record, 'invoice_status')),

                # Handling timestamp fields
                'date_order': str(format_timestamp(safe_get(record, 'date_order'))),
                'write_date': str(format_timestamp(safe_get(record, 'write_date'))),
                'create_date': str(format_timestamp(safe_get(record, 'create_date')))
            }

            processed_records.append(processed_record)

        return processed_records

    """Fetch Purchase Order Lines from Odoo API."""
    def fetch_purchase_order_line(self):
        
        fields = [
            "name", "product_id", "date_planned", "product_qty", "qty_received", 
            "qty_invoiced", "product_uom", "product_packaging_qty", "product_packaging_id", 
            "price_unit", "taxes_id", "discount", "price_subtotal", "price_total"
        ]
        
        # Fetch data from Odoo (assuming the _make_request method is available)
        purchase_order_line = self._make_request('purchase.order.line', fields)

        if not purchase_order_line:
            logging.info("No purchase order lines found.")
            return []

        processed_records = []
        for record in purchase_order_line:
            processed_record = {
                'id': str(safe_get(record, 'id')),

                # Safely handling list fields: product_id and product_uom
                'product_id_id': str(record['product_id'][0]) if isinstance(record['product_id'], list) and len(record['product_id']) > 0 else str(record['product_id']) if not isinstance(record['product_id'], list) else None,
                'product_id_name': str(record['product_id'][1]) if isinstance(record['product_id'], list) and len(record['product_id']) > 1 else None,

                'product_uom_id': str(record['product_uom'][0]) if isinstance(record['product_uom'], list) and len(record['product_uom']) > 0 else str(record['product_uom']) if not isinstance(record['product_uom'], list) else None,
                'product_uom_name': str(record['product_uom'][1]) if isinstance(record['product_uom'], list) and len(record['product_uom']) > 1 else None,

                # Regular fields
                'name': str(safe_get(record, 'name')),
                'product_qty': str(safe_get(record, 'product_qty')),
                'qty_received': str(safe_get(record, 'qty_received')),
                'qty_invoiced': str(safe_get(record, 'qty_invoiced')),
                'product_packaging_qty': str(safe_get(record, 'product_packaging_qty')),

                # Handling list field: product_packaging_id
                'product_packaging_id': str(record['product_packaging_id'][0]) if isinstance(record['product_packaging_id'], list) and len(record['product_packaging_id']) > 0 else str(record['product_packaging_id']) if not isinstance(record['product_packaging_id'], list) else None,

                'price_unit': str(safe_get(record, 'price_unit')),
                
                # Handling taxes_id, assuming it's a list
                'taxes_id': str(record['taxes_id'][0]) if isinstance(record['taxes_id'], list) and len(record['taxes_id']) > 0 else None,

                'discount': str(safe_get(record, 'discount')),
                'price_subtotal': str(safe_get(record, 'price_subtotal')),
                'price_total': str(safe_get(record, 'price_total')),

                # Timestamp field: date_planned
                'date_planned': format_timestamp(safe_get(record, 'date_planned'))
            }

            processed_records.append(processed_record)

        return processed_records


    def fetch_accounts(self):
        """Fetch Accounts (account.move) from Odoo API."""
        fields = [
            "name", "date", "invoice_date", "delivery_date", "invoice_date_due", 
            "partner_id", "invoice_partner_display_name", "invoice_user_id", 
            "invoice_payment_term_id", "team_id", "invoice_origin", "journal_id", 
            "amount_untaxed_signed", "amount_tax_signed", "amount_total_signed", 
            "amount_total_in_currency_signed", "amount_residual_signed", "currency_id", 
            "payment_state", "state", "write_date", "create_date", "activity_ids", 
            "ref", "to_check", "warehouse_id", "payment_reference"
        ]
        
        # Fetch data from Odoo (assuming the _make_request method is available)
        accounts = self._make_request('account.move', fields)

        if not accounts:
            logging.info("No account moves found.")
            return []

        processed_records = []
        for record in accounts:
            processed_record = {
                'id': str(safe_get(record, 'id')),

                # Safely handling list fields: partner_id, team_id, journal_id, currency_id
                'partner_id_id': str(record['partner_id'][0]) if isinstance(record['partner_id'], list) and len(record['partner_id']) > 0 else str(record['partner_id']) if not isinstance(record['partner_id'], list) else None,
                'partner_id_name': str(record['partner_id'][1]) if isinstance(record['partner_id'], list) and len(record['partner_id']) > 1 else None,

                'team_id_id': str(record['team_id'][0]) if isinstance(record['team_id'], list) and len(record['team_id']) > 0 else str(record['team_id']) if not isinstance(record['team_id'], list) else None,
                'team_id_name': str(record['team_id'][1]) if isinstance(record['team_id'], list) and len(record['team_id']) > 1 else None,

                'journal_id_id': str(record['journal_id'][0]) if isinstance(record['journal_id'], list) and len(record['journal_id']) > 0 else str(record['journal_id']) if not isinstance(record['journal_id'], list) else None,
                'journal_id_name': str(record['journal_id'][1]) if isinstance(record['journal_id'], list) and len(record['journal_id']) > 1 else None,

                'currency_id_id': str(record['currency_id'][0]) if isinstance(record['currency_id'], list) and len(record['currency_id']) > 0 else str(record['currency_id']) if not isinstance(record['currency_id'], list) else None,
                'currency_id_name': str(record['currency_id'][1]) if isinstance(record['currency_id'], list) and len(record['currency_id']) > 1 else None,

                # Regular fields
                'name': str(safe_get(record, 'name')),
                'invoice_partner_display_name': str(safe_get(record, 'invoice_partner_display_name')),
                'invoice_user_id': str(safe_get(record['invoice_user_id'], 0)) if isinstance(record['invoice_user_id'], list) else str(safe_get(record, 'invoice_user_id')),
                'invoice_payment_term_id': str(safe_get(record['invoice_payment_term_id'], 0)) if isinstance(record['invoice_payment_term_id'], list) else str(safe_get(record, 'invoice_payment_term_id')),
                'invoice_origin': str(safe_get(record, 'invoice_origin')),
                'amount_untaxed_signed': str(safe_get(record, 'amount_untaxed_signed')),
                'amount_tax_signed': str(safe_get(record, 'amount_tax_signed')),
                'amount_total_signed': str(safe_get(record, 'amount_total_signed')),
                'amount_total_in_currency_signed': str(safe_get(record, 'amount_total_in_currency_signed')),
                'amount_residual_signed': str(safe_get(record, 'amount_residual_signed')),
                'payment_state': str(safe_get(record, 'payment_state')),
                'state': str(safe_get(record, 'state')),
                'ref': str(safe_get(record, 'ref')),
                'to_check': str(safe_get(record, 'to_check')),
                'payment_reference': str(safe_get(record, 'payment_reference')),

                # Handling list field: warehouse_id
                'warehouse_id': str(record['warehouse_id'][0]) if isinstance(record['warehouse_id'], list) and len(record['warehouse_id']) > 0 else str(record['warehouse_id']) if not isinstance(record['warehouse_id'], list) else None,

                # Timestamp fields
                'date': format_timestamp(safe_get(record, 'date')),
                'invoice_date': format_timestamp(safe_get(record, 'invoice_date')),
                'delivery_date': format_timestamp(safe_get(record, 'delivery_date')),
                'invoice_date_due': format_timestamp(safe_get(record, 'invoice_date_due')),
                'write_date': format_timestamp(safe_get(record, 'write_date')),
                'create_date': format_timestamp(safe_get(record, 'create_date')),

                # Handling activity_ids (assuming it's a list, but you may need more detailed handling)
                'activity_ids': str(record['activity_ids'][0]) if isinstance(record['activity_ids'], list) and len(record['activity_ids']) > 0 else None
            }

            processed_records.append(processed_record)

        return processed_records
    
    """Fetch Account Move Lines from Odoo API."""
    def fetch_account_move_lines(self):
        
        fields = [
            "product_id", "product_template_id", "name", "stock_item_note", "price_unit", 
            "quantity", "product_uom_id", "rrp_price", "tax_ids", "price_subtotal", 
            "deferred_start_date", "deferred_end_date", "discount", "before_rebate_price", 
            "rebate_perc", "after_rebate_price", "move_id"
        ]
        
        # Fetch data from Odoo (assuming the _make_request method is available)
        records = self._make_request('account.move.line', fields)

        if not records:
            logging.info("No account move lines found.")
            return []

        processed_records = []
        for record in records:
            processed_record = {
                'id': str(safe_get(record, 'id')),

                # Safely handling list fields: product_id, product_uom_id, move_id
                'product_id_id': str(record['product_id'][0]) if isinstance(record['product_id'], list) and len(record['product_id']) > 0 else str(record['product_id']) if not isinstance(record['product_id'], list) else None,
                'product_id_name': str(record['product_id'][1]) if isinstance(record['product_id'], list) and len(record['product_id']) > 1 else None,

                'product_template_id': str(record['product_template_id']) if record['product_template_id'] else None,

                'product_uom_id': str(record['product_uom_id'][0]) if isinstance(record['product_uom_id'], list) and len(record['product_uom_id']) > 0 else str(record['product_uom_id']) if not isinstance(record['product_uom_id'], list) else None,
                'product_uom_name': str(record['product_uom_id'][1]) if isinstance(record['product_uom_id'], list) and len(record['product_uom_id']) > 1 else None,

                'move_id_id': str(record['move_id'][0]) if isinstance(record['move_id'], list) and len(record['move_id']) > 0 else str(record['move_id']) if not isinstance(record['move_id'], list) else None,
                'move_id_name': str(record['move_id'][1]) if isinstance(record['move_id'], list) and len(record['move_id']) > 1 else None,

                # Regular fields
                'name': str(safe_get(record, 'name')),
                'stock_item_note': str(safe_get(record, 'stock_item_note')),
                'price_unit': str(safe_get(record, 'price_unit')),
                'quantity': str(safe_get(record, 'quantity')),
                'rrp_price': str(safe_get(record, 'rrp_price')),
                'tax_ids': str(record['tax_ids'][0]) if isinstance(record['tax_ids'], list) and len(record['tax_ids']) > 0 else None,
                'price_subtotal': str(safe_get(record, 'price_subtotal')),
                'deferred_start_date': format_timestamp(safe_get(record, 'deferred_start_date')),
                'deferred_end_date': format_timestamp(safe_get(record, 'deferred_end_date')),
                'discount': str(safe_get(record, 'discount')),
                'before_rebate_price': str(safe_get(record, 'before_rebate_price')),
                'rebate_perc': str(safe_get(record, 'rebate_perc')),
                'after_rebate_price': str(safe_get(record, 'after_rebate_price'))
            }

            processed_records.append(processed_record)

        return processed_records

    """Fetch Stock Inventory (stock.picking) from Odoo API."""
    def fetch_stock_inventory(self):
        
        fields = [
            "name", "location_id", "origin", "user_id", "partner_id", 
            "location_dest_id", "total_value", "date_done", "state", 
            "write_date", "create_date", "product_id", "product_quantity"
        ]
        
        # Fetch data from Odoo (assuming the _make_request method is available)
        stock = self._make_request('stock.picking', fields)

        if not stock:
            logging.info("No stock pickings found.")
            return []

        processed_records = []
        for record in stock:
            processed_record = {
                'id': str(safe_get(record, 'id')),

                # Safely handling list fields: location_id, partner_id, location_dest_id, product_id
                'location_id_id': str(record['location_id'][0]) if isinstance(record['location_id'], list) and len(record['location_id']) > 0 else str(record['location_id']) if not isinstance(record['location_id'], list) else None,
                'location_id_name': str(record['location_id'][1]) if isinstance(record['location_id'], list) and len(record['location_id']) > 1 else None,

                'partner_id_id': str(record['partner_id'][0]) if isinstance(record['partner_id'], list) and len(record['partner_id']) > 0 else str(record['partner_id']) if not isinstance(record['partner_id'], list) else None,
                'partner_id_name': str(record['partner_id'][1]) if isinstance(record['partner_id'], list) and len(record['partner_id']) > 1 else None,

                'location_dest_id_id': str(record['location_dest_id'][0]) if isinstance(record['location_dest_id'], list) and len(record['location_dest_id']) > 0 else str(record['location_dest_id']) if not isinstance(record['location_dest_id'], list) else None,
                'location_dest_id_name': str(record['location_dest_id'][1]) if isinstance(record['location_dest_id'], list) and len(record['location_dest_id']) > 1 else None,

                'product_id_id': str(record['product_id'][0]) if isinstance(record['product_id'], list) and len(record['product_id']) > 0 else str(record['product_id']) if not isinstance(record['product_id'], list) else None,
                'product_id_name': str(record['product_id'][1]) if isinstance(record['product_id'], list) and len(record['product_id']) > 1 else None,

                # Regular fields
                'name': str(safe_get(record, 'name')),
                'origin': str(safe_get(record, 'origin')),
                'user_id': str(safe_get(record, 'user_id')),  # Assuming user_id is not a list
                'total_value': str(safe_get(record, 'total_value')),
                'product_quantity': str(safe_get(record, 'product_quantity')),
                'state': str(safe_get(record, 'state')),

                # Timestamp fields
                'date_done': format_timestamp(safe_get(record, 'date_done')),
                'write_date': format_timestamp(safe_get(record, 'write_date')),
                'create_date': format_timestamp(safe_get(record, 'create_date'))
            }

            processed_records.append(processed_record)

        return processed_records


    def fetch_contacts(self):
        """Fetch Contacts (res.partner) from Odoo API."""
        fields = [
            "name", "cust_category_id", "contact_type", "stop_supply", "write_date", "create_date"
        ]
        
        # Fetch data from Odoo (assuming the _make_request method is available)
        contacts = self._make_request('res.partner', fields)

        if not contacts:
            logging.info("No contacts found.")
            return []

        processed_records = []
        for record in contacts:
            processed_record = {
                'id': str(safe_get(record, 'id')),

                # Regular fields (None are lists in this case)
                'name': str(safe_get(record, 'name')),
                'cust_category_id': str(safe_get(record, 'cust_category_id')),  # Assuming cust_category_id is not a list
                'contact_type': str(safe_get(record, 'contact_type')),
                'stop_supply': str(safe_get(record, 'stop_supply')),

                # Timestamp fields
                'write_date': format_timestamp(safe_get(record, 'write_date')),
                'create_date': format_timestamp(safe_get(record, 'create_date'))
            }
            processed_records.append(processed_record)
        return processed_records

    """Fetch Manufacturing Orders (mrp.production) from Odoo API."""
    def fetch_manufacturing(self):
       
        fields = [
            "name", "date_start", "date_finished", "date_deadline", "product_id", 
            "lot_producing_id", "bom_id", "origin", "user_id", "components_availability", 
            "reservation_state", "product_qty", "product_uom_id", "batch_production_id", 
            "state", "write_date", "create_date"
        ]
        
        # Fetch data from Odoo API using the class method _make_request
        records = self._make_request('mrp.production', fields)

        if not records:
            logger.info("No manufacturing orders found.")
            return []

        processed_records = []
        for record in records:
            processed_record = {
                'id': str(safe_get(record, 'id')),

                # Regular fields
                'name': str(safe_get(record, 'name')),
                'date_start': format_timestamp(safe_get(record, 'date_start')),
                'date_finished': format_timestamp(safe_get(record, 'date_finished')),
                'date_deadline': format_timestamp(safe_get(record, 'date_deadline')),
                'origin': str(safe_get(record, 'origin')),
                'components_availability': str(safe_get(record, 'components_availability')),
                'reservation_state': str(safe_get(record, 'reservation_state')),
                'product_qty': str(safe_get(record, 'product_qty')),
                'state': str(safe_get(record, 'state')),

                # Handling list fields: product_id, bom_id, product_uom_id
                'product_id_id': str(record['product_id'][0]) if isinstance(record['product_id'], list) and len(record['product_id']) > 0 else str(record['product_id']) if not isinstance(record['product_id'], list) else None,
                'product_id_name': str(record['product_id'][1]) if isinstance(record['product_id'], list) and len(record['product_id']) > 1 else None,

                'bom_id_id': str(record['bom_id'][0]) if isinstance(record['bom_id'], list) and len(record['bom_id']) > 0 else str(record['bom_id']) if not isinstance(record['bom_id'], list) else None,
                'bom_id_name': str(record['bom_id'][1]) if isinstance(record['bom_id'], list) and len(record['bom_id']) > 1 else None,

                'product_uom_id_id': str(record['product_uom_id'][0]) if isinstance(record['product_uom_id'], list) and len(record['product_uom_id']) > 0 else str(record['product_uom_id']) if not isinstance(record['product_uom_id'], list) else None,
                'product_uom_id_name': str(record['product_uom_id'][1]) if isinstance(record['product_uom_id'], list) and len(record['product_uom_id']) > 1 else None,

                # Other fields that might be false
                'lot_producing_id': str(safe_get(record, 'lot_producing_id')),
                'batch_production_id': str(safe_get(record, 'batch_production_id')),
                'user_id': str(safe_get(record, 'user_id')),

                # Timestamp fields
                'write_date': format_timestamp(safe_get(record, 'write_date')),
                'create_date': format_timestamp(safe_get(record, 'create_date'))
            }

            processed_records.append(processed_record)
        return processed_records