import logging
import json
from odoo_api import OdooAPI
from bigquery_handler import BigQueryHandler
from utils import load_config

logging.basicConfig(level=logging.INFO)

def main(cloud_event, abc):
    # Load configuration
    config = load_config()

    # Initialize OdooAPI and BigQueryHandler
    odoo_api = OdooAPI(config['odoo'])
    bigquery_handler = BigQueryHandler(config['bigquery'])

    ##### Fetch Sales Orders from Odoo #####
    sales_orders = odoo_api.fetch_sales_orders()
    if sales_orders:
        # Insert Sales Orders into BigQuery
        bigquery_handler.insert_into_bigquery_in_batches('sales_orders', sales_orders, 1)
    else:
        logging.info("No sales orders fetched.")

    ##### Sales Orders Lines #####
    sales_order_line = odoo_api.fetch_sales_order_line()
    if sales_order_line:
        # Insert Sales Order Lines into BigQuery
        bigquery_handler.insert_into_bigquery_in_batches('sales_order_line', sales_order_line, 1)
    else:
        logging.info("No sales order line fetched.")

    ##### Fetch Purchase Orders from Odoo #####
    purchase_orders = odoo_api.fetch_purchase_orders()
    if purchase_orders:
        # Insert Purchase Orders into BigQuery
        bigquery_handler.insert_into_bigquery_in_batches('purchase_orders', purchase_orders, 1)
    else:
        logging.info("No purchase orders fetched.")

    ##### Fetch Purchase Oders Lines #####
    purchase_order_line = odoo_api.fetch_purchase_order_line()
    if purchase_order_line:
        # Insert Purchase Order Lines into BigQuery
        bigquery_handler.insert_into_bigquery_in_batches('purchase_order_line', purchase_order_line, 1)
    else:
        logging.info("No purchase order line fetched.")

    ##### Fetch Accounts #####
    accounts = odoo_api.fetch_accounts()
    if accounts:
        # Insert Accounts into BigQuery
        bigquery_handler.insert_into_bigquery_in_batches('accounts', accounts, 200)
    else:
        logging.info("No account fetched.")

    ##### Fetch Account Move Lines from Odoo #####

    account_move_lines = odoo_api.fetch_account_move_lines()
    if account_move_lines:
        # Insert Account Move Lines into BigQuery
        bigquery_handler.insert_into_bigquery_in_batches('account_move_lines', processed_records, 200)
    else:
        logging.info("No account move lines fetched.")

    ##### Fetch Stock Inventory #####
    stock_inventory = odoo_api.fetch_stock_inventory()
    if stock_inventory:
        # Insert Stock Inventory into BigQuery
        bigquery_handler.insert_into_bigquery_in_batches('stock_inventory', stock_inventory, 1)
    else:
        logging.info("No stock inventory fetched.")

    ##### Fetch Contacts #####
    contacts = odoo_api.fetch_contacts()
    if contacts:
        bigquery_handler.insert_into_bigquery_in_batches('contacts', contacts, 1)
    else:
        logging.info("No contacts fetched.")

    ##### Fetch Manufacturing #####
    mnf = odoo_api.fetch_manufacturing()
    if mnf:
        bigquery_handler.insert_into_bigquery_in_batches('manufacturing', mnf, 1)
    else:
        logging.info("No contacts fetched.")
