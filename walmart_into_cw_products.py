import requests
import pandas
import pandas as pd
from pandas import DataFrame
import json
from sqlalchemy import create_engine
import time
import datetime
from time import sleep

print("connecting to postgres server via SQLAlchemy + psycopg2")
#INSERT DATABASE CREDENTIALS
engine = create_engine(#INSERT CREDENTIALS)
connection = engine.connect()

api_key = #INSERT API KEY

#remove dupliactes from walmart table
print("removing duplicates from walmart table")
connection.execute("DELETE FROM walmart WHERE external_product_id IN (SELECT external_product_id FROM (SELECT external_product_id, ROW_NUMBER() OVER( PARTITION BY external_product_id ORDER BY updated_at DESC ) AS row_num FROM walmart ) t WHERE t.row_num > 1 );")

#Pull Walmart taxonomy get from postgres. ordered so top categories are shown first
#wtaxLeft = pd.read_sql("select * from walmart_taxonomy order by length(category_id)", engine)
#print("reading in walmart table")
#walmart = pd.read_sql("select * from walmart order by created_at desc", engine)
#print("reading in cw_products table")
#cw_walmart = pd.read_sql("select * from cw_products where site_type_id = 5", engine)

#remove dupliactes from duplicates 
#print("removing duplicates from walmart table")
#walmart = walmart.drop_duplicates(subset='external_product_id')

#add in_store_boolean
print("adding in_store=1 boolean")
connection.execute("update walmart set in_store_boolean = 1 where in_store='Avaiable' or in_store = 'Limited Supply' or in_store = 'Last Few Items'")
print("adding in_store=0 boolean")
connection.execute("update walmart set in_store_boolean = 0 where in_store='Not available'")
#walmart.loc[walmart['in_store'] == 'Available', ['in_store_boolean']] = 1
#walmart.loc[walmart['in_store'] == 'Limited Supply', ['in_store_boolean']] = 1
#walmart.loc[walmart['in_store'] == 'Last Few Items', ['in_store_boolean']] = 1
#walmart.loc[walmart['in_store'] == 'Not available', ['in_store_boolean']] = 0

#walmart['external_product_id'] 

#print("sending walmart table back to sql")
#table_name = 'walmart_distinct'
#walmart.to_sql(table_name, con = engine, if_exists = "replace")


#two cases
#1) id's that are already in cw_products
#UPDATE
#updates products with skus that are already in table if the updated_time is more recent
print("UPDATE cw_products for skus that are already in table if updated_time is more recent")
connection.execute("UPDATE cw_products c SET in_store = w.in_store_boolean, name = w.name, updated_at = w.updated, inventory_last_seen_at = w.updated, price = w.price, external_product_id = w.external_product_id FROM (select in_store_boolean, name, category_path, price, external_product_id, updated_at::timestamp updated, concat(name, ' ', upc) as nameupc from walmart where external_product_id in (select cast(sku as bigint) from cw_products where site_type_id = 5)) w WHERE concat(c.name, ' ', c.upc) = w.nameupc and c.updated_at < w.updated")
print()


#2) new id's 
#INSERT
#inserts products where the sku has not been added to 
print("INSERT into cw_products when the sku has not been seen before")
connection.execute("INSERT INTO cw_products (in_store, name, category_path, sku, url, site_type_id, created_at, updated_at, upc, unique_attrs, inventory_last_seen_at, orig_thumbnail_url, orig_image_misc_url, brand, external_product_id, price, gender) SELECT in_store_boolean, name, category_path, external_product_id, url, site_type_id, created_at::timestamp, updated_at::timestamp, upc, unique_attrs, updated_at::timestamp, orig_thumbnail_url, orig_image_misc_url, brand, external_product_id, price, gender from walmart where external_product_id not in (select cast(sku as bigint) from cw_products where site_type_id = 5)")

print("close connection")
connection.close()
engine.dispose()




#best step?
#products that are in walmart with upcs that are already in cw_products with a gpc_product_id