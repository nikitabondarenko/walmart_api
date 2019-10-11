# walmart_api

3 Scripts

1) run walmart_first_pull.py the first time you try to pull inventory data from walmart

2) after you run that once you can setup up walmart_new_pulls.py to run. This script sees which see the categories' products that were most recently pulled and begins pulling from the next category until the API call limit is reached for the day. These new products are updated into a temporary postgres table called walmart_current. From this table cw_products is updated. If the sku has been seen before we UPDATE the corresponding rows. If the sku has not been seen before we INSERT new rows.

Note: if you set up a cronjob for walmart_new_pulls.py then cw_products will continue being updated for new products.

Note: can be difficult to install psycopg2 dependency on AWS ec2
