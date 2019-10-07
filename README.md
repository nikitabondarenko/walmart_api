# walmart_api

3 Scripts

1) run walmart_first_pull.py the first time you try to pull inventory data from walmart

2) after you run that once you can setup up walmart_new_pulls.py to run. This script sees which categories products were most recently pulled and begins pulling from the next category until the API call limit is reached for the day. These new products are updated into the walmart postgres table. The script makes sures to get rid of duplicate products.

