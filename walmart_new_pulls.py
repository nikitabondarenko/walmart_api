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
engine = create_engine('postgresql://deployer:p0877e546c0539179b1fb234649e2aba6691a28b53815b7710a93d65cc21c2059@haystack.c9ylakvczd8w.us-west-2.rds.amazonaws.com:5432/haystack3_production')
connection = engine.connect()

#allProds = pd.read_csv('walmart_all_products.csv')

#wtax = pd.read_excel('walmart_taxonomy_lowest_level_categories.xlsx')

api_key = 'zd9j7ey5b6qzmh2fv2m8nyyv'


###############################
#### comment out from here if don't want to pull taxonomy file
#Taxonomy API 
req = requests.get('http://api.walmartlabs.com/v1/taxonomy?apiKey=zd9j7ey5b6qzmh2fv2m8nyyv&format=json')
taxonomy = req.json()

#function to parse through nested list of dictionaries from Taxonomy API
def taxonomyParse(listOfDicts):
    row = []
    for i in range(0, len(listOfDicts)):
        #add all categories (code for only lowest categories below)
        row.append([listOfDicts[i]['id'], listOfDicts[i]['name'], listOfDicts[i]['path']])
        #only adds lowest level categories (no children)
        #if 'children' not in listOfDicts[i].keys():
        #    row.append([listOfDicts[i]['id'], listOfDicts[i]['name'], listOfDicts[i]['path']])
        if 'children' in listOfDicts[i].keys():
            print(listOfDicts[i].keys())
            row+= taxonomyParse(listOfDicts[i]['children'])
            #id.append(listOfDicts[i]['id'])
            #name.append(listOfDicts[i]['name'])
            #path.append(listOfDicts[i]['path'])
            #children.append(listOfDicts[i]['children'])
            #taxonomyParse()
            #children.append('None')
    #df = pd.DataFrame({'id':id, 'name':name,'path':path})
    return row

walmart_taxonomy = taxonomyParse(taxonomy['categories'])

#convert list of lists to DF
wtax = pd.DataFrame(walmart_taxonomy)
wtax.columns = ["category_id", "name", "path"]

#save to excel File, comment out if not necessary
#excelFile = 'walmart_taxonomy_lowest_level_categories.xlsx'
excelFile = 'walmart_taxonomy.xlsx'
wtax.to_excel(excelFile, index=False)

#send table to SQL
table_name = 'walmart_taxonomy'
wtax.to_sql(table_name, con = engine, if_exists = "replace", chunksize = 10)

###############################
### end comment out if don't want to repull taxonomy


#Pull Walmart taxonomy get from postgres. ordered so top categories are shown first
#wtaxLeft = pd.read_sql("select * from walmart_taxonomy order by length(category_id)", engine)
print("reading in walmart_taxonomy")
wtaxLeft = pd.read_sql("select * from walmart_taxonomy", engine)

#get all walmart skus from cw_products
print("getting in all walmart skus from cw_products")
currentskus = pd.read_sql("select sku from cw_products where site_type_id = 5", engine)


print("getting category_id of most recent product")
#currentCatID = pd.read_sql("select category_id from walmart_current order by created_at desc limit 1", engine)['category_id'][0]
currentUPC = pd.read_sql("select upc from cw_products where site_type_id = 5 and in_store = 1 and upc is not null order by created_at desc limit 1", engine)
cUPC = currentUPC["upc"][0]
req = requests.get('http://api.walmartlabs.com/v1/search?apiKey=zd9j7ey5b6qzmh2fv2m8nyyv&format=json&query='+cUPC)
currentCatID = req.json()['items'][0]['categoryNode']

print("getting index of walmart_taxonomy of current category id")
currentCat = pd.read_sql("select index from walmart_taxonomy where category_id = '"+currentCatID+"'", engine)['index'][0]

#currentCatIX = pd.read_sql("select index from walmart_taxonomy where category_id = '"+currentCatID+"'", engine)['index'][0]

print("if current category index < length keep going")
#if current category index < length, keep going
#if currentCatID != wtaxLeft.iloc[len(wtaxLeft)-1]['category_id'] and currentCatID != '6735581_4705218_6175357':
if currentCatID != wtaxLeft.iloc[len(wtaxLeft)-1]['category_id']:

    print("shorten wtaxLeft to start at the right index")
    wtaxLeft = wtaxLeft[currentCatIX:]
    wtaxLeft = wtaxLeft.reset_index()
#if current category index == length, start over cycle
else:
    print("reset current Category index to 0")
    currentCatIX == 0
    

#Pull Walmart taxonomy but exclude categories that have already been pulled
#wtaxLeft = pd.read_sql("select * from walmart_taxonomy where category_id not in (select distinct category_id from walmart)", engine)

#ordered so top level categories are shown first
#wtaxLeft = pd.read_sql("select * from walmart_taxonomy where category_id not in (select distinct category_id from walmart) order by length(category_id)", engine)


#pull current walmart products
#oldProducts = pd.read_sql("select * from walmart", engine)

#categories = pd.read_sql("select category_id from walmart", engine)

#drop duplicate products, comment out if needed
#oldProducts = oldProducts.drop_duplicates(subset='name', keep='first')
#oldProducts = oldProducts.drop_duplicates(subset=['name', 'url', 'category_path', 'upc'],keep='first')

#get category of the last product pulled
#recentCategory = oldProducts['category_id'].iloc[-1]

#get index value of last category pulled
#catIx = wtax.loc[wtax['category_id']==recentCategory].index 

allProducts = []

def sendtoSQL(currentProducts):
    #convert list of lists to DF
    #once we have a list of all the categories products, we can send to SQL (one at a time)
    if currentProducts is None:
        print("currentProducts is None")
    else:
        try:
            print(currentProducts)
            print("length currentProducts: " + str(len(currentProducts)))
            currentProducts = pd.DataFrame(currentProducts)
            currentProducts.columns =["in_store", "name", "category_id", "category_path", "url", "site_type_id", "created_at", "updated_at", "upc", "unique_attrs", "orig_thumbnail_url", "orig_image_misc_url", "brand", "external_product_id", "price", "gender"]
            #wtax.columns = ["id", "name", "path"]
            
          
            #convert unique_attrs dictionary to string
            print("convert unique attrs to string")
            currentProducts['unique_attrs'] = currentProducts['unique_attrs'].apply(lambda x:str(x).replace("'",""))
            
            #add in_store_boolean
            print("adding in_store=1 or 0 boolean")
            currentProducts['in_store_boolean'] = [1 if x =='Available' or x== 'available' or x=='Limited Supply' or x=='Limited supply' or x == 'Last Few Items' else 0 for x in currentProducts['in_store']]
            
            print(currentProducts)
            print("updating_walmart_current")
            table_name = 'walmart_current'
            currentProducts.to_sql(table_name, con = engine, if_exists = "replace", chunksize = 10)
                        
            #add to cw_products
            #two cases
            #1) id's that are already in cw_products
            #UPDATE
            #updates products with skus that are already in table if the updated_time is more recent
            print("UPDATING cw_products skus that are already in table (if updated_time is more recent)")
            connection.execute("UPDATE cw_products c SET in_store = w.in_store_boolean, name = w.name, updated_at = w.updated, inventory_last_seen_at = w.updated, price = w.price, external_product_id = w.external_product_id FROM (select in_store_boolean, name, category_path, price, external_product_id, updated_at::timestamp updated, concat(name, ' ', upc) as nameupc from walmart_current where external_product_id in (select cast(sku as bigint) from cw_products where site_type_id = 5)) w WHERE concat(c.name, ' ', c.upc) = w.nameupc and c.updated_at < w.updated")
            
            #2) new id's 
            #INSERT
            #inserts products where the sku has not been added to 
            print("INSERTING cw_product skus that have not been seen before")
            
            #this is not working because cw_products_id_seq is messed up. using for loop instead 
            #connection.execute("INSERT INTO cw_products (in_store, name, category_path, sku, url, site_type_id, created_at, updated_at, upc, unique_attrs, inventory_last_seen_at, orig_thumbnail_url, orig_image_misc_url, brand, external_product_id, price, gender) SELECT in_store_boolean, name, category_path, external_product_id, url, site_type_id, created_at::timestamp, updated_at::timestamp, upc, unique_attrs, updated_at::timestamp, orig_thumbnail_url, orig_image_misc_url, brand, external_product_id, price, gender from walmart_current where external_product_id not in (select cast(sku as bigint) from cw_products where site_type_id = 5)")
            
            wc = pd.read_sql("SELECT count(*) from walmart_current where external_product_id not in (select cast(sku as bigint) from cw_products where site_type_id = 5)", engine)
            wc_count = wc['count'][0]
            for i in range(0, wc_count):
                print ("adding new row to cw_products")
                connection.execute("INSERT INTO cw_products (id, in_store, name, category_path, sku, url, site_type_id, created_at, updated_at, upc, unique_attrs, inventory_last_seen_at, orig_thumbnail_url, orig_image_misc_url, brand, external_product_id, price, gender) SELECT (SELECT (max(id) + 1) FROM cw_products), in_store_boolean, name, category_path, cast(external_product_id as varchar), url, site_type_id, created_at::timestamp, updated_at::timestamp, upc, unique_attrs, updated_at::timestamp, orig_thumbnail_url, orig_image_misc_url, brand, external_product_id, price, gender from walmart_current where external_product_id not in (select cast(sku as bigint) from cw_products where site_type_id = 5) limit 1")
                

            #add menu_name, category_name, subcategory_name from category_path
            #print("fill in menu_name, category_name, subcategory_name columns for new products using category_path column")
            #connection.excute("UPDATE cw_products set category_path = replace(category_path, 'Home Page/', '') WHERE menu_name is null and site_type_id = 5 and category_path ilike 'Home Page%'")
            #connection.excute("UPDATE cw_products c1 SET menu_name = split_part(c2.category_path, '/', 1), category_name = split_part(c2.category_path, '/', 2), subcategory_name = split_part(c2.category_path, '/', 3) FROM cw_products c2 WHERE c1.sku = c2.sku AND c1.menu_name is null and c1.site_type_id = 5")

        except Exception:
            print("InternalError")
            pass  # or you could use 'continue'

def getPageProducts(productArray, category_id):
    productList = []
    print("length productArray: " + str(len(productArray)))
    for i in range(0, len(productArray)):
        #print("Product Number: "+ str(i))
        #print(productArray[i]['name'])
        in_store = productArray[i]['stock']
        name = productArray[i]['name']
        if 'categoryPath' in productArray[i].keys():
            category_path = productArray[i]['categoryPath']
        else:
            category_path = None
        #sku = productArray[i]['']
        if 'productUrl' in productArray[i].keys():
            url = productArray[i]['productUrl']
        else:
            url = None
        if 'upc' in productArray[i].keys():
            upc = productArray[i]['upc']
        else:
            upc = None
        #unique attributes (only add if exists in keys)
        unique_attrs = {'stock': productArray[i]['stock']}
        if 'parentItemId' in productArray[i].keys():
            unique_attrs['parentItemId'] = productArray[i]['parentItemId']
        if 'productTrackingUrl' in productArray[i].keys():
            unique_attrs['productTrackingUrl'] = productArray[i]['productTrackingUrl']
        if 'clearance' in productArray[i].keys():
            unique_attrs['clearance'] = productArray[i]['clearance']
        if 'freeShippingOver35Dollars' in productArray[i].keys():
            unique_attrs['freeShippingOver35Dollars'] = productArray[i]['freeShippingOver35Dollars']    
        if 'rhid' in productArray[i].keys():
            unique_attrs['rhid'] = productArray[i]['rhid']
        if 'attributes' in productArray[i].keys():
            unique_attrs['attributes'] = productArray[i]['attributes']
        if 'shortDescription' in productArray[i].keys():
            unique_attrs['shortDescription'] = productArray[i]['shortDescription']  
        if 'longDescription' in productArray[i].keys():
            unique_attrs['longDescription'] = productArray[i]['longDescription'] 
        if 'preOrder' in productArray[i].keys():
            unique_attrs['preOrder'] = productArray[i]['preOrder']
        if 'addToCartUrl' in productArray[i].keys():
            unique_attrs['addToCartUrl'] = productArray[i]['addToCartUrl']
        if 'affiliateAddToCartUrl' in productArray[i].keys():
            unique_attrs['affiliateAddToCartUrl'] = productArray[i]['affiliateAddToCartUrl']
        
        if 'thumbnailImage' in productArray[i].keys():
            orig_thumbnail_url = productArray[i]['thumbnailImage']
        else:
            orig_thumbnail_url = None
        if 'largeImage' in productArray[i].keys():
            orig_image_misc_url = productArray[i]['largeImage']
        else:
            orig_image_misc_url = None
        if 'brandName' in productArray[i].keys():
            brand = productArray[i]['brandName']
        else:
            brand = None
        if 'itemId' in productArray[i].keys():
            external_product_id = productArray[i]['itemId']
        else:
            external_product_id = None
        if 'salePrice' in productArray[i].keys():
            price = productArray[i]['salePrice']
        else:
            price = None
        if 'gender' in productArray[i].keys():
            gender = productArray[i]['gender']
        else:
            gender = None
        #gender = productArray[i]['gender']
        site_type_id = 5
        created_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        updated_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        newProduct = [in_store, name, category_id, category_path, url, site_type_id, created_at, updated_at, upc, json.dumps(unique_attrs), orig_thumbnail_url, orig_image_misc_url, brand, external_product_id, price, gender]
        print(newProduct)
        productList.append(newProduct)
    print("length of product list: " + str(len(productList)))
    #print(productList)
    if len(productList)>0:
        return productList

def getCategoryProducts(category_id):
    #first page
    request = 'http://api.walmartlabs.com/v1/paginated/items?category=' + category_id + '&apiKey=' + api_key
    req = requests.get(request)
    #protect from raised JSONDecodeError("Expecting value", s, err.value) from None for certain categories
    try:
        products = req.json()
        #get products for the first page
        allCatProducts = getPageProducts(products['items'], category_id)
        print(products['totalPages'])
        print(type(products['totalPages']))
        #length = len(products['totalPages'])
        length = products['totalPages']
        if length > 1:
            #get products for the next pages if more than 1 page
            for i in range(1, length):
                #get maxID so we are able to go to next page
                print("Category Page: " + str(i))
                #the two rows below were how nextPage requests were made before API was updated
                #the two lines under that were added in December 2020 based off these new changes
                #maxId = products['nextPage'].split('maxId=')[1].split('&apiKey')[0]
                #newRequest = 'http://api.walmartlabs.com/v1/paginated/items?category=' + category_id + '&apiKey=' + api_key + '&maxId=' + maxId
                nextPage = products['nextPage']
                newRequest = 'http://api.walmartlabs.com'+nextPage
                newReq = requests.get(newRequest)
                products = newReq.json()
                #add new products to category product list
                pageProducts = getPageProducts(products['items'], category_id)
                print("length of page products: " + str(len(pageProducts)))
                allCatProducts = allCatProducts + pageProducts
                #print(allCatProducts)
        if allCatProducts is None:
            "allCatProducts is None"
        else:
            sendtoSQL(allCatProducts)
        return allCatProducts
    except json.decoder.JSONDecodeError:
        print("N'est pas JSON")
        return []


def getAllCategories(categories):
    for i in range(0, len(categories)):
    #if rerunning from specific category index (catIx)
    #for i in range(catIx[0]+1, len(categories)):
    #for i in range(catIx[0], len(categories)):
    #reverse
    #for i in range(len(categories)-1, -1, -1):
        sleep(1)
        currentCatID = categories['category_id'][i]
        print(categories['name'][i])
        #print(categories['category_id'][i])
        print(currentCatID)
        global allProducts
        categoryProducts = getCategoryProducts(categories['category_id'][i])
        if categoryProducts is None:
            print("category products is none")
        else:
            allProducts = allProducts + categoryProducts
    #print("vacuuming walmart_current")
    #connection.execute("vacuum walmart_current")
    #print("vacuuming cw_products")
    #connection.execute("vacuum cw_products")
    return allProducts


allProds = getAllCategories(wtaxLeft)

#rerun if breaks (get new recent category )
#recentCategory = allProducts[-1][2]
#catIx = wtax.loc[wtax['category_id']==recentCategory].index 
#allProds = getAllCategories(wtax)

#currentCatID = '4171_4173_8031140'
#catIx = wtaxLeft.loc[wtaxLeft['name']=='VideoGames'].index
#catIx = wtaxLeft.loc[wtaxLeft['category_id']=='976760_1166769_4270807'].index

#convert list of lists to DF
#allProducts = pd.DataFrame(allProducts)
#allProducts.columns =["in_store", "name", "category_id", "category_path", "url", "site_type_id", "created_at", "updated_at", "upc", "unique_attrs", "orig_thumbnail_url", "orig_image_misc_url", "brand", "external_product_id", "price", "gender"]
#wtax.columns = ["id", "name", "path"]

#allProductsNew = oldProducts.append(allProducts)

#drop potential duplicates
#allProductsNew.drop_duplicates(subset=['name', 'category_id', 'category_path', 'url']) 


#save to excel File, comment out if not necessary
#csvFile = 'walmart_all_products.csv'
#allProds.to_csv(csvFile, index=False)

#send table to SQL
#table_name = 'walmart'
#allProductsNew.to_sql(table_name, con = engine, if_exists = "replace", chunksize = 10)

#CLEAN UP CATEGORY PATH and adds menu_name category_name, subcategory_name
print("UPDATING category path to remove 'Home Page' if there:")
connection.execute("UPDATE cw_products set category_path = replace(category_path, 'Home Page/', '') WHERE menu_name is null and site_type_id = 5 and category_path ilike 'Home Page%'")
print("UPDATING menu_name category_name, subcategory_name from category_path if not there:")
connection.execute("UPDATE cw_products c1 SET menu_name = split_part(c2.category_path, '/', 1), category_name = split_part(c2.category_path, '/', 2), subcategory_name = split_part(c2.category_path, '/', 3) FROM cw_products c2 WHERE c1.sku = c2.sku AND c1.menu_name is null and c1.site_type_id = 5")


connection.close()
engine.dispose()


