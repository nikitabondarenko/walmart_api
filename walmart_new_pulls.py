import requests
import pandas
import pandas as pd
from pandas import DataFrame
import json
from sqlalchemy import create_engine
import time
import datetime
from time import sleep

#INSERT DATABASE CREDENTIALS
engine = create_engine(#INSERT CREDENTIALS)
connection = engine.connect()

#allProds = pd.read_csv('walmart_all_products.csv')

#wtax = pd.read_excel('walmart_taxonomy_lowest_level_categories.xlsx')

api_key = #INSERT API KEY

#Pull Walmart taxonomy get from postgres
#wtax = pd.read_sql("select * from walmart_taxonomy", engine)

#Pull Walmart taxonomy but exclude categories that have already been pulled
wtaxLeft = pd.read_sql("select * from walmart_taxonomy where category_id not in (select distinct category_id from walmart)", engine)

#ordered so top level categories are shown first
wtaxLeft = pd.read_sql("select * from walmart_taxonomy where category_id not in (select distinct category_id from walmart) order by length(category_id)", engine)


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
    if currentProducts is None:
        print("currentProducts is None")
    else:
        print(currentProducts)
        print("length currentProducts: " + str(len(currentProducts)))
        currentProducts = pd.DataFrame(currentProducts)
        currentProducts.columns =["in_store", "name", "category_id", "category_path", "menu_name", "category_name", "subcategory_name", "url", "site_type_id", "created_at", "updated_at", "upc", "unique_attrs", "orig_thumbnail_url", "orig_image_misc_url", "brand", "external_product_id", "price", "gender"]
        #wtax.columns = ["id", "name", "path"]
        
      
        #convert unique_attrs dictionary to string so p
        currentProducts['unique_attrs'] = currentProducts['unique_attrs'].apply(lambda x:str(x).replace("'",""))
        
        #send table to SQL (split up by category so that SQLAlchemy works better)
        print(currentProducts)
        table_name = 'walmart'
        currentProducts.to_sql(table_name, con = engine, if_exists = "append", chunksize = 10)

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
            split_cp = category_path.split("/")
            length_cp = len(split_cp)
            if length_cp >= 3:
                menu_name = split_cp[0]
                category_name = split_cp[1]
                subcategory_name = split_cp[2]
            if length_cp == 2:
                menu_name = split_cp[0]
                category_name = split_cp[1]
            if length_cp == 1:
                menu_name = split_cp[0]
        else:
            category_path = None
            menu_name = None
            category_name = None
            subcategory_name = None
        #sku = productArray[i]['']
        if 'productUrl' in productArray[i].keys():
            url = productArray[i]['productUrl']
        else:
            url = None
        if 'upc' in productArray[i].keys():
            upc = productArray[i]['upc']
        else:
            upc = None
        unique_attrs = {'parentItemId': productArray[i]['parentItemId'],'productTrackingUrl': productArray[i]['productTrackingUrl'],  
                        'rhid': productArray[i]['rhid'], 'bundle': productArray[i]['bundle'], 'clearance': productArray[i]['clearance'],  'stock': productArray[i]['stock'],
                        'freeShippingOver35Dollars': productArray[i]['freeShippingOver35Dollars']}
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
        newProduct = [in_store, name, category_id, category_path, menu_name, category_name, subcategory_name, url, site_type_id, created_at, updated_at, upc, json.dumps(unique_attrs), orig_thumbnail_url, orig_image_misc_url, brand, external_product_id, price, gender]
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
        length = len(products['totalPages'])
        if length > 1:
            #get products for the next pages if more than 1 page
            for i in range(1, length):
                #get maxID so we are able to go to next page
                print("Category Page: " + str(i))
                maxId = products['nextPage'].split('maxId=')[1].split('&apiKey')[0]
                newRequest = 'http://api.walmartlabs.com/v1/paginated/items?category=' + category_id + '&apiKey=' + api_key + '&maxId=' + maxId
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
    #for i in range(0, len(categories)):
    #if rerunning from specific category index (catIx)
    #for i in range(catIx[0]+1, len(categories)):
    #for i in range(catIx[0], len(categories)):
    #reverse
    for i in range(len(categories)-1, 0, -1):
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
    return allProducts


allProds = getAllCategories(wtaxLeft)

#rerun if breaks (get new recent category )
#recentCategory = allProducts[-1][2]
#catIx = wtax.loc[wtax['category_id']==recentCategory].index 
#allProds = getAllCategories(wtax)

currentCatID = '4171_4173_8031140'
catIx = wtaxLeft.loc[wtaxLeft['name']=='VideoGames'].index
catIx = wtaxLeft.loc[wtaxLeft['category_id']=='976760_1166769_4270807'].index

#convert list of lists to DF
#allProducts = pd.DataFrame(allProducts)
#allProducts.columns =["in_store", "name", "category_id", "category_path", "url", "site_type_id", "created_at", "updated_at", "upc", "unique_attrs", "orig_thumbnail_url", "orig_image_misc_url", "brand", "external_product_id", "price", "gender"]
#wtax.columns = ["id", "name", "path"]

#allProductsNew = oldProducts.append(allProducts)

#drop potential duplicates
#allProductsNew.drop_duplicates(subset=['name', 'category_id', 'category_path', 'url']) 


#save to excel File, comment out if not necessary
csvFile = 'walmart_all_products.csv'
allProds.to_csv(csvFile, index=False)

#send table to SQL
#table_name = 'walmart'
#allProductsNew.to_sql(table_name, con = engine, if_exists = "replace", chunksize = 10)

connection.close()
engine.dispose()


