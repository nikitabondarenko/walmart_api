import requests
import pandas
import pandas as pd
from pandas import DataFrame
import json
from sqlalchemy import create_engine
import time
import datetime

#INSERT DATABASE CREDENTIALS
engine = create_engine(#INSERT CREDENTIALS)
connection = engine.connect()

#allProds = pd.read_csv('walmart_all_products.csv')

#wtax = pd.read_excel('walmart_taxonomy_lowest_level_categories.xlsx')

api_key = #INSERT API KEY

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


#Paginated Products API

#Get all products of a category using the category ID
#product count cannot exceed 100
#'id': '4125_546956_4128',
#       'name': 'Camping Gear',
#       'path': 'Sports & Outdoors/Outdoor Sports/Camping Gear'},
#category_id = wtax['id'][0]
#category_id = '4125_546956_4128'
#request = 'http://api.walmartlabs.com/v1/paginated/items?category=' + category_id + '&apiKey=' + api_key
#req = requests.get('http://api.walmartlabs.com/v1/paginated/items?category=4125_546956_4128&apiKey=zd9j7ey5b6qzmh2fv2m8nyyv')
#products = req.json()

def sendtoSQL(currentProducts):
    #convert list of lists to DF
    currentProducts = pd.DataFrame(currentProducts)
    currentProducts.columns =["in_store", "name", "category_id", "category_path", "menu_name", "category_name", "subcategory_name", "url", "site_type_id", "created_at", "updated_at", "upc", "unique_attrs", "orig_thumbnail_url", "orig_image_misc_url", "brand", "external_product_id", "price", "gender"]
    #wtax.columns = ["id", "name", "path"]
    
  
    #convert unique_attrs dictionary to string so p
    currentProducts['unique_attrs'] = currentProducts['unique_attrs'].apply(lambda x:str(x).replace("'",""))
    
    #send table to SQL (split up by category so that SQLAlchemy works better)
    table_name = 'walmart'
    currentProducts.to_sql(table_name, con = engine, if_exists = "append", chunksize = 10)

def getPageProducts(productArray, category_id):
    productList = []
    print(len(productArray))
    for i in range(0, len(productArray)):
        print("Product Number: "+ str(i))
        print(productArray[i]['name'])
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
        productList.append([in_store, name, category_id, category_path, menu_name, category_name, subcategory_name, url, site_type_id, created_at, updated_at, upc, unique_attrs, orig_thumbnail_url, orig_image_misc_url, brand, external_product_id, price, gender])
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
                allCatProducts = allCatProducts + getPageProducts(products['items'], category_id)
        sendtoSQL(allCatProducts)
        return allCatProducts
    except json.decoder.JSONDecodeError:
        print("N'est pas JSON")
        return []

def getAllCategories(categories):
    for i in range(0, len(categories)):
        print(categories['name'][i])
        global allProducts
        allProducts = allProducts + getCategoryProducts(categories['category_id'][i])
    return allProducts


allProds = getAllCategories(wtax)

#save to csv File, comment out if not necessary
csvFile = 'walmart_all_products.csv'
allProducts.to_csv(csvFile, index=False)


connection.close()
engine.dispose()
