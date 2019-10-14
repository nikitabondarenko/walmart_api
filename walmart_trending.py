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

request = 'http://api.walmartlabs.com/v1/trends?apiKey=' + api_key + '&publisherId=xyz&format=json'

req = requests.get(request)
trending_products = req.json()

#list(trending_products.keys())
#Out[21]: ['time', 'items']

#(unix?) epoch timestamp
currentTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
#number of trending products
length = len(trending_products['items'])

def sendtoSQL(currentProducts):
    #convert list of lists to DF
    print("converting 2d array into pandas dataframe")
    currentProducts = pd.DataFrame(currentProducts)
    currentProducts.columns =["name", "rank", "totalProducts", "timeStamp", "itemId", "parentItemId", "msrp", "salePrice", "upc", "shortDescription", "brandName", "thumbnailImage", "mediumImage", "largeImage", "productTrackingUrl", "ninetySevenCentShipping", "standardShipRate", "size", "marketplace", "shipToStore", "freeShipToStore", "modelNumber", "productUrl", "customerRating", "numReviews", "customerRatingImage", "categoryNode", "rhid", "bundle", "clearance", "preOrder", "stock", "attributes", "addToCartUrl", "freeShippingOver35Dollars", "giftOptions", "imageEntities", "offerType", "isTwoDayShippingEligible", "availableOnline", "gender"]

    print("sending to SQL")
    #send table to SQL (split up by category so that SQLAlchemy works better)
    table_name = 'walmart_trending'
    currentProducts.to_sql(table_name, con = engine, if_exists = "append")

productList= []

if length > 0:
    #get products for the next pages if more than 1 page
    for i in range(0, length):
        currentProduct = trending_products['items'][i]
        rank = i+1
        totalProducts = length
        timeStamp = currentTime
        itemId = currentProduct['itemId']
        parentItemId  = currentProduct['parentItemId']
        name = currentProduct['name']
        #manufacturer suggested retail price
        if 'msrp' in currentProduct.keys():
            msrp = currentProduct['msrp']
        else:
            msrp = None
        if 'salePrice' in currentProduct.keys():
            salePrice = currentProduct['salePrice']
        else:
            salePrice = None
        if 'upc' in currentProduct.keys():
            upc = currentProduct['upc']
        else:
            upc = None
        categoryPath = currentProduct['categoryPath']
        if 'shortDescription' in currentProduct.keys():
            shortDescription = currentProduct['shortDescription']
        else:
            shortDescription = None
        
        if 'brandName' in currentProduct.keys():
            brandName = currentProduct['brandName']
        else:
            brandName = None
        
        if 'thumbnailImage' in currentProduct.keys():
            thumbnailImage = currentProduct['thumbnailImage']
        else:
            thumbnailImageupc = None
        
        if 'mediumImage' in currentProduct.keys():
            mediumImage = currentProduct['mediumImage']
        else:
            mediumImage = None
        
        if 'largeImage' in currentProduct.keys():
            largeImage = currentProduct['largeImage']
        else:
            largeImage = None
        
        if 'productTrackingUrl' in currentProduct.keys():
            productTrackingUrl = currentProduct['productTrackingUrl']
        else:
            productTrackingUrl = None
        
        if 'ninetySevenCentShipping' in currentProduct.keys():
            ninetySevenCentShipping = currentProduct['ninetySevenCentShipping']
        else:
            ninetySevenCentShipping = None
        
        if 'standardShipRate' in currentProduct.keys():
            standardShipRate = currentProduct['standardShipRate']
        else:
            standardShipRate = None
        
        if 'size' in currentProduct.keys():
            size = currentProduct['size']
        else:
            size = None
        
        if 'marketplace' in currentProduct.keys():
            marketplace = currentProduct['marketplace']
        else:
            marketplace = None
        
        if 'shipToStore' in currentProduct.keys():
            shipToStore = currentProduct['shipToStore']
        else:
            shipToStore = None
        
        if 'freeShipToStore' in currentProduct.keys():
            freeShipToStore = currentProduct['freeShipToStore']
        else:
            freeShipToStore = None
        
        if 'modelNumber' in currentProduct.keys():
            modelNumber = currentProduct['modelNumber']
        else:
            modelNumber = None
        
        if 'productUrl' in currentProduct.keys():
            productUrl = currentProduct['productUrl']
        else:
            productUrl = None
        
        if 'customerRating' in currentProduct.keys():
            customerRating = currentProduct['customerRating']
        else:
            customerRating = None
        
        if 'numReviews' in currentProduct.keys():
            numReviews = currentProduct['numReviews']
        else:
            numReviews = None
        
        if 'customerRatingImage' in currentProduct.keys():
            customerRatingImage = currentProduct['customerRatingImage']
        else:
            customerRatingImage = None
        
        if 'categoryNode' in currentProduct.keys():
            categoryNode = currentProduct['categoryNode']
        else:
            categoryNode = None
        
        if 'rhid' in currentProduct.keys():
            rhid = currentProduct['rhid']
        else:
            rhid = None
        
        if 'bundle' in currentProduct.keys():
            bundle = currentProduct['bundle']
        else:
            bundle = None
        
        if 'clearance' in currentProduct.keys():
            clearance = currentProduct['clearance']
        else:
            clearance = None
        
        if 'preOrder' in currentProduct.keys():
            preOrder = currentProduct['preOrder']
        else:
            preOrder = None
        
        if 'stock' in currentProduct.keys():
            stock = currentProduct['stock']
        else:
            stock = None
        
        if 'attributes' in currentProduct.keys():
            attributes = currentProduct['attributes']
        else:
            attributes = None
        
        if 'addToCartUrl' in currentProduct.keys():
            addToCartUrl = currentProduct['addToCartUrl']
        else:
            addToCartUrl = None
        
        if 'affiliateAddToCartUrl' in currentProduct.keys():
            affiliateAddToCartUrl = currentProduct['affiliateAddToCartUrl']
        else:
            affiliateAddToCartUrl = None
        
        if 'freeShippingOver35Dollars' in currentProduct.keys():
            freeShippingOver35Dollars = currentProduct['freeShippingOver35Dollars']
        else:
            freeShippingOver35Dollars = None
        
        if 'giftOptions' in currentProduct.keys():
            giftOptions = currentProduct['giftOptions']
        else:
            giftOptions = None
        
        if 'imageEntities' in currentProduct.keys():
            imageEntities = currentProduct['imageEntities']
        else:
            imageEntities = None
        
        if 'offerType' in currentProduct.keys():
            offerType = currentProduct['offerType']
        else:
            offerType = None
        
        if 'isTwoDayShippingEligible' in currentProduct.keys():
            isTwoDayShippingEligible = currentProduct['isTwoDayShippingEligible']
        else:
            isTwoDayShippingEligible = None
        
        if 'availableOnline' in currentProduct.keys():
            availableOnline = currentProduct['availableOnline']
        else:
            availableOnline = None
        if 'gender' in currentProduct.keys():
            gender = currentProduct['gender']
        else:
            gender = None
        newProduct = [name, rank, totalProducts, timeStamp, itemId, parentItemId, msrp, salePrice, upc, shortDescription, brandName, thumbnailImage, mediumImage, largeImage, productTrackingUrl, ninetySevenCentShipping, standardShipRate, size, marketplace, shipToStore, freeShipToStore, modelNumber, productUrl, customerRating, numReviews, customerRatingImage, categoryNode, rhid, bundle, clearance, preOrder, stock, attributes, addToCartUrl, freeShippingOver35Dollars, giftOptions, imageEntities, offerType, isTwoDayShippingEligible, availableOnline, gender]
        productList.append(newProduct)
    print("length of product list: " + str(len(productList)))
    sendtoSQL(productList)    
        
