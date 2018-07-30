#!flask/bin/python
from pyArango.connection import *
from flask import Flask
import sys
from flask import jsonify 
from flask import Flask, abort, request 
app = Flask(__name__)
@app.route('/')
def index():
    return "This is a sample rest service"
#### Rest Service definition for returning the top 5 most expensive items in terms of their sales price from ecommercedb
@app.route('/api/v1.0/get5expensive', methods=['GET'])
def get5expensive():
	global query_Result
	try:
		conn = Connection(username="root", password="69366251")
		db = conn["ecommercedb"]
		aql = "FOR o IN OrderHeaders FOR a IN Addresses FILTER o.Address_AddressID == a.AddressID COLLECT ZipCode = a.Zip WITH COUNT INTO numOrders SORT numOrders DESC LIMIT 0, 5 RETURN {ZipCode,numOrders}"
		query_Result = db.AQLQuery(aql, rawResults=True, batchSize=100)
		#cursor = connection.cursor()
		#cursor.execute("SELECT min(saleprice) as minprice, Item_ItemNumber FROM itemprice order by minprice asc limit 1;")
		#query_Result = [ dict(line) for line in [zip([ column[0] for column in query_Result.description], row) for row in query_Result.fetchall()] ]
	except Exception, e:
		print "Error [%r]" % (e)
		sys.exit(1)
	finally:
		if db:
			db.connection.resetSession()
	finalresult=str(query_Result)
	#return str(query_Result)
	return jsonify({'The5MostExpensive': finalresult})
	
## Get the least expensive item in terms of its sales prices from the item price table
@app.route('/api/v1.0/getLeastexpensive', methods=['GET'])
def getLeastExpensive():
	global query_Result
	try:
		conn = Connection(username="root", password="69366251")
		db = conn["ecommercedb"]
		aql = "FOR i IN ItemPrices COLLECT SalePrice = i.SalePrice, ItemNumber = i.Item_ItemNumber INTO ItemsBySalePrice SORT SalePrice ASC LIMIT 1 RETURN {SalePrice,ItemNumber}"
		query_Result = db.AQLQuery(aql, rawResults=True, batchSize=100)
	except Exception, e:
		print "Error [%r]" % (e)
		sys.exit(1)
	finally:
		if db:
			db.connection.resetSession()
	finalresult=str(query_Result)
	#return str(query_Result)
	return jsonify({'TheLeastExpensive': finalresult})
## Aggregate the number of orders placed by the cities that the customers belong to.
@app.route('/api/v1.0/getOrderCountByCity', methods=['GET'])
def getOrderCountByCity():
	global query_Result
	try:
		conn = Connection(username="root", password="69366251")
		db = conn["ecommercedb"]
		aql = "  FOR o IN OrderHeaders FOR a IN Addresses FILTER o.Address_AddressID == a.AddressID COLLECT City = a.City WITH COUNT INTO numOrders RETURN {City,numOrders}"
		query_Result = db.AQLQuery(aql, rawResults=True, batchSize=100)
	except Exception, e:
		print "Error [%r]" % (e)
		sys.exit(1)
	finally:
		if db:
			db.connection.resetSession()
	finalresult=str(query_Result)
	#return str(query_Result)
	return jsonify({'TheOrderCountByCity': finalresult})
## Get only those customers that have placed 10 or more orders
@app.route('/api/v1.0/getCustomersGreaterThan10', methods=['GET'])
def getCustomersGreaterThan10():
	global query_Result
	try:
		conn = Connection(username="root", password="69366251")
		db = conn["ecommercedb"]
		aql = "FOR o IN OrderHeaders FOR c IN Customers FILTER o.CustomerTable_CustomerID == c.CustomerID COLLECT CustomerName = c.CustomerName WITH COUNT INTO numOrders FILTER numOrders >=10 SORT numOrders DESC RETURN {CustomerName,numOrders}"
		query_Result = db.AQLQuery(aql, rawResults=True, batchSize=100)
	except Exception, e:
		print "Error [%r]" % (e)
		sys.exit(1)
	finally:
		if db:
			db.connection.resetSession()
	finalresult=str(query_Result)
	#return str(query_Result)
	return jsonify({'TheCustomersGreaterThan10': finalresult})
##List the top 5 zipcodes with the most number of orders being shipped to. ( Use the Address referenced on the OrderHeader table)
@app.route('/api/v1.0/getTop5ZipCodes', methods=['GET'])
def getTop5ZipCodes():
	global query_Result
	try:
		conn = Connection(username="root", password="69366251")
		db = conn["ecommercedb"]
		aql = "FOR o IN OrderHeaders FOR a IN Addresses FILTER o.Address_AddressID == a.AddressID COLLECT ZipCode = a.Zip WITH COUNT INTO numOrders SORT numOrders DESC LIMIT 0, 5 RETURN {ZipCode,numOrders}"
		query_Result = db.AQLQuery(aql, rawResults=True, batchSize=100)
	except Exception, e:
		print "Error [%r]" % (e)
		sys.exit(1)
	finally:
		if db:
			db.connection.resetSession()
	finalresult=str(query_Result)
	#return str(query_Result)
	return jsonify({'TheTop5ZipCodes': finalresult})

## Starts the server for serving Rest Services 
if __name__ == '__main__':
    app.run(debug=True)