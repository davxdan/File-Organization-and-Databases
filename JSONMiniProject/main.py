#!flask/bin/python
from pyArango.connection import *
from flask import Flask
import sys
from flask import jsonify 
from flask import Flask, abort, request 



app = Flask(__name__)

## Get the least expensive item in terms of its sales prices from the item price table

@app.route('/api/v1.0/getLeastExpensive', methods=['GET'])
def getLeastExpensive():

	global query_result
	try:
		conn = Connection(username="root", password="69366251")
		db = conn["ecommercedb"]
		aql = "FOR o IN OrderHeaders FOR a IN Addresses FILTER o.Address_AddressID == a.AddressID COLLECT ZipCode = a.Zip WITH COUNT INTO numOrders SORT numOrders DESC LIMIT 0, 5 RETURN {ZipCode,numOrders}"
		query_Result = db.AQLQuery(aql, rawResults=True, batchSize=100)
		#cursor = connection.cursor()
		#cursor.execute("SELECT min(saleprice) as minprice, Item_ItemNumber FROM itemprice order by minprice asc limit 1;")
		#query_result = [ dict(line) for line in [zip([ column[0] for column in cursor.description], row) for row in cursor.fetchall()] ]
	except Exception, e:
		print "Error [%r]" % (e)
		sys.exit(1)
	finally:
		if db:
			db.close()
	
	return jsonify({'TheLeastExpensive': query_result})

## Aggregate the number of orders placed by the cities that the customers belong to.
@app.route('/api/v1.0/getAggregateByCity', methods=['GET'])
def getgetAggregateByCity():

	global query_result
	try:
		connection = pymysql.connect(host='localhost', user='root', password='69366251',db='ecommercedb',charset='utf8mb4')
		cursor = connection.cursor()
		cursor.execute("select count(ordernumber), address.City from orderheader inner join address on orderheader.Address_AddressID = address.AddressID group by address.city;")
		query_result = [ dict(line) for line in [zip([ column[0] for column in cursor.description], row) for row in cursor.fetchall()] ]
	except Exception, e:
		print "Error [%r]" % (e)
		sys.exit(1)
	finally:
		if cursor:
			cursor.close()
	return jsonify({'ThegetAggregatesByCity': query_result})

## Get only those customers that have placed 10 or more orders
@app.route('/api/v1.0/get10OrMore', methods=['GET'])
def get10OrMore():

	global query_result
	try:
		connection = pymysql.connect(host='localhost', user='root', password='69366251',db='ecommercedb',charset='utf8mb4')
		cursor = connection.cursor()
		cursor.execute("select customertable.CustomerName from customertable inner join orderheader on customertable.CustomerID = orderheader.CustomerTable_CustomerID having count(orderheader.CustomerTable_CustomerID)>9;")
		query_result = [ dict(line) for line in [zip([ column[0] for column in cursor.description], row) for row in cursor.fetchall()] ]
	except Exception, e:
		print "Error [%r]" % (e)
		sys.exit(1)
	finally:
		if cursor:
			cursor.close()
	return jsonify({'Customers10OrMore': query_result})

#List the top 5 zipcodes with the most number of orders being shipped to. ( Use the Address referenced on the OrderHeader table)
@app.route('/api/v1.0/getTop5Zip', methods=['GET'])
def getTop5Zip():

	global query_result
	try:
		connection = pymysql.connect(host='localhost', user='root', password='69366251',db='ecommercedb',charset='utf8mb4')
		cursor = connection.cursor()
		cursor.execute("select address.Zip from orderheader inner join address on orderheader.Address_AddressID = address.AddressID group by address.zip order by count(orderheader.ordernumber) desc limit 5;")
		query_result = [ dict(line) for line in [zip([ column[0] for column in cursor.description], row) for row in cursor.fetchall()] ]
	except Exception, e:
		print "Error [%r]" % (e)
		sys.exit(1)
	finally:
		if cursor:
			cursor.close()
	return jsonify({'TheTop5Zipcoes': query_result})

#Update Item1 Price
##I almost had this bonus question but got stuck and ran out of time.
#@app.route('/api/v1.0/setItemPrices', methods=['POST','GET'])
#def setItemPrices():

#	global query_result
#	try:
		# Create connection to the MYSQL instance running on the local machine
		## Change the credentials to match your system
#		connection = pymysql.connect(host='localhost', user='root', password='69366251',db='sakila',charset='utf8mb4')
#		cursor = connection.cursor()
		
		## Read the input json from the incoming request
		#reqObj = request.get_json 
#		data = request.get_json() 
		#queryStr = "select * from actor where actor_id=%s;"%data['id']
#		queryStr =  "update`itemprice` set `saleprice` = '300'  where `item_itemnumber` = %s;"%data['id']
#		print(queryStr)
#		cursor.execute(queryStr)
#		query_result = [ dict(line) for line in [zip([ column[0] for column in cursor.description], row) for row in cursor.fetchall()] ]
#	except Exception, e:
#		print "Error [%r]" % (e)
#		sys.exit(1)
#	finally:
#		if cursor:
#			cursor.close()
#			
#	return jsonify({'SetItemPrice': query_result})

## Starts the server for serving Rest Services 
if __name__ == '__main__':
    app.run(debug=True)

	