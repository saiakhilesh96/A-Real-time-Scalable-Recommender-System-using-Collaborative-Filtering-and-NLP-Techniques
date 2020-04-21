from kafka import KafkaConsumer
import os
import json
from io import StringIO

count=1
count=int(count)

consumer_customer = KafkaConsumer('Amazon_product_reviews.',bootstrap_servers=['localhost:9092'])
#consumer_merchant = KafkaConsumer('merchant-transactions',bootstrap_servers=['localhost:9092'])

#read the rules from the file and implement them.


print ("Review System is ON!")
print ("Waiting for the stream to come!")
#combined_streams=consumer_customer#,consumer_merchant)
for cus in consumer_customer:
	print((json.loads(cus.value)))
	cus_T = json.loads(cus.value)
	#mer_T = json.loads(mer.value)
#	if (cus_T['cus_txn_amt'] - mer_T['mer_txn_amt'] >  800):
	
	#print( "cust_id", cus_T['cust_id'])#,"mer_id", mer_T['mer_mer_id'], "cus_txn_amt ", cus_T['cus_txn_amt'])
	print("\n")