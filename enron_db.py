#!/usr/bin/env python
import pymongo
from pymongo import MongoClient
from pprint import pprint

#The original database can be found @ https://www.dropbox.com/s/amfp7o1dnpz9jsv/dump.zip
client = MongoClient()
db = client.enron

messages = db.messages
authors = messages.distinct('headers.From')
print 'There were {} messages written at enron.'.format(messages.count()), '\n'
print 'There were {} message authors at enron.'.format(len(messages.distinct('headers.From'))), '\n'

def get_count(from_u = 'andrew.fastow@enron.com', to = 'jeff.skilling@enron.com'):
	#This function counts how many messages there were between any two users at enron in the messages collection
	count = messages.find({'headers.From' : from_u, 'headers.To' : to}).count()
	#count = messages.find({'headers.From' : from_u, 'headers.To' : {'$elemMatch' : to}}).count()
	print "The number of messages sent from {} to {} was {}.".format(from_u, to, count), '\n' 

def send_count(lim = len(authors)):
	#This function finds and orders how many messages were sent between two users at enron in the messages collection
	counted = messages.aggregate([
		{'$project':{'headers.From':1, 'headers.To':1}},
		{'$unwind':'$headers.To'},
		{'$group': {'_id': {'from': '$headers.From', 'to' : '$headers.To'},'total' : {'$sum' : 1}}},
		{'$sort' : {'total' : -1}},{'$limit':lim}
		])
	print 'The top {} from/to messages pairs are...'.format(lim)
	pprint(counted)
	return counted

if __name__ == '__main__':
	get_count()
	send_count(3)