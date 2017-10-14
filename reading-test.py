# -*- coding: utf-8 -*-
#author:Haochun Wang

import time
from elasticsearch import Elasticsearch

start = time.clock()

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

num_info = es.count(index="nprobe-2017.07.01")
num_index = num_info[u'count']
print num_index

# search获取
#res = es.search(index="nprobe-2017.07.01", body={"query": {"match_all": {}}})
res = es.search(index="nprobe-2017.07.01", size=100)#
#read loop pieces  csvfile java  query-field 1-2 inbyte measurement
#semi

end = time.clock()
print(res)
# {u'hits':
#    {
#    u'hits': [
#        {u'_score': 1.0, u'_type': u'test-type', u'_id': u'2', u'_source': {u'timestamp': u'2016-01-20T10:53:58.562000', u'any': u'data02'}, u'_index': u'my-index'},
#        {u'_score': 1.0, u'_type': u'test-type', u'_id': u'1', u'_source': {u'timestamp': u'2016-01-20T10:53:36.997000', u'any': u'data01'}, u'_index': u'my-index'},
#        {u'_score': 1.0, u'_type': u'test-type', u'_id': u'3', u'_source': {u'timestamp': u'2016-01-20T11:09:19.403000', u'any': u'data033'}, u'_index': u'my-index'}
#    ],
#    u'total': 5,
#    u'max_score': 1.0
#    },
# u'_shards': {u'successful': 5, u'failed': 0, u'total':5},
# u'took': 1,
# u'timed_out': False
# }
'''
for hit in res['hits']['hits']:
    print(hit["_source"])
res = es.search(index="nprobe-2017.07.01", body={'query': {'match': {'any': 'data'}}})  # 获取any=data的所有值
print(res)
'''
print "Done! With a time comsuming of %0.2f seconds" % (end - start)