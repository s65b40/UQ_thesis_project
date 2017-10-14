# -*- coding: utf-8 -*-
#author:Haochun Wang

import time
import sys
from elasticsearch import Elasticsearch



# set up the connection with elasticsearch (this is based on the service)
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


class Elas_test:
    def __init__(self):
        pass

    def count(self):
        num_info = es.count(index="nprobe-2017.07.01")
        num_index = num_info[u'count']
        print num_index

    def read(self):
        res = es.search(index="nprobe-2017.07.01", size=1)
        print res


if __name__ == "__main__":
    start = time.clock()

    end = time.clock()
    print "Done! With a time comsuming of %0.6f seconds" % (end - start)

#res = es.search(index="nprobe-2017.07.01", body={"query": {"match_all": {}}})
# res = es.search(index="nprobe-2017.07.01", size=100, filter_path=['hits.hits._id', 'hits.hits._type'])

#res = es.search(index="nprobe-2017.07.01", size=1, filter_path=['hits.hits._source.IN_BYTES'], request_timeout=30)

'''
# This block is to get the size of each component in the dictionary
for i in res['hits']['hits'][0][u'_source']:
    tp = res['hits']['hits'][0][u'_source'][i]
    print i, tp, type(tp), sys.getsizeof(tp)
print type(res['hits']['hits'][0][u'_source'][u'IN_BYTES'])
'''
#filter_path=['hits.hits._id', 'hits.hits._type']
#read loop pieces  csvfile java  query-field 1-2 inbyte measurement
#semi


# print(res)
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
'''
# res = es.search(index="nprobe-2017.07.01", body={'query': {'match': {'any': 'data'}}})  # 获取any=data的所有值
#print(res)

