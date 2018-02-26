# -*- coding: utf-8 -*-
# author:Haochun Wang

import time
import sys
import csv
from elasticsearch import Elasticsearch

reload(sys)
sys.setdefaultencoding('utf8')


# set up the connection with elasticsearch (this is based on the service)


class ElasTest:
    def __init__(self, index_name, ip_addr, port):
        self.index = index_name
        self.ip = ip_addr
        self.port = port
        self.es = Elasticsearch([{'host': self.ip, 'port': self.port}])

    def basic(self, num_size):
        '''
        The following basic function is based on index structure itself, which means it is NOT reusable for all indices.
        :param num_size: expected numbers of items
        :return: null
        '''
        res = self.es.search(index=self.index, size=num_size)
        for i in xrange(num_size):
            for j in res['hits']['hits'][i]['_source']:
                print str(res['hits']['hits'][i]['_source'][j])

        print list(res['hits']['hits'][0]['_source'])
        '''
        with open('0.txt', 'w') as a:
            a.write(str(res))
        '''
        return

    def count(self):
        '''
        This is to count how many items in an index
        :return:
        '''
        num_info = self.es.count(index=self.index)
        num_index = num_info[u'count']
        return num_index


if __name__ == "__main__":
    # three parameters: 1 index name 2 ip address 3 port
    es = ElasTest('document', '118.190.201.165', '9300')
    # number of rows in index
    es.basic(5)
