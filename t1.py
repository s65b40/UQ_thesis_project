# -*- coding: utf-8 -*-
# author:Haochun Wang
'''
question:
2017.07.01 delete? ok?
time difference same time but feel like different
'''
import time
import sys
import csv
from elasticsearch import Elasticsearch

# import matplotlib.pyplot as plt

reload(sys)
sys.setdefaultencoding('utf8')
# set up the connection with elasticsearch (this is based on the service)


class ElasTest:
    def __init__(self):
        self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

    def count(self):
        num_info = self.es.count(index="nprobe-2017.07.01")
        num_index = num_info[u'count']
        return num_index

    def read_test(self, flist, read_size=100):
        # print len(flist)
        res = self.es.search(index="nprobe-2017.07.01", size=read_size, filter_path=flist, request_timeout=60)

        # res = self.es.search(index="nprobe-2017.07.01", size=read_size, filter_path=['hits.hits._source.IN_BYTES'], request_timeout=30)
        # res = self.es.search(index="nprobe-2017.07.01", size=read_size, filter_path=['hits.hits._source.IN_BYTES', 'hits.hits._source.@timestamp'], request_timeout=30)
        # print res
        return

    def read_write(self, flist, read_size=100):
        start_read = time.time()

        filterlst = []
        for k in flist:
            filterlst.append('hits.hits._source.' + k)
        res = self.es.search(index="nprobe-2017.07.01", size=read_size, filter_path=filterlst, request_timeout=30)
        end_read = time.time()
        print "For %d and " % read_size, " %d columns" % len(flist), '\t\t', \
            "Read of %0.6f seconds" % (end_read - start_read)

        start_write = time.time()
        with open('a/some%d_%d.csv' % (read_size, len(flist)), 'wb') as f:
            writer = csv.writer(f)
            writer.writerow(flist)
            # print len(res['hits']['hits'])
            for l in res['hits']['hits']:
                tp = []
                for j in flist:
                    tp.append(l['_source'][j])
                # print tp
                writer.writerow(tp)
        end_write = time.time()

        print "For %d and " % read_size, " %d columns" % len(flist), '\t\t', \
            "Write of %0.6f seconds" % (end_write - start_write)
        return

    def r_w_pieces(self, flist, read_size=0, block=1000):
        start = time.time()
        if read_size == 0:  # get the length of index
            read_size = self.count()

        filterlst = []  # build the filter_path
        for k in flist:
            filterlst.append('hits.hits._source.' + k)

        l_times = 0
        if read_size % block == 0:
            l_times = read_size / block
        elif read_size % block > 0:
            l_times = read_size / block + 1
        # print l_times
        flag = 0
        for i in xrange(0, l_times):
            res = self.es.search(index="nprobe-2017.07.01", size=block, filter_path=filterlst, request_timeout=30,
                                 body={"query": {"match_all": {}}, "from": flag, "size": block})
            flag += block
        '''
        with open('a/res_%d_%d_%d.csv' % (len(flist), block, read_size), 'a+') as f:
            writer = csv.writer(f)
            writer.writerow(flist)
        for i in xrange(0, l_times):

            res = self.es.search(index="nprobe-2017.07.01", size=block, filter_path=filterlst, request_timeout=30,
                                 body={"query": {"match_all": {}}, "from": flag, "size": block})
            # print res
            # body={"query": { "match_all": {} },"from": 10,"size": 10}
            with open('a/res_%d_%d_%d.csv' % (len(flist), block, read_size), 'a+') as f:
                writer = csv.writer(f)
                for l in res['hits']['hits']:
                    tp = []
                    for j in flist:
                        tp.append(l['_source'][j])
                    writer.writerow(tp)
            flag += block
        '''
        end = time.time()
        print "For %d and " % read_size, "%d columns" % len(flist), "%d as a block" % block, '\t\t', \
            "Reading & Writing of %0.6f seconds" % (end - start)
        return

    def draw_line_chart(self):
        read_x = range(1, 30)
        write_x = range(1, 30)
        read_y = []
        write_y = []
        i = 0
        with open('u', 'rb') as f:
            lines = f.readlines()
            print len(lines)
            for line in lines:
                print line
                res = line.split(", ")
                # print type(int(res[0].split(" ")[1]))
                if int(res[0].split(" ")[1]) == 100000:
                    if i % 2 == 0:
                        read_y.append(float(res[2].split(" ")[2]))
                        i += 1
                    else:
                        write_y.append(float(res[2].split(" ")[2]))
                        i += 1
        plt.plot(read_x, read_y, label='Reading time', linewidth=3, color='r', markerfacecolor='blue', markersize=12)
        plt.plot(write_x, write_y, label='Writing time')
        plt.xlabel('Columns')
        plt.ylabel('Time(seconds)')
        plt.title('R/W time test for 100000')
        plt.legend()
        plt.show()
        return


if __name__ == "__main__":
    col_list = [u'L7_PROTO', u'ENGINE_ID', u'@timestamp', u'IPV4_DST_ADDR', u'UPSTREAM_TUNNEL_ID',
                u'NPROBE_IPV4_ADDRESS', u'L4_DST_PORT', u'UNTUNNELED_IPV4_SRC_ADDR', u'DOWNSTREAM_TUNNEL_ID',
                u'FIRST_SWITCHED', u'LAST_SWITCHED', u'UPSTREAM_SESSION_ID', u'SRC_VLAN', u'FLOW_ID',
                u'PROTOCOL_MAP', u'PROTOCOL', u'OUT_BYTES', u'L4_SRC_PORT', u'IN_PKTS', u'IN_BYTES', u'SRC_TOS',
                u'APPLICATION_ID', u'FLOW_PROTO_PORT', u'IPV4_SRC_ADDR', u'OUT_PKTS', u'UNTUNNELED_PROTOCOL',
                u'DOWNSTREAM_SESSION_ID', u'@version', u'L7_PROTO_NAME']
    es = ElasTest()
    es.r_w_pieces(col_list[:2], read_size=0, block=500000)
    # es.r_w_pieces(col_list[:2], read_size=100000, block=10000)
    '''
    for i in [100000, 250000, 500000, 600000, 700000, 800000, 900000, 1000000]:
        for j in [1, 2, 5, 10, 15]:
            tp_list = col_list[:j]
            es.r_w_pieces(tp_list, read_size=0, block=i)
    '''
    '''
    # This block is to test reading and writing time for different scale of pieces
    for k in [5000000, 10000000]:
        for i in [100000, 500000]:
            for j in [1, 2, 5, 10, 15]:
                tp_list = col_list[:j]
                es.r_w_pieces(tp_list, read_size=k, block=i)
    '''
    # es.draw_line_chart()
    # es.read_write(flist=['IN_BYTES', '@timestamp'], read_size=10)
    ###################################################################################################################
    ###################################################################################################################
    '''
    # This block is to test reading and writing time for different scales of rows and columns.
    for i in [1, 10, 100, 1000, 10000, 100000, 1000000]:
        for j in xrange(1, len(col_list) + 1):
            tp_list = col_list[:j]
            es.read_write(flist=tp_list, read_size=i)
    '''
    ###################################################################################################################
    ###################################################################################################################
    '''
    # This block to test reading time for different scales of rows and columns.
    for i in [1, 10, 100, 1000, 10000, 100000, 1000000]:
        for j in xrange(3, len(col_list) + 1):
            tp_list = col_list[:j]
            filterlst = []
            for k in tp_list:
                filterlst.append('hits.hits._source.' + k)
            #print filterlst
            start = time.clock()
            es = Elas_test()
            es.read_test(filterlst, read_size=i)
            end = time.clock()
            print "For %d and " % i, " %d columns" % j, '\t\t', "Done! With a time consumption of %0.6f seconds" % (end - start)
    '''
    ###################################################################################################################
    ###################################################################################################################
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
#read loop pieces 1000 csvfile java  query-field 1-2 inbyte measurement
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

