# -*- coding: utf-8 -*-
# author:Haochun Wang

import csv
import sys
import time
import random
from numpy import *
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import matplotlib.pyplot as plt
# /   604 bilk write
# # meaning graaphs max speed reading
# xavg bandwidth example bits/sec

# kibana
# symmetric
# timstamp first switch last s
# histogram

reload(sys)
sys.setdefaultencoding('utf8')


# set up the connection with elasticsearch (based on the service)


class ElasTest:
    def __init__(self):
        # Start an Elasticsearch service.
        # Default local server: localhost or 127.0.0.1, and default port:9200
        self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

    def te(self):
        res = self.es.search(index="nprobe-2017.07.01", size=5)
        print res
        for i in res['hits']['hits'][1]['_source']:
            print str(res['hits']['hits'][1]['_source'][i])
            # print i
        print list(res['hits']['hits'][0]['_source'])
        # print res
        with open('3.txt', 'wb') as a:
            a.write(str(res))

        # print res
        return

    def count(self, index_name):
        # Get the number of the index
        num_info = self.es.count(index=index_name)
        num_index = num_info[u'count']
        return num_index

    def create_index(self, index_name):
        self.es.indices.create(index=index_name, ignore=400)

    def delete_index(self, index_name):
        self.es.indices.delete(index=index_name, ignore=[400, 404])

    def read_test(self, flist, read_size=100):
        '''
            Reading test for Elasticsearch
            :param flist:       The chosen fields list from the index
            :param read_size:   The number of items in the index that will be read
            :return:            Null
        '''
        # param: 1. index name; 2. reading size;; 3. filter list; 4. timeout
        filterlst = []
        for k in flist:
            filterlst.append('hits.hits._source.' + k)
        res = self.es.search(index="nprobe-2017.07.01", size=read_size, filter_path=filterlst, request_timeout=60)
        print res
        return

    def read_write(self, flist, read_size=100):
        '''
            Reading and writing to CSV files test for Elasticsearch.
            ###########################################################################################
            Output file format: 1. Reading size 2. The numbers of columns 3. Reading time 4. Writing time
            :param flist:       The chosen fields list from the index
            :param read_size:   The number of items in the index that will be read
            :return:            Null
        '''
        start_read = time.time()  # reading timer starts
        filterlst = []
        for k in flist:
            filterlst.append('hits.hits._source.' + k)
        res = self.es.search(index="nprobe-2017.07.01", size=read_size, filter_path=filterlst, request_timeout=60)
        end_read = time.time()  # reading timer stops
        # print "For %d and " % read_size, " %d columns" % len(flist), '\t\t', "Read of %0.6f seconds" % (end_read - start_read)

        start_write = time.time()  # writing timer starts
        with open('tmp_storage/r_w_csv%d_%d.csv' % (read_size, len(flist)), 'w') as f:
            writer = csv.writer(f)
            writer.writerow(flist)
            # print len(res['hits']['hits'])
            for l in res['hits']['hits']:
                tp = []
                for j in flist:
                    tp.append(l['_source'][j])
                writer.writerow(tp)
        end_write = time.time()  # writing timer stops

        with open('time/r_w_csvtime.csv', 'a+') as csvres:
            writer = csv.writer(csvres)
            line_out = [read_size, len(flist), "%0.6f" % (end_read - start_read), end_write - start_write]
            writer.writerow(line_out)

        # print "For %d and " % read_size, " %d columns" % len(flist), '\t\t', \"Write of %0.6f seconds" % (end_write - start_write)
        return

    def r_w_pieces(self, flist, read_size=0, block=1000):

        '''
            Reading in pieces and writing to CSV files test for Elasticsearch.
            ######################################################################################333333
            Output file format: 1. Reading size 2. The numbers of columns 3. Reading time 4. Writing time 5. Block size
            :param flist:       The chosen fields list from the index
            :param read_size:   The number of items in the index that will be read
            :param block:       The number of items in the index that will be read in one time
            :return:            Null
        '''
        start = time.time()
        if read_size == 0:  # get the length of index
            read_size = self.count()

        filterlst = []  # build the filter_path
        for k in flist:
            filterlst.append('hits.hits._source.' + k)

        l_times = 0  # count the number of loop
        if read_size % block == 0:
            l_times = read_size / block
        elif read_size % block > 0:
            l_times = read_size / block + 1

        # print l_times

        flag = 0

        for i in xrange(0, l_times):
            res = self.es.search(index="nprobe-2017.07.01", size=block, filter_path=filterlst, request_timeout=30,
                                 body={"query": {"match_all": {}}, "from": flag, "size": block})
            with open('tmp_storage/res_%d_%d_%d.csv' % (len(flist), block, read_size), 'w') as f:
                writer = csv.writer(f)
                for l in res['hits']['hits']:
                    tp = []
                    for j in flist:
                        tp.append(l['_source'][j])
                    writer.writerow(tp)
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
        with open('time/r_w_pieces_%d_%d_%d.csv' % (read_size, len(flist), block), 'a+') as f:
            writer = csv.writer(f)
            line_out = [read_size, len(flist), "%0.6f" % (end - start), block]
            writer.writerow(line_out)
        # print "For %d and " % read_size, "%d columns" % len(flist), "%d as a block" % block, '\t\t', "Reading & Writing of %0.6f seconds" % (end - start)
        return

    def r_pieces(self, flist, read_size=0, block=1000):
        '''
            Reading in pieces test for Elasticsearch.
            #############################################################################################
            Output file format: 1. Reading size 2. The numbers of columns 3. Reading time 4. Writing time 5. Block size
            :param flist:       The chosen fields list from the index
            :param read_size:   The number of items in the index that will be read
            :param block:       The number of items in the index that will be read in one time
            :return:            Null
        '''
        start = time.time()
        if read_size == 0:  # get the length of index
            read_size = self.count()

        filterlst = []  # build the filter_path
        for k in flist:
            filterlst.append('hits.hits._source.' + k)

        l_times = 0  # count the number of loop
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

        end = time.time()
        with open('time_three_core/r_pieces_%d_%d_%d.csv' % (read_size, len(flist), block), 'a+') as f:
            writer = csv.writer(f)
            line_out = [read_size, len(flist), "%0.6f" % (end - start), block]
            writer.writerow(line_out)
        return

    def r_w_index(self, flist, read_size, w_times):
        '''
            Reading and writing test for elasticsearch index for industrial usage.
            Read a certain size of a block into memory and write the block back to a new elasticsearch index in a loop.
            ######################################################################################
            Output file format: 1. Reading size 2. The numbers of columns 3. Reading time
                                4. Writing times 5. Writing time
            :param flist:       The chosen fields list from the index
            :param read_size:   The number of items in the index that will be read
            :param w_times:     Times of writing loop
            :return:            Null
        '''
        # es.r_w_index(col_list, read_size=10, w_times=1)
        # read
        r_start = time.time()

        filterlst = []  # build the filter_path
        for k in flist:
            filterlst.append('hits.hits._source.' + k)

        # read into memory
        res = self.es.search(index="nprobe-2017.07.01", size=read_size)
        print res
        r_finish = time.time()

        r_time = r_finish - r_start

        # write
        w_flag = 0

        w_start = time.time()

        _source = []
        for i in range(read_size):
            # _id = res[u'hits'][u'hits'][i][u'_id']
            _source.append(res[u'hits'][u'hits'][i][u'_source'])

        for j in range(w_times):
            for i in range(read_size):
                self.es.index(index="writeback-index", doc_type="flows", id=w_flag, body=_source[i])
                w_flag += 1
        w_finish = time.time()
        w_time = w_finish - w_start
        print w_flag
        num = self.es.count('writeback-index')[u'count']
        print num
        with open('w_index/r_w.csv', 'a+') as f:
            writer = csv.writer(f)
            line_out = [read_size, len(flist), "%0.6f" % r_time, "%0.6f" % w_time, w_times]
            writer.writerow(line_out)
        # '''
        return

    def r_w_index_bulk(self, flist, read_size, w_times):
        '''
            Reading and writing with bulk test for elasticsearch index for industrial usage.
            Read a certain size of a block into memory and write the block back to a new elasticsearch with bulk api
            index in a loop.
            ######################################################################################
            Output file format: 1. Reading size 2. The numbers of columns 3. Reading time
                                4. Writing times 5. Writing time
            :param flist:       The chosen fields list from the index
            :param read_size:   The number of items in the index that will be read
            :param w_times:     Times of writing loop
            :return:            Null
        '''

        # read
        r_start = time.time()

        # read into memory
        res = self.es.search(index="nprobe-2017.07.01", size=read_size)

        r_finish = time.time()

        r_time = r_finish - r_start

        # write

        data_list = []

        for i in range(read_size):
            data_list.append({"_index": "writeback-index", "_type": "flows"
                                 , "_source": res[u'hits'][u'hits'][i][u'_source']})

        w_start = time.time()

        for j in range(w_times):
            helpers.bulk(self.es, data_list, "writeback-index", raise_on_error=True)

        w_finish = time.time()

        w_time = w_finish - w_start

        num = self.es.count('writeback-index')[u'count']
        print num
        with open('w_index/r_w_bulk.csv', 'a+') as f:
            writer = csv.writer(f)
            line_out = [read_size, len(flist), "%0.6f" % r_time, "%0.6f" % w_time, w_times]
            writer.writerow(line_out)

        return

    def r_w_index_bulk_bandwidth(self, flist, read_size):
        '''
            Reading and writing with bulk test for elasticsearch index for industrial usage.
            Read a certain size of a block into memory and calculate the average bandwidth per second
            to a new elasticsearch index with bulk api.

            ######################################################################################
            Output file format: 1. Reading size 2. The numbers of columns 3. Reading time
                                4. Writing times 5. Writing time
            :param flist:       The chosen fields list from the index
            :param read_size:   The number of items in the index that will be read
            :return:            Null
        '''

        # read
        t_i_o_dic = {}  # {'time':[in_bytes, out_bytes, bytes]}
        # read into memory
        filterlst = []  # storage the filter list
        for k in flist:
            filterlst.append('hits.hits._source.' + k)
        res = self.es.search(index="nprobe-2017.07.01", filter_path=filterlst, size=read_size)
        # print res[u'hits'][u'hits'][2][u'_source']
        # print 'res', res
        for i in res[u'hits'][u'hits']:
            tmp_source = i[u'_source']
            timestamp = tmp_source[u'@timestamp']
            hour_min = timestamp[11:16]
            if t_i_o_dic.has_key(hour_min):
                t_i_o_dic[hour_min][0] += tmp_source[u'IN_BYTES'] * 8
                t_i_o_dic[hour_min][1] += tmp_source[u'OUT_BYTES'] * 8
            else:
                t_i_o_dic[hour_min] = [tmp_source[u'IN_BYTES'] * 8, tmp_source[u'OUT_BYTES'] * 8]

        # {u'IN_BYTES': 2090, u'@timestamp': u'2017-07-01T09:20:03.155Z', u'OUT_BYTES': 0}}

        # write

        data_list = []

        for i in t_i_o_dic:
            data_list.append(
                {"_index": "bandwidth",
                 "_type": "flows",
                 "_source": {u'hour_min': i,
                             u'IN_BIT_BD': t_i_o_dic[i][0],
                             u'OUT_BIT_BD': t_i_o_dic[i][1],
                             u'BIT_BD': t_i_o_dic[i][0] + t_i_o_dic[i][1]
                             }
                 })
        # print len(data_list)
        print len(data_list)
        helpers.bulk(self.es, data_list, "bandwidth", raise_on_error=True)

        num = self.es.count('bandwidth')[u'count']
        print num
        return

    # {u'hits': {u'hits': [{u'_source': {u'L7_PROTO': 91, u'ENGINE_ID': 1, u'@timestamp': u'2017-07-01T09:20:03.155Z'
    # , u'IPV4_DST_ADDR': u'223.29.241.82', u'UPSTREAM_TUNNEL_ID': 0, u'NPROBE_IPV4_ADDRESS': u'10.140.64.70'
    # , u'L4_DST_PORT': 63787, u'UNTUNNELED_IPV4_SRC_ADDR': u'40.100.144.242', u'DOWNSTREAM_TUNNEL_ID': 0
    # , u'FIRST_SWITCHED': 1498900803, u'LAST_SWITCHED': 1498900911, u'UPSTREAM_SESSION_ID': 0, u'SRC_VLAN': 52
    # , u'FLOW_ID': 92042368, u'PROTOCOL_MAP': u'tcp', u'PROTOCOL': 6, u'OUT_BYTES': 0, u'L4_SRC_PORT': 443
    # , u'IN_PKTS': 11, u'IN_BYTES': 2090, u'SRC_TOS': 0, u'APPLICATION_ID': u'0', u'FLOW_PROTO_PORT': 443
    # , u'IPV4_SRC_ADDR': u'40.100.144.242', u'OUT_PKTS': 0, u'UNTUNNELED_PROTOCOL': 6, u'DOWNSTREAM_SESSION_ID': 0
    # , u'@version': u'1', u'L7_PROTO_NAME': u'SSL'}}]}}

    def symmetry(self):
        '''
        The symmetry is defined as follow
        symmetry = in_packets/out_packets
        :param index_name:
        :return:
        '''
        l_times = 11804900 / 200000 + 1  # count the number of loop

        flag = 0
        symmax = 0
        symmin = 10000
        count = 0
        # symmax 104.0  symmin 0.027027027027
        # sym_dic = {'0': 0, 'in0': 0, '(0,0.5)': 0, '[0.5,0.6)': 0, '[0.6,0.7)': 0, '[0.7,0.8)': 0, '[0.8,0.9)': 0,
        #           '[0.9,1)': 0, '[1, 1.1)': 0, '[1.1, 1.2)': 0,'[1.2, 1.3)': 0, '[1.3, 1.6)': 0, '[1.6, 2)': 0
        #        , '[2, 3)': 0, '[3, 4)': 0, '[4, 10)': 0, '[10, 105)': 0}
        # sym_dic = {'[1,1.01)': 0, '[1.01,1.02)': 0, '[1.02,1.03)': 0, '[1.03,1.04)': 0, '[1.04,1.05)': 0, '[1.05,1.06)': 0, '[1.06,1.07)': 0, '[1.07,1.08)': 0, '[1.08,1.09)': 0, '[1.09,1.1)': 0}
        count_0 = 0
        with open('out_pkts_random10000.txt', 'a+') as tf:
            for i in xrange(0, l_times):
                res = self.es.search(index="nprobe-2017.07.01", request_timeout=60,
                                     body={"query": {"match_all": {}}, "from": flag, "size": 200000})
                for j in res[u'hits'][u'hits']:
                    count += 1
                    tmp_source = j[u'_source']
                    out_packets = tmp_source[u'OUT_PKTS']
                    in_packets = tmp_source[u'IN_PKTS']
                    if out_packets == 0:
                        random_num = random.random()
                        if random_num < 0.01:
                            count_0 += 1
                            tf.writelines(str(tmp_source) + '\n')
                            if count_0 == 10000:
                                exit(0)
                            continue
                    '''
                    else:
                        sym = (in_packets + 0.0) / out_packets
                        
                        if 0 < sym < 0.5:
                            sym_dic['(0,0.5)'] += 1
                        elif sym == 0:
                            sym_dic['in0'] += 1
                        elif 0.5 <= sym < 0.6:
                            sym_dic['[0.5,0.6)'] += 1
                        elif 0.6 <= sym < 0.7:
                            sym_dic['[0.6,0.7)'] += 1
                        elif 0.7 <= sym < 0.8:
                            sym_dic['[0.7,0.8)'] += 1
                        elif 0.8 <= sym < 0.9:
                            sym_dic['[0.8,0.9)'] += 1
                        elif 0.9 <= sym < 1:
                            sym_dic['[0.9,1)'] += 1
                        
                        if 1 <= sym < 1.01:
                            sym_dic['[1,1.01)'] += 1
                        elif 1.01 <= sym < 1.02:
                            sym_dic['[1.01,1.02)'] += 1
                        elif 1.02 <= sym < 1.03:
                            sym_dic['[1.02,1.03)'] += 1
                        elif 1.03 <= sym < 1.04:
                            sym_dic['[1.03,1.04)'] += 1
                        elif 1.04 <= sym < 1.05:
                            sym_dic['[1.04,1.05)'] += 1
                        elif 1.05 <= sym < 1.06:
                            sym_dic['[1.05,1.06)'] += 1
                        elif 1.06 <= sym < 1.07:
                            sym_dic['[1.06,1.07)'] += 1
                        elif 1.07 <= sym < 1.08:
                            sym_dic['[1.07,1.08)'] += 1
                        elif 1.08 <= sym < 1.09:
                            sym_dic['[1.08,1.09)'] += 1
                        elif 1.09 <= sym < 1.1:
                            sym_dic['[1.09,1.1)'] += 1
                        
                        elif 1.1 <= sym < 1.2:
                            sym_dic['[1.1, 1.2)'] += 1
                        elif 1.2 <= sym < 1.3:
                            sym_dic['[1.2, 1.3)'] += 1
                        elif 1.3 <= sym < 1.6:
                            sym_dic['[1.3, 1.6)'] += 1
                        elif 1.6 <= sym < 2:
                            sym_dic['[1.6, 2)'] += 1
                        elif 2 <= sym < 3:
                            sym_dic['[2, 3)'] += 1
                        elif 3 <= sym < 4:
                            sym_dic['[3, 4)'] += 1
                        elif 4 <= sym < 10:
                            sym_dic['[4, 10)'] += 1
                        else:
                            sym_dic['[10, 105)'] += 1
                    '''

                    # {'[4, 10)': 4680, '(0,0.5)': 26608, '[10, 105)': 742, '0': 3278571, '[0.5,1)': 660187, '[1, 2)': 6889607, '[2, 4)': 944544}

            flag += 200000

        # print sym_dic
        # print 'count', count
        return

    def symmetry2(self, block, size):
        l_times = size / block + 1
        # l_times = 11804900 / block + 1  # count the number of loop
        flag = 0
        # symmax 104.0  symmin 0.027027027027
        x = []
        y = []
        for i in xrange(0, l_times):
            res = self.es.search(index="nprobe-2017.07.01", request_timeout=60,
                                 body={"query": {"match_all": {}}, "from": flag, "size": block})
            for j in res[u'hits'][u'hits']:
                tmp_source = j[u'_source']
                out_packets = tmp_source[u'OUT_PKTS']
                in_packets = tmp_source[u'IN_PKTS']
                flow_size = out_packets + in_packets
                sym = 0
                if out_packets == 0:
                    pass
                    # sym = (in_packets + 0.0) / (out_packets + 1)
                else:
                    sym = (in_packets + 0.0) / out_packets
                    if sym > 3:
                        pass
                    else:
                        x.append(flow_size)
                        y.append(sym)

            flag += block
        self.draw_line_chart(x, y)
        return

    def symmetry_kibana(self, block, size):
        l_times = size / block + 1
        # l_times = 11804900 / block + 1  # count the number of loop
        flag = 0
        # symmax 104.0  symmin 0.027027027027
        y = []
        data_list = []
        for i in xrange(0, l_times):
            res = self.es.search(index="nprobe-2017.07.01", request_timeout=60,
                                 body={"query": {"match_all": {}}, "from": flag, "size": block})
            for j in res[u'hits'][u'hits']:
                tmp_source = j[u'_source']
                out_packets = tmp_source[u'OUT_PKTS']
                in_packets = tmp_source[u'IN_PKTS']
                sym = 0
                if out_packets == 0:
                    pass
                    # sym = (in_packets + 0.0) / (out_packets + 1)
                else:
                    sym = (in_packets + 0.0) / out_packets
                    if sym > 3 or sym < 0.8:
                        tmp_source[u'symmetry'] = sym
                        data_list.append(
                            {"_index": "symmetry_abnormal",
                             "_type": "flows",
                             "_source": tmp_source
                             })
                    else:
                        pass
            flag += block
        print len(data_list)
        helpers.bulk(self.es, data_list, "symmetry_abnormal", raise_on_error=True)

        return

    # poster
    # write back results two thres 1% kibana symmetry
    # new histogram flow size(in+out)-s(in/out+1)
    # seminar graph kibana

    def draw_line_chart(self, x, y):
        # x = random.rand(50, 30)
        # basic
        # f1 = plt.figure(1)

        plt.scatter(x, y, s=0.5)
        plt.savefig("fig_11804900_200000_reduced_3.png")
        plt.show()
        # with label
        '''
        plt.subplot(212)
        label = list(ones(20)) + list(2 * ones(15)) + list(3 * ones(15))
        label = array(label)
        plt.scatter(x[:, 1], x[:, 0], 15.0 * label, 15.0 * label)
        '''
        return


if __name__ == "__main__":
    col_list = [u'L7_PROTO', u'ENGINE_ID', u'@timestamp', u'IPV4_DST_ADDR', u'UPSTREAM_TUNNEL_ID',
                u'NPROBE_IPV4_ADDRESS', u'L4_DST_PORT', u'UNTUNNELED_IPV4_SRC_ADDR', u'DOWNSTREAM_TUNNEL_ID',
                u'FIRST_SWITCHED', u'LAST_SWITCHED', u'UPSTREAM_SESSION_ID', u'SRC_VLAN', u'FLOW_ID',
                u'PROTOCOL_MAP', u'PROTOCOL', u'OUT_BYTES', u'L4_SRC_PORT', u'IN_PKTS', u'IN_BYTES', u'SRC_TOS',
                u'APPLICATION_ID', u'FLOW_PROTO_PORT', u'IPV4_SRC_ADDR', u'OUT_PKTS', u'UNTUNNELED_PROTOCOL',
                u'DOWNSTREAM_SESSION_ID', u'@version', u'L7_PROTO_NAME']

    es = ElasTest()
    es.symmetry_kibana(block=200000, size=11804900)
    # es.symmetry2(block=200000)
# print es.count('bandwidth')
# es.delete_index('bandwidth')
#    es.create_index('symmetry_abnormal')
# es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
# res = es.search(index="bandwidth")
# print res
# es.r_w_index_bulk_bandwidth(flist=['IN_BYTES', 'OUT_BYTES', '@timestamp'], read_size=100000)
# es.delete_index('writeback-index')

# es.read_write(flist=['IN_BYTES', '@timestamp'], read_size=100)
# es.r_w_pieces(flist=['IN_BYTES', '@timestamp'], read_size=1000, block=10)
# es.r_w_pieces(col_list[:2], read_size=0, block=500000)
# es = ElasTest()
# es.te()
# es.r_w_pieces(col_list[:2], read_size=100000, block=10000)
'''


for i in [100000, 250000, 500000, 600000, 700000, 800000, 900000, 1000000]:
    for j in [1, 2, 5, 10, 15]:
        tp_list = col_list[:j]
        es.r_w_pieces(tp_list, read_size=0, block=i)
'''
###################################################################################################################
###################################################################################################################

# This block is to test reading and writing time for different scales of rows and columns.



#############################250000######################################################################################
# This block is to test reading and writing time for different scale of pieces
'''
for j in [1, 10, 29]:
    tp_list = col_list[:j]
    es.r_pieces(tp_list, read_size=11804900, block=250000)
'''
'''
for x in xrange(3):
    for i in [10000, 100000, 200000, 300000, 400000, 500000, 600000]:
        for j in xrange(1, len(col_list) + 1):
            tp_list = col_list[:j]
            es.read_write(flist=tp_list, read_size=i)

            # This block is to test reading and writing time for different scale of pieces
for y in xrange():
    for k in [1000000, 5000000, 10000000, 11804900]:
        for i in [100000, 500000]:
            for j in [1, 2, 5, 10, 20, 29]:
                tp_list = col_list[:j]
                es.r_w_pieces(tp_list, read_size=k, block=i)
                '''
###################################################################################################################
'''
# This block to test reading time for different scales of rows and columns.
for i in [1, 10, 100, 1000, 10000, 100000, 1000000]:
    for j in xrange(3, len(col_list) + 1):
        tp_list = col_list[:j]
        filterlst = []
        for k in tp_list:
            filterlst.append('hits.hits._source.' + k)
        # print filterlst
        start = time.clock()
        es = Elas_test()
        es.read_test(filterlst, read_size=i)
        end = time.clock()
        print "For %d and " % i, " %d columns" % j, '\t\t', "Done! With a time consumption of %0.6f seconds" % (
        end - start)
'''
###################################################################################################################
###################################################################################################################
# res = es.search(index="nprobe-2017.07.01", body={"query": {"match_all": {}}})
# res = es.search(index="nprobe-2017.07.01", size=100, filter_path=['hits.hits._id', 'hits.hits._type'])

# res = es.search(index="nprobe-2017.07.01", size=1, filter_path=['hits.hits._source.IN_BYTES'], request_timeout=30)

'''  # This block is to get the size of each component in the dictionary
for i in res['hits']['hits'][0][u'_source']:
    tp = res['hits']['hits'][0][u'_source'][i]
    print i, tp, type(tp), sys.getsizeof(tp)
print type(res['hits']['hits'][0][u'_source'][u'IN_BYTES'])
'''
# filter_path=['hits.hits._id', 'hits.hits._type']
# read loop pieces 1000 csvfile java  query-field 1-2 inbyte measurement
# semi


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
# print(res)
