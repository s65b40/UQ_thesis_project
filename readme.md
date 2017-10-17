This is the thesis project of UQ ENGG4802.
Author: Haochun Wang, 44948544 of UQ; 1140310109 of HIT
This is to build a frame for elasticsearch.


# errors and warnings
## Result window is too large, from + size must be less than or equal to: [10000] but was [100000]
curl -XPUT "http://localhost:9200/my_index/_settings" -d '{ "index" : { "max_result_window" : 500000 } }'
PUT /nprobe-2017.07.01/_settings
{ "index" : { "max_result_window" : 50000000 } }
## http://www.chepoo.com/elasticsearch-installation-parameters-configuration-considerations.html

# snapshot and restore
https://www.elastic.co/guide/en/elasticsearch/reference/5.6/modules-snapshots.html
# reminders
## figure_1000 - figure_1000000
Ubuntu 16.0.4 virtual machine 2 cores i7-4770hq 2.2Ghz 8G memory
##
Ubuntu 14.xxx OS                      i7-6700hq 2.6Ghz 16G memory