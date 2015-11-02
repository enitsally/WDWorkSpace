__author__ = 'yueming'

from pymongo import MongoClient
import gridfs
import datetime
import time
import csv


class Demo:
    def __init__(self, server, port, db, user_name):
        self.conn_str = "mongodb://{}:{}".format(server, port)
        self.client = MongoClient(self.conn_str)
        self.db = self.client[db]
        self.user_name = user_name

    def update_column_mapping(self):
        pass

    def update_user_standard_column(self, result):
        user_standard_column = self.db.user.find_one({'user_name': self.user_name})
        old_cols_list = user_standard_column.get('standard_cols')
        if old_cols_list is None:
            user_standard_column['standard_cols'] = result
        self.db.user.find_one_and_update({'user_name': self.user_name}, {'$set': {'standard_cols': result}})

    def update_user_customized_column(self, result):
        user_customized_column = self.db.user.find_one({'user_name': self.user_name})
        old_cols_list = user_customized_column.get('customized_cols')
        if old_cols_list is None:
            user_customized_column['customized_cols'] = result
        self.db.user.find_one_and_update({'user_name': self.user_name}, {'$set': {'customized_cols': result}})

    def update_common_standard_column(self, result):
        system_conf_record = self.db.system_conf.find_one({})
        new_record = system_conf_record
        new_record['standard_cols'] = result;
        self.db.system_conf.delete_one({})
        self.db.system_conf.insert_one(new_record)

    def upload_data_files(self, data_file, conf_file, doe_name, doe_descr, comment):
        fs = gridfs.GridFS(self.db)

        # ------ Check if the DOE name is already stored in database, if exist, delete, for both data file and conf file
        exist_data_file = self.db.data_file.find({'doe_name': doe_name}, projection={'data_file_id': True, '_id': True})
        exist_conf_file = self.db.conf_file.find({'doe_name': doe_name})
        if (exist_data_file is not None) or (exist_conf_file is not None):
            print "show alert that file exist, let user choose to update or rename the DOE name"
            if True:  # user choose to update the existing data
                # 1 for data file in 'data_file' collection, if find, delete
                count = 0
                if exist_data_file.count() > 0:
                    for f in exist_data_file:
                        fs.delete(f['data_file_id'])
                        count += 1
                    print 'Delete data file record from detdp database gridfs collection, # of deleted records:', count
                    result = self.db.data_file.delete_many({'data_file_id': f['data_file_id']})
                    print 'Delete data file index record from data_file collection, # of deleted records:', result.deleted_count
                # 2 for conf file in 'conf_file' collection, if find, delete
                if exist_conf_file.count() > 0:
                    result = self.db.conf_file.delete_many({'doe_name': doe_name})
                    print 'Delete conf file record from conf_file collection, # of deleted records:', result.deleted_count

            else:
                return

        # ------- Insert new data file into dridfs and an index file into 'data_file' collection
        data_file_id = fs.put(data_file)
        temp = fs.find_one(filter=data_file_id)
        data_dict = {'doe_name': doe_name,
                     'doe_descr': doe_descr,
                     'comment': comment,
                     'upload_user': self.user_name,
                     'upload_date': temp.upload_date,  # time.strftime('%m/%d/%Y,%H:%M:%S')
                     'file_size': temp.chunk_size,
                     'data_file_id': data_file_id}
        self.db.data_file.insert_one(data_dict)

        # -------- Insert conf file into the 'conf_file' collection
        conf_file.seek(0)
        reader = csv.reader(conf_file)
        conf_file_cols_list = reader.next()
        reader = csv.DictReader(conf_file, fieldnames=conf_file_cols_list)
        for row in reader:
            row['doe_name'] = doe_name
            self.db.conf_file.insert_one(row)

        # -------- Update the full columns list, in the 'system_conf' collection
        data_file.seek(0)
        reader = csv.reader(data_file)
        new_full_cols_list = reader.next()
        old_full_cols = self.db.system_conf.find_one({})
        if old_full_cols is None:
            temp = {'full_cols': new_full_cols_list}
            self.db.system_conf.insert_one(temp)
        else:
            old_full_cols_list = old_full_cols.get('full_cols')
            if old_full_cols_list is None:
                update_full_cols_list = old_full_cols
                update_full_cols_list['full_cols'] = new_full_cols_list
                self.db.system_conf.delete_one({})
                self.db.system_conf.insert_one(update_full_cols_list)
            else:
                update_full_cols_list = set(old_full_cols_list).union(set(new_full_cols_list))
                result = self.db.system_conf.replace_one({'full_cols': old_full_cols_list},
                                                         {'full_cols': list(update_full_cols_list)}, True)
                # print 'Update full_cols, matched_count',result.matched_count
                # print 'Update full_cols, modified_count',result.modified_count
        conf_file.seek(0)
        reader = csv.reader(conf_file)
        new_conf_cols_list = reader.next()

        old_conf_cols = self.db.system_conf.find_one({})

        if old_conf_cols is None:
            temp = {'conf_cols': new_conf_cols_list}
            self.db.system_conf.insert_one(temp)
        else:
            old_conf_cols_list = old_conf_cols.get('conf_cols')
            if old_conf_cols_list is None:
                update_conf_cols_list = old_conf_cols
                update_conf_cols_list['conf_cols'] = new_conf_cols_list
                self.db.system_conf.delete_one({})
                self.db.system_conf.insert_one(update_conf_cols_list)
            else:
                update_conf_cols_list = set(old_conf_cols_list).union(set(new_conf_cols_list))
                result = self.db.system_conf.replace_one({'conf_cols': old_conf_cols_list},
                                                         {'conf_cols': list(update_conf_cols_list)}, True)
                # print 'Update conf_cols, matched_count',result.matched_count
                # print 'Update conf_cols, modified_count',result.modified_count

    def login(self, user_name, user_password):
        user_group = self.db.user.find({'user_name': user_name, 'user_password': user_password}, {'user_group': 1})
        if user_group.count() == 1:
            print user_group[0]['user_group']
            print 'here point to retrieve page'
            self.user_name = user_name
        elif user_group is None:
            print 'No user and password pair exists'
            self.user_name = user_name
        else:
            print 'Multiple users with the same user_name and user_password, system error!'
            print user_group[0]['user_group']
            print 'here point to retrieve page'

    def doe_retrieve(self, doe_no, design_no, parameter, addition_email):
        doe_no_list = doe_no.split(',')
        pass

    def doe_summary(self, doe_name, doe_descr, comment, s_y, s_m, s_d, e_y, e_m, e_d):

        query_dict = {}
        query_dict['upload_date'] = {}

        if doe_name != '':
            query_dict['doe_name'] = doe_name
        if doe_descr != '':
            query_dict['doe_descr'] = doe_descr
        if comment != '':
            query_dict['comment'] = comment
        if s_y != '' and s_m != '' and s_d != '':
            query_dict[u'upload_date']['$gt'] = datetime.datetime(int(s_y),int(s_m), int(s_d), 0, 0, 1, 0)
        if e_y != '' and e_m != '' and e_d != '':
            end = datetime.datetime(int(e_y),int(e_m), int(e_d), 23, 59, 59, 0)
            print end
            query_dict[u'upload_date']['$lt'] = datetime.datetime(2015, 11, 2, 23, 59, 59, 0)
        print query_dict
        result = self.db.data_file.find(query_dict, {'_id': False, 'data_file_id': False})
        return result


if __name__ == '__main__':
    demo = Demo('localhost', '27017', 'detdp', 'map')
    demo.login('map', 'map')
    # print time.strftime('%m/%d/%Y,%H:%M:%S')
    # with open ('input/DOE976 WFTY Config.csv','r') as conf_file:
    #     with open ('input/DOE949 WFWG Data.csv','r') as data_file:
    #         demo.upload_data_files(data_file,conf_file,'doe_003','first test for doe_003', 'Try it out')
    unique_dict = {}
    file_list = ['DOE949 WFWG', 'DOE950 W9UA', 'DOE976 WFTY']
    # for file in file_list:
    #     with open('input/{} Data.csv'.format(file), 'r') as data_file:
    #         data_file.seek(0)
    #         header = data_file.next()
    #         header_list = header.split(',')
    #
    #         content = data_file.next()
    #         content_list = content.split(',')
    #
    #         data_dict = dict(zip(header_list, content_list))
    #         for key, value in data_dict.iteritems():
    #             if (value is not None) and (value != ''):
    #                 if unique_dict.get(key) is None:
    #                     unique_dict[key] = 1
    #                 else:
    #                     unique_dict[key] += 1
    # result = []
    # for key, value in unique_dict.iteritems():
    #     if value == 3:
    #         result.append(key)

    # result.append('MyTestHere')
    # print result
    # demo.update_user_customized_column(result)
    result = demo.doe_summary('', '', '', 2015,10,31, 2015,11,2)
    for r in result:
        print r
