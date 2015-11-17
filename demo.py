__author__ = 'yueming'

from pymongo import MongoClient
import gridfs
import datetime
import time
import unicodecsv
from hurry.filesize import size
import pandas as pd
from fuzzywuzzy import fuzz


class Demo:
    def __init__(self, server, port, db, user):
        self.conn_str = "mongodb://{}:{}".format(server, port)
        self.client = MongoClient(self.conn_str)
        self.db = self.client[db]
        self.user_name = user

    # ------------- login function
    def login(self, user_name, user_password):
        user_group = self.db.user.find({'user_name': user_name, 'user_password': user_password}, {'user_group': 1})
        if user_group.count() == 1:
            print 'User Group is :', user_group[0]['user_group']
            print 'here point to retrieve page'
            self.user_name = user_name
        elif user_group is None:
            print 'No user and password pair exists'
            self.user_name = user_name
        else:
            print 'Multiple users with the same user_name and user_password, system error!'
            print user_group[0]['user_group']
            print 'here point to retrieve page'

    # -------------- update the column mapping, collection column_mapping
    def update_column_mapping(self, old_col, new_col):
        exist_old_cols = self.db.column_mapping.find_one({'old_cols': old_col})
        if exist_old_cols is None:
            self.db.column_mapping.insert_one({'old_cols': old_col, 'new_cols': new_col})
        else:
            self.db.user.find_one_and_update({'old_cols': old_col}, {'$set': {'new_cols': new_col}})

    # ---------------- update standard columns list for login user, collection user
    def update_user_standard_column(self, result):
        result = [x for x in result]
        user_standard_column = self.db.user.find_one({'user_name': self.user_name})
        old_cols_list = user_standard_column.get('standard_cols')
        if old_cols_list is None:
            user_standard_column['standard_cols'] = result
        self.db.user.find_one_and_update({'user_name': self.user_name}, {'$set': {'standard_cols': result}})

    # -----------------update customized columns list for login user, collection user
    def update_user_customized_column(self, result):
        result = [x for x in result]
        user_customized_column = self.db.user.find_one({'user_name': self.user_name})
        old_cols_list = user_customized_column.get('customized_cols')
        if old_cols_list is None:
            user_customized_column['customized_cols'] = result
        self.db.user.find_one_and_update({'user_name': self.user_name}, {'$set': {'customized_cols': result}})

    # ----------------update common standard column list, collection system_conf
    def update_common_standard_column(self, result):
        result = [x for x in result]
        system_conf_record = self.db.system_conf.find_one({})
        if system_conf_record is None:
            new_record = {}
        else:
            new_record = {x: y for x, y in system_conf_record.iteritems()}
        new_record['standard_cols'] = result;
        self.db.system_conf.delete_one({})
        self.db.system_conf.insert_one(new_record)

    # ----------------upload data file to dridfs and collection data_file,  and configuration file to conf_file
    def upload_data_files(self, program, record_mode, read_only, data_file, conf_file, doe_name, doe_descr, comment):
        fs = gridfs.GridFS(self.db)
        # doe_name = doe_name
        # doe_descr = doe_descr
        # comment = comment

        # ------ Check if the DOE name is already stored in database, if exist, delete, for both data file and conf file
        exist_data_file = self.db.data_file.find({'doe_name': doe_name},
                                                 projection={'data_file_id': True, '_id': True})
        exist_conf_file = self.db.conf_file.find({'doe_name': doe_name})
        if (exist_data_file.count() > 0) or (exist_conf_file.count() > 0):
            print "show alert that file exist, let user choose to update or rename the DOE name"
            print "The existing file have the Program name as : {}, Record_mode as : {}, Read_only as : {}".format(
                exist_conf_file.get('program'), exist_conf_file.get('record_mode'), exist_conf_file.get('read_only'))
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

        # ------- Insert new data file into gridfs and an index file into 'data_file' collection
        data_file_id = fs.put(data_file)
        temp = fs.find_one(filter=data_file_id)
        data_dict = {'doe_name': doe_name,
                     'doe_descr': doe_descr,
                     'comment': comment,
                     'upload_user': self.user_name,
                     'upload_date': temp.upload_date,  # time.strftime('%m/%d/%Y,%H:%M:%S')
                     'file_size': size(temp.length),
                     'data_file_id': data_file_id}
        self.db.data_file.insert_one(data_dict)

        # -------- Insert conf file into the 'conf_file' collection
        conf_file.seek(0)
        reader = unicodecsv.reader(conf_file)
        conf_file_cols_list = reader.next()
        conf_file_cols_list = [x.lower().encode('ascii', 'ignore') for x in conf_file_cols_list]
        reader = unicodecsv.DictReader(conf_file, fieldnames=conf_file_cols_list)
        for row in reader:
            row['doe_name'] = doe_name
            row['program'] = program
            row['record_mode'] = record_mode
            row['read_only'] = read_only
            self.db.conf_file.insert_one(row)

        # -------- Update the full columns list, in the 'system_conf' collection
        data_file.seek(0)
        reader = unicodecsv.reader(data_file)
        new_full_cols_list = reader.next()
        new_full_cols_list = [x.lower().encode('ascii', 'ignore') for x in new_full_cols_list]

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

        # ----------- Update the conf columns list, in the 'system_conf' collection
        conf_file.seek(0)
        reader = unicodecsv.reader(conf_file)
        new_conf_cols_list = reader.next()
        new_conf_cols_list = [x.lower().encode('ascii', 'ignore') for x in new_conf_cols_list]
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

    # -------------------retrieve file with aggregation conditions
    def doe_retrieve(self, program, record_mode, read_only, doe_no, design_no, parameter, addition_email, flag):
        fs = gridfs.GridFS(self.db)
        # flag = ['S']  ##-----------------##

        key_list = ['doe#', 'design', 'wafer']
        query_dict = {}
        if len(program) > 0:
            query_dict['program'] = program
        if len(record_mode) > 0:
            query_dict['record_mode'] = record_mode
        if len(read_only) > 0:
            query_dict['read_only'] = read_only
        if len(doe_no) > 0:
            query_dict['doe#'] = doe_no
        if len(design_no) > 0:
            query_dict['design'] = design_no
        if len(parameter) > 0:
            for key, value in parameter.iteritems():
                query_dict[key] = value
        print query_dict
        conf_file = self.db.conf_file.find(query_dict, {'_id': False})
        if conf_file.count() > 0:
            # searched file exist--------------------
            conf = self.db.system_conf.find_one({}, {'conf_cols': True}).get('conf_cols')
            if conf is not None:
                conf_head = [x for x in conf]
            else:
                conf_head = []
            mapping = self.db.column_mapping.find({}, {'_id': False})
            mapping_head = {}
            for m in mapping:
                for k, v in m.iteritems():
                    mapping_head[k.lower()] = v.lower()

            final_header_list = []
            final_header_list_full = []
            final_header_list_cust = []

            # -----------Flag is 'S', using standard columns list
            if 'S' in flag:
                file_name_stand = 'output/{}_STANDARD_{}.csv'.format(self.user_name, time.strftime('%Y%m%d%H%M%S'))
                data = self.db.user.find_one({'user_name': self.user_name}, {'standard_cols': True}).get(
                    'standard_cols')
                if data is None:
                    print "User don't have standard columns list, use the system standard column list"
                    data = self.db.system_conf.find_one({}).get('standard_cols')

                data_header = [x for x in data]

                # print 'mapping_head:', mapping_head

                for head in data_header:
                    if head not in final_header_list:
                        final_header_list.append(head)
                for head in conf_head:
                    if head in key_list:
                        if head not in final_header_list:
                            final_header_list.append(head)
                    else:
                        if head + '_conf' not in final_header_list:
                            final_header_list.append(head + '_conf')

            if 'F' in flag:
                file_name_full = 'output/{}_FULL_{}.csv'.format(self.user_name, time.strftime('%Y%m%d%H%M%S'))
                data_full = self.db.user.find_one({'user_name': self.user_name}, {'full_cols': True}).get(
                    'full_cols')
                if data_full is None:
                    print "User don't have full columns list, use the system full column list"
                    data_full = self.db.system_conf.find_one({}).get('full_cols')
                if data_full is not None:
                    data_header_full = [x for x in data_full]
                else:
                    data_header_full = []

                # print 'mapping_head:', mapping_head
                for head in data_header_full:
                    if head not in final_header_list_full:
                        final_header_list_full.append(head)
                for head in conf_head:
                    if head in key_list:
                        if head not in final_header_list_full:
                            final_header_list_full.append(head)
                    else:
                        if head + '_conf' not in final_header_list_full:
                            final_header_list_full.append(head + '_conf')

            if 'C' in flag:
                file_name_cust = 'output/{}_CUSTOMIZED_{}.csv'.format(self.user_name, time.strftime('%Y%m%d%H%M%S'))
                data_cust = self.db.user.find_one({'user_name': self.user_name}, {'customized_cols': True}).get(
                    'customized_cols')
                if data_cust is None:
                    print "User don't have customized columns list, use the system standard column list"
                    data_cust = self.db.system_conf.find_one({}).get('standard_cols')
                if data_cust is not None:
                    data_header_cust = [x for x in data_cust]
                else:
                    data_header_cust = []
                # print 'mapping_head:', mapping_head
                for head in data_header_cust:
                    if head not in final_header_list_cust:
                        final_header_list_cust.append(head)
                for head in conf_head:
                    if head in key_list:
                        if head not in final_header_list_cust:
                            final_header_list_cust.append(head)
                    else:
                        if head + '_conf' not in final_header_list_cust:
                            final_header_list_cust.append(head + '_conf')

            final = []
            final_cust = []
            final_full = []

            for f in conf_file:
                conf_pf = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in f.iteritems()]))
                doe_name_search = f.get('doe_name')
                if doe_name_search is not None:
                    print 'doe_name_search', doe_name_search
                    data_file_id = self.db.data_file.find_one({'doe_name': doe_name_search},
                                                              {'data_file_id': True}).get('data_file_id')
                    if data_file_id is not None:
                        with fs.get(data_file_id) as data_file:
                            data_pf = pd.read_csv(data_file, encoding='utf-8-sig')
                            data_pf = data_pf.rename(columns=lambda x: mapping_head[x] if x in mapping_head else x)
                            data_pf.columns = [x.lower() for x in data_pf.columns]
                            result = pd.merge(data_pf, conf_pf, on=key_list, how='inner',
                                              suffixes=['', '_conf'])
                            result_header = result.columns.values

                            if len(final_header_list) > 0:
                                for h in final_header_list:
                                    if h not in result_header:
                                        result[h] = ''
                                temp = result[final_header_list]
                                temp.is_copy = False
                                temp['doe_name'] = doe_name_search
                                final.append(temp)
                                print 'standard doe_name_search: ', doe_name_search
                            if len(final_header_list_cust) > 0:
                                for h in final_header_list_cust:
                                    if h not in result_header:
                                        result[h] = ''
                                temp = result[final_header_list_cust]
                                temp.is_copy = False
                                temp['doe_name'] = doe_name_search
                                final_cust.append(temp)
                                print 'customized doe_name_search: ', doe_name_search

                            if len(final_header_list_full) > 0:
                                for h in final_header_list_full:
                                    if h not in result_header:
                                        result[h] = ''
                                temp = result[final_header_list_full]
                                temp.is_copy = False
                                temp['doe_name'] = doe_name_search
                                final_full.append(temp)
                                print 'full doe_name_search: ', doe_name_search
            if len(final_header_list) > 0:
                final_pf = pd.concat(final)
                final_pf.to_csv(file_name_stand, index=False)
            if len(final_header_list_cust) > 0:
                final_cust_pf = pd.concat(final_cust)
                final_cust_pf.to_csv(file_name_cust, index=False)
            if len(final_header_list_full) > 0:
                final_full_pf = pd.concat(final_full)
                final_full_pf.to_csv(file_name_full, index=False)
        else:
            print "Aggregated file not found."

    # --------------------- show upload files summary
    def doe_summary(self, doe_name, doe_descr, comment, s_y, s_m, s_d, e_y, e_m, e_d):
        doe_name = doe_name
        doe_descr = doe_descr
        comment = comment
        query_dict = {}
        if len(doe_name) > 0:
            query_dict['doe_name'] = doe_name
        if len(doe_descr) > 0:
            query_dict['doe_descr'] = doe_descr
        if len(comment) > 0:
            query_dict['comment'] = comment
        if s_y != '' and s_m != '' and s_d != '':
            if query_dict.get('upload_date') is None:
                query_dict['upload_date'] = {}
            query_dict['upload_date']['$gt'] = datetime.datetime(int(s_y), int(s_m), int(s_d), 0, 0, 1, 0)
        if e_y != '' and e_m != '' and e_d != '':
            if query_dict.get('upload_date') is None:
                query_dict['upload_date'] = {}
            query_dict['upload_date']['$lt'] = datetime.datetime(int(e_y), int(e_m), int(e_d), 23, 59, 59, 0)
        print query_dict
        return self.db.data_file.find(query_dict, {'_id': False, 'data_file_id': False})

    def clear_all(self):
        self.db.system_conf.drop()
        self.db.data_file.drop()
        self.db.conf_file.drop()
        self.db.fs.files.drop()
        self.db.fs.chunks.drop()

    def chk_program_record(self, program, record_mode):
        result = self.db.data_conf.find({'program': program})
        if result.count() == 1:
            if result[0]['record_mode'] == record_mode:
                return True
        return False

    def chk_columns(self, data_file, conf_file):
        sys_cols = self.db.system_conf.find_one({})
        sys_cols_full = sys_cols.get('full_cols')
        sys_cols_conf = sys_cols.get('conf_cols')

        conf_file.seek(0)
        reader = unicodecsv.reader(conf_file)
        conf_file_cols_list = reader.next()
        conf_file_cols_list = [x.lower().encode('ascii', 'ignore') for x in conf_file_cols_list]
        data_file.seek(0)
        reader = unicodecsv.reader(data_file)
        data_file_cols_list = reader.next()
        data_file_cols_list = [x.lower().encode('ascii', 'ignore') for x in data_file_cols_list]

        new_conf_cols_list = []
        new_data_cols_list = []
        dup_conf_cols_list = []
        dup_conf_cols_set = set()
        dup_data_cols_list = []
        dup_data_cols_set = set()

        for t in conf_file_cols_list:
            if t not in sys_cols_conf:
                new_conf_cols_list.append(t)
            if t not in dup_conf_cols_set:
                dup_conf_cols_set.add(t)
            else:
                dup_conf_cols_list.append(t)

        for t in data_file_cols_list:
            if t not in sys_cols_full:
                new_data_cols_list.append(t)
            if t not in dup_data_cols_set:
                dup_data_cols_set.add(t)
            else:
                dup_data_cols_list.append(t)

        result = {'new_conf': new_conf_cols_list, 'new_data': new_data_cols_list, 'dup_conf': dup_conf_cols_list,
                  'dup_data': dup_data_cols_list}

        lst = []
        for i in range(len(conf_file_cols_list) - 1):
            temp = conf_file_cols_list[i]
            for j in range(i + 1, len(conf_file_cols_list)):
                comp = conf_file_cols_list[j]
                rate = fuzz.ratio(temp, comp)
                if rate > 80:
                    t = [temp, comp, rate]
                    lst.append(t)
        result['conf_cols_ratio'] = lst

        lst = []
        for t in new_conf_cols_list:
            for tt in conf_file_cols_list:
                rate = fuzz.ratio(t, tt)
                if rate > 80:
                    r = [t, tt, rate]
                    lst.append(t)
        result['new_conf_ratio'] = lst

        lst = []
        for i in range(len(data_file_cols_list) - 1):
            temp = data_file_cols_list[i]
            for j in range(i + 1, len(data_file_cols_list)):
                comp = data_file_cols_list[j]
                rate = fuzz.ratio(temp, comp)
                if rate > 80:
                    t = [temp, comp, rate]
                    lst.append(t)
        result['data_cols_ratio'] = lst

        lst = []
        for t in new_data_cols_list:
            for tt in data_file_cols_list:
                rate = fuzz.ratio(t, tt)
                if rate > 80:
                    r = [t, tt, rate]
                    lst.append(r)
        result['new_data_ratio'] = lst

        return result

    def setColMapping(self):
        with open('input/Mapping.csv', 'rb') as f:
            spamreader = unicodecsv.reader(f)
            for row in spamreader:
                old_col = row[0].lower().encode('ascii', 'ignore')
                new_col = row[1].lower().encode('ascii', 'ignore')
                self.update_column_mapping(old_col,new_col)

if __name__ == '__main__':
    demo = Demo('mapserverdev', '27017', 'detdp', 'map')

    # ------login-------------------------
    demo.login('map', 'map')
    # print time.strftime('%m/%d/%Y,%H:%M:%S')

    ##############################
    # demo.clear_all()
    ##############################

    file_list = ['DOE949 WFWG', 'DOE950 W9UA', 'DOE976 WFTY']
    program = ['Apollo', 'Rembrandt', 'Apollo']
    # ----------------- Load files from the above list to database
    #
    # count = len(file_list)
    # for f in file_list:
    #     with open('input/{} Config.csv'.format(f), 'rb') as conf_file:
    #         with open('input/{} Data.csv'.format(f), 'rb') as data_file:
    #             demo.upload_data_files(program[count - 1], 'CMR', 'Y', data_file, conf_file, 'doe_00{}'.format(count),
    #                                    'first test for {}'.format(f), 'Try it out')
    #             print 'Finish: doe_00{}'.format(count)
    #             count -= 1

    # ------------------- checking Program and Recorad_mode list
    # for i in range(3):
    #     print demo.chk_program_record(program[i], 'CMR')

    # -------------------Load  the common standard columns list into database

    # unique_dict = {}
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
    #         result.append(key.lower())
    # demo.update_common_standard_column(result)



    # ------------------Load common/customized columns list to user profile
    # demo.update_user_customized_column(result)
    # demo.update_user_standard_column(result)


    # -------------------show files summary
    # result = demo.doe_summary('', {'$regex':'^first'},'', 2015,10,31, 2015,11,17)
    # result = demo.doe_summary('', {'$regex':'WFTY$'},'', 2015,10,31, 2015,11,17)
    # result = demo.doe_summary('', {'$regex':'949'}, '', '','','', '','','')
    # print 'Here is the result:'
    # for r in result:
    #     print r


    # -------------------retrieve data based on customized condition
    # demo.doe_retrieve('Rembrandt', 'CMR', 'Y', {'$in': ['Ctrl M41.3a', 'Ref W950']}, 'C M41.3a',
    #                   {'wg': '21nm', 'sg': '25nm'}, '', ['S', 'F', 'C'])


    # ---------------------check column fuzzy
    # with open('input/DOE949 WFWG Config.csv', 'rb') as conf_file:
    #     with open('input/DOE949 WFWG Data.csv', 'rb') as data_file:
    #         result = demo.chk_columns(data_file, conf_file)
    #         for k,v in result.iteritems():
    #             print k, v


    # --------------------set columns mapping
    demo.setColMapping()
