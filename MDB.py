# coding=utf-8
# Date   : 18-11-28
# Author : neo
import yaml
import os
import torndb


class DB:
    options = {
        "field": "",
        "table": "",
        "join": "",
        "where": "",
        "having": "",
        "group": "",
        "order": "",
        "limit": "",
    }
    temp = {
        "add": "",
        "update": "",
    }

    def __init__(self, conf="./default.yaml"):
        try:
            if type(conf) == str:
                if os.path.exists(conf) == False:
                    err = "Error conf file not exists"
                    raise Exception(err)
                with open(conf) as cf:
                    dbconf = yaml.load(cf)["db"]
            if conf == False:
                raise ("no setting")
            else:
                self.dbconf = dbconf
        except:
            print "can not load database setting"
        self.getConn()

    def getConn(self):
        db = torndb.Connection(**self.dbconf)
        self.dbconn = db
        return self

    def table(self, table):
        for k in self.options:
            self.options[k] = ''
        for k in self.temp:
            self.temp[k] = ''

        tb = table.split(' ')
        if len(tb) > 1:
            table = "`%s` %s" % (tb[0], tb[1])
        else:
            table = "`%s`" % table
        self.options['table'] = table
        return self

    def query(self, sql):
        result = self.dbconn.query(sql)
        return result

    def execute(self, sql):
        result = self.dbconn.execute(sql)
        return result

    def fetch(self, type='select'):
        if self.options["table"] == "":
            raise Exception("no table selected")
        if type == "select":
            sql = "select  (field)  from  (table)   (join)   (where)   (group)   (having)   (order)   (limit) ;"
            for k, v in self.options.items():
                if k == "field" and v == '':
                    v = " * "
                sql = sql.replace(" (%s) " % k, v)
            sql = " ".join(sql.split())
        elif type == "insert":
            add = self.temp["add"]
            sql = "insert into  (table)  ( (key) ) value ( (value) );"
            keys = ",".join(["`%s`" % str(k) for k in add.keys()])
            values = ",".join(["'%s'" % str(v) for v in add.values()])
            sql = sql.replace(" (table) ", self.options["table"])
            sql = sql.replace(" (key) ", keys)
            sql = sql.replace(" (value) ", values)
        elif type == "update":
            sql = "update  (table)   (join)  set  (update)   (where)  ;"
            for k, v in self.options.items():
                if k == "where" and v == "":
                    raise Exception("no where identity, use ' ' for where ")
                sql = sql.replace(" (%s) " % k, v)
            updates = []
            for k, v in self.temp["update"].items():
                t = k.split('.')
                if len(t) > 1:
                    updates.append(" %s.`%s`='%s'" % (t[0], t[1], v))
                else:
                    updates.append(" `%s`='%s'" % (k, v))
            update = ",".join(updates)
            sql = sql.replace(" (update) ", update)
        elif type == "delete":
            sql = "delete from  (table)   (join)   (where)   (group)   (having) ;"
            for k, v in self.options.items():
                if k == "where" and v == "":
                    raise Exception("no where identity, use ' ' for where ")
                sql = sql.replace(" (%s) " % k, v)
        else:
            sql = ""
        sql = " ".join(sql.split())
        return sql

    def find(self):
        self.options["limit"] = "limit 0,1"
        sql = self.fetch("select")
        result = self.dbconn.get(sql)
        self.sql = sql
        return result

    def select(self):
        sql = self.fetch("select")
        result = self.dbconn.query(sql)
        self.sql = sql
        return result

    def insert(self, add=''):
        if add == '' or type(add) != dict:
            return ''
        self.temp["add"] = add
        sql = self.fetch("insert")
        result = self.dbconn.execute(sql)
        self.sql = sql
        return result

    def add(self, add=''):
        return self.insert(add)

    def delete(self):
        sql = self.fetch("delete")
        result = self.dbconn.execute_rowcount(sql)
        self.sql = sql
        return result

    def update(self, update):
        if update == '' or type(update) != dict:
            return ''
        self.temp["update"] = update
        sql = self.fetch("update")
        result = self.dbconn.execute(sql)
        self.sql = sql
        return result

    def save(self, update):
        return self.update(update)

    def where(self, where):
        if type(where) == dict and len(where) > 0:
            where_arr = []
            logic = ["exp", "eq", "neq", "lt", "gt", "elt", "egt",
                     "between", "not between", "in", "not in", "like"]
            for k, v in where.items():
                if len(k.split('.')) > 1:
                    key = " %s.`%s` " % (k.split('.')[0], k.split('.')[1])
                elif k == "_string":
                    where_arr.append(v)
                    continue
                else:
                    key = " `%s` " % (k)

                if type(v) is not list:
                    where_arr.append("%s = '%s'" % (key, str(v)))
                    continue

                vl = v[0].lower()
                if vl not in logic:
                    continue
                if vl == 'exp':
                    where_arr.append(" %s %s " % (key, str(v[1])))
                    continue
                elif vl == 'eq':
                    where_arr.append(" %s = '%s' " % (key, str(v[1])))
                    continue
                elif vl == 'neq':
                    where_arr.append(" %s <> '%s' " % (key, str(v[1])))
                    continue
                elif vl == 'lt':
                    where_arr.append(" %s < '%s' " % (key, str(v[1])))
                    continue
                elif vl == 'elt':
                    where_arr.append(" %s <= '%s' " % (key, str(v[1])))
                    continue
                elif vl == 'gt':
                    where_arr.append(" %s > '%s' " % (key, str(v[1])))
                    continue
                elif vl == 'egt':
                    where_arr.append(" %s >= '%s' " % (key, str(v[1])))
                    continue
                elif vl == 'between':
                    where_arr.append(" %s between '%s' and '%s' " % (key, str(v[1][0]), str(v[1][1])))
                    continue
                elif vl == 'not between':
                    where_arr.append(" %s not between '%s' and '%s' " % (key, str(v[1][0]), str(v[1][1])))
                    continue
                elif vl == 'in':
                    vv = ["'%s'" % str(x) for x in v[1]]
                    where_arr.append(" %s in (%s) " % (key, ",".join(vv)))
                    continue
                elif vl == 'not in':
                    vv = ["'%s'" % str(x) for x in v[1]]
                    where_arr.append(" %s not in (%s) " % (key, ",".join(vv)))
                    continue
                elif vl == 'like':
                    where_arr.append(" %s like '%%%s%%' " % (key, v[1]))
                    continue
                else:
                    continue
            where = " where " + ' and '.join(where_arr)
            where = ' '.join(where.split())
        if type(where) == list and len(where) > 0:
            where = "where " + " and ".join(where)
            where = ' '.join(where.split())
        self.options['where'] = where
        return self

    def join(self, join):
        if type(join) == list:
            join = ' '.join(join)
        self.options["join"] = join
        return self

    def field(self, field):
        self.options["field"] = field
        return self

    def group(self, group):
        self.options["group"] = "group by %s" % group
        return self

    def having(self, having):
        self.options["having"] = "having %s" % having
        return self

    def limit(self, limit):
        if type(limit) == list:
            self.options["limit"] = " limit %s,%s" % (limit[0], limit[1])
        else:
            self.options["limit"] = " limit %s" % limit
        return self

    def order(self, order):
        self.options["order"] = " order by %s" % order
        return self

    def getField(self, field):
        farr = field.split("as")
        if len(farr) > 1:
            get_field = farr[1].trim()
        else:
            get_field = field
        self.options["field"] = field
        result = self.find()
        if result == None:
            return ''
        return result[get_field]
