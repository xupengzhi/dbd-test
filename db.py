# -*- coding: utf-8 -*-
#!/usr/bin/python

from bsddb import db
from threading import RLock
from functools import wraps
import os, sys

def synchronized(lock_name):
    def _synchronize(func):
        @wraps(func)
        def _synchronized_func(self, *args, **kwargs):
            lock = self.__getattribute__(lock_name)
            lock.acquire()
            try:
                return func(self, *args, **kwargs)
            finally:
                lock.release()
        return _synchronized_func
    return _synchronize

    
class FooDB(object):
    # eg.: db_home_paht="./" db_file_name=".FOO" db_name="001"
    def __init__(self, db_home_path, db_file_name, db_name, is_txn=False):
        self.is_txn = is_txn
        if not os.path.exists(db_home_path):
            os.makedirs(db_home_path)
        self._db_home_path = db_home_path
        self._db_env = db.DBEnv()
        try:
            print "env initializing..."
            if(self.is_txn):
                self._db_env.open(
                    self._db_home_path.encode(sys.getfilesystemencoding()),
                    (db.DB_CREATE |
                     db.DB_INIT_MPOOL |
                     db.DB_INIT_LOCK |
                     db.DB_INIT_TXN |
                #     db.DB_INIT_LOG |
                     db.DB_THREAD |
                #     db.DB_LOG_AUTOREMOVE |
                     db.DB_RECOVER_FATAL))
            else:
                self._db_env.open(
                    self._db_home_path.encode(sys.getfilesystemencoding()),
                    (db.DB_CREATE |
                     db.DB_INIT_MPOOL |
                     db.DB_INIT_LOCK |
                     db.DB_THREAD ))
            
        except db.DBError as e:
            #shutil.rmtree(self._db_home_path)
            raise
            
        self._inst_lock = RLock()
        self.foo_db = db.DB(self._db_env)
        
        print "db initializing..."
        if(self.is_txn):
            txn=self._db_env.txn_begin()
            self.foo_db.open(db_file_name,
                            "{0}.bar".format(db_name),
                            db.DB_BTREE,
                            db.DB_CREATE | db.DB_THREAD,
                            txn=txn)
            txn.commit()
        else:
            self.foo_db.open(db_file_name,
                        "{0}.bar".format(db_name),
                        db.DB_BTREE,
                        db.DB_CREATE | db.DB_THREAD)
        
        # equal to db verification
        self.foo_db.stat()
        print "db done!!!"
        
    def __del__(self):
        self.close()
    
    @synchronized("_inst_lock")
    def close(self):
        self.foo_db.close()

    def put(self, key, value, txn=None):
        if txn is None:
            self.foo_db.put(key, value)
        else:
            self.foo_db.put(key, value, txn)
                           
    @synchronized("_inst_lock")
    def get(self, key):
        value = self.foo_db.get(key, default=None)
        return value if value is not None else None
        
    @synchronized("_inst_lock")
    def writeTxn(self, tree):
        # get transaction context
        txn = self._db_env.txn_begin(flags=db.DB_DIRTY_READ|db.DB_TXN_SYNC)

        self.foo_db.truncate(txn=txn)
        items = tree.items()
        for k, v in items:
            self.put(k, v, txn)
        #self.foo_db.sync(txn=txn)
        
        txn.commit()

    @synchronized("_inst_lock")
    def write(self, tree):
        if self.is_txn:
            self.writeTxn(tree)
            return
            
        self.foo_db.truncate()
        items = tree.items()
        for k, v in items:
            self.put(k, v)  
        self.foo_db.sync()

def main():
    foo = FooDB("./foo", ".FOO", "001", False)
    # generate 10000 k,v items
    tree = {}
    for i in range(100000):
        k_prefix = "this/is/the/test/program/key"
        v_prefix = "this/is/the/test/program/val"
        k = k_prefix + str(i)
        v = v_prefix + str(i)
        tree[k] = v
    
    print "press enter key to start db testing\n"
    raw_input()
    n = 0
    while 1:
        n = n + 1
        foo.write(tree)
        #foo.writeTxn(tree)
        print n

if __name__ == "__main__":
    main()