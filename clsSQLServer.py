"""

SQLServer connection class

Description:
    Collection of methods for interacting with a SQLServer database.
    Offers logging feature so your project can capture logging events.

Created: 2020 05

Usage:
    Init:
        Set a variable equal to the Interface, passing in arguments for database and logger name:
            DB = clsSQLServer.Interface(database='Investments', logname='projectlog')

    SelectQuery(SQL):
        Returns a recordset based on SQL:
            Recordset = DB.SelectQuery('select foo from bazTable')
        FYI for some reason this returns empty values in the tuples:
            [('foo1', ), ('foo2', )]

    Execute(SQL):
        Executes SQL where no return of data is expected:
            DB.Execute('truncate table bazTable')

    InsertMany(SQLStatement, Values):
        Inserts a list of tuples:
            SQLStatement: 'insert into bazTable (foo, bar) values (?, ?)'
            Values: a list of tuples [(foo1, bar1), (foo2, bar2)]
            DB.InsertMany(SQLStatement, Values)

"""

import logging

class Interface:

    def __init__(self, **kw):
        self._InitialDatabase = kw.get('database')
        try:
            self._logname = kw.get('logname')
            self._logger = logging.getLogger(self._logname)
        except Exception:
            pass

        if self._logname:
            self._logger.debug('%s: DB server interface initialized' % (__name__))

    def __repr__(self):
        return "clsSQLServer.Interface(database='{}', logname='{}')".format(self._InitialDatabase, self._logname)

    def __str__(self):
        return '{} - {}'.format(self._InitialDatabase, self._logname)

    def Connection(self):
        import pyodbc
        ConnectionParameters = 'Driver={SQL Server};Server=DESKTOP-T7O31RM;Database=%s;Trusted_Connection=yes;' % (self._InitialDatabase)
        conn = pyodbc.connect(ConnectionParameters)
        return conn

    def SelectQuery(self, SQLStatement):
        cursor = self.Connection().cursor()
        self.LogThis(SQLStatement)
        cursor.execute(SQLStatement)
        return cursor.fetchall()

    def Execute(self, SQLStatement):
        cursor = self.Connection().cursor()
        self.LogThis(SQLStatement)
        cursor.execute(SQLStatement)
        cursor.commit()

    def InsertMany(self, SQLStatement, Values):
        cursor = self.Connection().cursor()
        self.LogThis(SQLStatement)
        cursor.executemany(SQLStatement, Values)
        cursor.commit()

    def LogThis(self, Thing):
        self._logger.debug('%s: DB interface is trying \'%s\'' % (__name__, Thing))
