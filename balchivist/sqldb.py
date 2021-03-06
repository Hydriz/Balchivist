#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (C) 2015-2018 Hydriz Scholz
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import MySQLdb
import socket

from config import BALConfig
from exception import IncorrectUsage
import message


class BALSqlDb(object):
    """
    This module is used to provide an interface for interacting with an SQL
    database and the regular functions specific to Balchivist.
    """
    hostname = socket.gethostname()

    def __init__(self, database="balchivist", host="localhost", user="root", passwd=""):
        """
        This function is executed when a new instance of BALSqlDb is
        initialized.

        - database (string): The database name to work with.
        - host (string): The MySQL server hosting the database
        - default (string): The path to the file with the MySQL credentials.
        """
        self.database = database
        self.host = host
        self.user = user
        self.passwd = passwd

    @classmethod
    def getFromConf(cls):
        """
        This function is used to initialize a BALSqlDb instance based on the
        configuration stored in settings.conf.
        """
        config = BALConfig('main')
        return cls(database=config.get('database'),
                   host=config.get('host'),
                   user=config.get('user'),
                   passwd=config.get('passwd'))

    def getConds(self, params):
        """
        This function is used for getting the conditions necessary for the
        SQL query to work.

        - params (dict): A dictionary of the conditions to transform.

        Returns: String with the SQL-like conditions.
        """
        conds = []
        for cond in params:
            conds.append('%s="%s"' % (cond, params[cond]))
        return ' AND '.join(conds)

    def claimItem(self, params, dbtable=None):
        """
        This function is used to claim an item from the database to prevent
        other instances of Balchivist from operating on the same item.

        Note: This function will only work if the module supports claiming of
        items and that all claims are tracked under the "claimed_by" column,
        which should be the case for most modules.

        - params (dict): Information about the item to claim, must be as
        unique as possible.
        - dbtable (string): The name of the database table.

        Returns: True if the operation is successful, False if an error has
        occurred.
        """
        vals = {
            'claimed_by': '"%s"' % (self.hostname)
        }
        return self.update(dbtable=dbtable, values=vals,
                           conds=self.getConds(params=params))

    def execute(self, query, params=()):
        """
        This function is used to execute a query on the database given when
        initializing the module.

        - query (string): The query to execute on the database.
        - params (tuple): Parameters to substitute in query to prevent SQL
        injection attacks.

        Returns: Tuple with the MySQL query results, else None if empty set.
        """
        result = ()
        conn = MySQLdb.connect(host=self.host, db=self.database, user=self.user, passwd=self.passwd)
        cursor = conn.cursor()
        cursor.execute(query, params)
        result = cursor.fetchall()
        conn.commit()
        cursor.close()
        conn.close()
        if result is None or result is ():
            return None
        else:
            return result

    def count(self, dbtable=None, conds='', options='', params=()):
        """
        This function is used to get a count of the number of rows in the
        database depending on the given conditions.

        - dbtable (string): The database table to query from.
        - conds (string): Conditions (WHERE clauses) to add.
        - options (string): Query options.
        - params (tuple): Parameters to substitute in the query.

        Returns: Int with the number of rows for a given query, None if the
        dbtable parameter is missing (which is required).
        """
        if (dbtable is None):
            return None
        else:
            output = 0
            query = [
                'SELECT', 'COUNT(*)',
                'FROM', dbtable
            ]
            if (conds != ''):
                extra = ['WHERE', conds, options]
            else:
                extra = [options]
            query.extend(extra)
            execute = ' '.join(query) + ';'
            results = self.execute(execute, params)
            if results is None:
                output = 0
            else:
                for result in results:
                    output = result[0]
            return output

    def insert(self, dbtable=None, values={}, params=()):
        """
        This function is used for inserting new rows into the database.

        - dbtable (string): The database table to query from.
        - values (dict): A dictionary with key and value pairs to insert.
        - params (tuple): Parameters to substitute in the query.

        Returns: True if insert is successful, False if an error occurred.
        """
        if (dbtable is None):
            return False
        else:
            keys = []
            vals = []
            for key, val in values.iteritems():
                keys.append(key)
                vals.append(val)

            query = [
                'INSERT INTO', dbtable,
                '(' + ', '.join(keys) + ')',
                'VALUES', '(' + ', '.join(vals) + ')'
            ]
            execute = ' '.join(query) + ';'
            try:
                self.execute(execute, params)
                return True
            except:
                return False

    def select(self, dbtable=None, columns=[], conds='', options='',
               params=()):
        """
        This function is used for obtaining information from the database.

        - dbtable (string): The database table to query from.
        - columns (dict): The column(s) to retrieve from the database.
        - conds (string): Conditions (WHERE clauses) to add.
        - options (string): Query options.
        - params (tuple): Parameters to substitute in the query.

        Note: options should contain a LIMIT clause to prevent retrieving an
        excessive amount of data from the database.

        Returns: Tuple with the SQL query results, None if empty set.
        """
        if (dbtable is None):
            return None
        else:
            query = [
                'SELECT', ', '.join(columns),
                'FROM', dbtable
            ]
            if (conds != ''):
                extra = ['WHERE', conds, options]
            else:
                extra = [options]
            query.extend(extra)
            execute = ' '.join(query) + ';'
            try:
                return self.execute(execute, params)
            except:
                return None

    def update(self, dbtable=None, values={}, conds='', params=()):
        """
        This function is used for updating information in the database. This
        is different from INSERT (see self.insert instead).

        - dbtable (string): The database table to query from.
        - values (dict): A dictionary of columns and values to update.
        - conds (string): Conditions (WHERE clauses) to add.
        - params (tuple): Parameters to substitute in the query.

        Returns: True if update is successful, False if an error occurred.
        """
        if (dbtable is None):
            return False
        else:
            vals = []
            for key, val in values.iteritems():
                vals.append('%s=%s' % (key, val))

            query = [
                'UPDATE', dbtable,
                'SET', ', '.join(vals)
            ]
            if (conds != ''):
                extra = ['WHERE', conds]
            else:
                extra = []
            query.extend(extra)
            execute = ' '.join(query) + ';'
            try:
                self.execute(execute, params)
                return True
            except:
                return False


if __name__ == "__main__":
    BALMessage = message.BALMessage()
    raise IncorrectUsage(BALMessage.getMessage('exception-incorrectusage'))
