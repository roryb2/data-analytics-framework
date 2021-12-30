# -------------------------------------------------------------------------------------
# Created By: Rory Barrett
# Created Date: 2021-12-29
# version: 1.24
# -------------------------------------------------------------------------------------

import pandas as pd
import pyodbc
import os
import msaccessdb
import csv
import re

class DataAnalytics:
    """
    Toolkit for managing and manipulating pandas dataframes (referred to as tables).
    Contains operations for creating analytics processes.

    ...

    Attributes
    ----------
    db : dict
        dictionary for managing access to dataframes
    tblName : str
        key of dataframe currently being accessed from db
    context : pandas.dataframe
        dataframe currently being accessed from db

    Methods
    -------
    wd()
        Returns the location of the working directory
    explore()
        Returns a dataframe with all keys in attribute db
    loadProject()
        Returns a dictionary containing all key-value pairs of tablename-dataframe from the working directory
    saveall()
        Writes the current dataframes in attribute db to file
    add(tblName, df, open=True)
        Adds a new tablename-dataframe to attribute db
    delete(tblName)
        Removes a tablename-dataframe from attribute db
    open(tblName)
        "Opens" a dataframe from attribute db
    close()
        "Closes" the dataframe currently "open"
    extract(tblName, filter=None, open=True, cols = None)
        Creates new tablename-dataframe pair from dataframe currently "open"
    append(tblName, tbl2)
        Creates new tablename-dataframe pair by appending a dataframe to the dataframe currently "open"
    filter(condition)
        Apply set filter to dataframe curently "open"
    exportFile(format, sep, filename=None)
        Export "open" dataframe to a selected format
    exportMDB(filename=None, tbl=None)
        Export "open" dataframe to MDB
    createAccessMDB(path=None, filename=None)
        Return MDB file location for use in method exportMDB
    SQL_CREATE_STATEMENT_FROM_DATAFRAME(SOURCE, TARGET)
        Return string with SQL create statement for a given dataframe
    SQL_INSERT_STATEMENT_FROM_DATAFRAME(SOURCE, TARGET)
        Return string with SQL insert statement(s) for a given dataframe
    addCol(colName, eqn)
        Return dataframe with column added for supplied lambda function to dataframe currently "open"
    renameCol(**kwargs)
        Return dataframe with newly renamed columns from dataframe currently "open"
    summBy(tblName,cols,agg_funcs=None,open=True)
        Return dataframe resulting from aggregations performed on dataframe currently "open"
    importSQL(cxn, table=None, query=None, open=True,tblName=None)
        Creates new dataframe from provided SQL statement on a given database connection
    importFile(filename, sep, tblName=None)
        Creates new dataframe from a given delimited text file
    importExcel(filename, sheet=0, tblName=None)
        Creates new dataframe from a given excel file
    join(tblName, right, how='inner', on=None, left_on=None, right_on=None, left_index=False, right_index=False, sort=False, suffixes=('_x', '_y'), copy=True, indicator=False, validate=None)
        Creates new dataframe by performing a join between a dataframe and the dataframe currently "open"
    drivers()
        Return a list of available database drivers
    """

    csv = 'csv';mdb = 'mdb';txt = 'txt';sql = 'sql'; data_format = '.das'

    def __init__(self):
        self.db = self.loadProject()
        self.tblName = None
        self.context = None
    
    def __str__(self):
        return str(self.db.keys())

    def __repr__(self):
        #return 'Tables \n' + '\n'.join(self.db.keys())
        return repr(self.explore())

    @staticmethod
    def wd():
        return os.path.abspath(os.getcwd())

    # Explore: View tables in database
    def explore(self):
        return pd.DataFrame(list(self.db.keys()),columns=['Table Name'])

    def loadProject(self):
        tmp_dict = {}
        for file in os.listdir():
            if file.endswith(self.data_format):
                tmp_dict[os.path.splitext(file)[0]] = pd.read_feather(file)
        return tmp_dict

    def saveall(self):
        for k in self.db:
            self.db[k].reset_index(drop=True).to_feather(k + self.data_format)
            # print('\'{}\' was saved to \'{}\''.format(k,self.wd() + k + self.data_format))

    # Add: Add table to database
    def add(self, tblName, df, open=True):
        filename = tblName + self.data_format
        df.reset_index(drop=True).to_feather(filename)
        self.db[tblName] = df
        if open:
            self.open(tblName)
    
    # Delete: Delete table from database
    def delete(self, tblName):
        if self.tblName == tblName:
            print("Cannot delete open table '{}'".format(tblName))
        else:
            try:
                del self.db[tblName]
                if os.path.exists(tblName + self.data_format):
                    os.remove(tblName + self.data_format)
            except KeyError:
                print("Table \'{}\' does not exist.".format(tblName))

    # Open: Open table in database
    def open(self, tblName):
        try:
            self.tblName = tblName
            self.context = self.db[self.tblName]
            return self.context
        except KeyError:
            print("Table \'{}\' does not exist.".format(tblName))
    
    # Close: Close the current table
    def close(self):
        self.context = None
        self.tblName = None
    
    # Extract: Create a separate table from open table
    def extract(self, tblName, filter=None, open=True, cols = None):
        if not self.context.empty:
            if cols:
                self.context = self.context[cols]
            if(filter != None):
                self.add(tblName,self.filter(filter))
            else:
                self.add(tblName,self.context)
        else:
            self.add(tblName,self.context)
        if open:
            self.open(tblName)
            
    def append(self, tblName, tbl2):
        self.add(tblName,self.context.append(tbl2, ignore_index=True))
        
    # Filter: Define a series of conditions or criteria and apply to dataframe for results
    def filter(self, condition):
        if not self.context.empty:
            return self.context.query(condition)
        else:
            return self.context

    # Export: Export dataframe values to a supported specific file format
    def exportFile(self, format, sep, filename=None):
        if not self.context.empty:
            if filename==None:
                filename = self.tblName

            self.context.to_csv(filename + '.' + format, index=False, sep=sep, quoting = csv.QUOTE_ALL)

    # Export MDB: Export dataframe values to MDB
    def exportMDB(self, filename=None, tbl=None):
        # Identify MS Access Drivers
        # [x for x in pyodbc.drivers() if x.startswith('Microsoft Access Driver')]
        if not self.context.empty:
            if filename==None:
                filename = self.tblName

            df = self.context
            mdb_file = self.createAccessMDB(filename=filename)
            conn = pyodbc.connect('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; DBQ=' + mdb_file)
            # conn = pyodbc.connect('DRIVER={SQL Server}; DBQ=' + mdb_file)
            cur = conn.cursor()
            tbl_temp = 'Reports' if not tbl else tbl
            if cur.tables(table = tbl_temp).fetchone():
                cur.execute('DROP TABLE ' + tbl_temp)
            create_st = self.SQL_CREATE_STATEMENT_FROM_DATAFRAME(df,tbl_temp)

            print(create_st)

            cur.execute(create_st)
            conn.commit()

            date_cols = list(df.select_dtypes(['<M8[ns]']).columns)

            if date_cols:
                df[date_cols] = df[date_cols].astype(str)

            """
            insert_st = self.SQL_INSERT_STATEMENT_FROM_DATAFRAME(df,tbl)
            insert_st = [re.sub(r"\bnan\b",'NULL',ln) for ln in insert_st]
            insert_st = [re.sub(r"\bNaT\b",'NULL',ln) for ln in insert_st]
            insert_st = [re.sub(r"\bNone\b",'NULL',ln) for ln in insert_st]
            """
            cols = list(df.columns)
            prms = ['['+r+']' for r in cols]
            cols = ','.join('?' for i in range(len(cols)))
            prms = ','.join(i for i in prms)
            vals = df.values.tolist()

            sql = 'INSERT INTO '+tbl+' (%s)' % prms
            sql = sql+' VALUES (%s)' % cols
            for row in vals:
                vals = [x if x not in ['nan',None] else '' for x in list(row)]
                vals = [x if x not in ['NaT'] else None for x in list(vals)]
                vals = tuple(vals)
                print(sql)

                print(vals)
                cur.execute(sql,vals)

            conn.commit()
            cur.close()
            conn.close()

    def createAccessMDB(self, path=None, filename=None):
        ext = self.mdb
        if path == None:
            path = self.wd()
        
        if filename == None:
            filename = self.tblName
        
        db_file = path + '\\' + filename + '.' + ext 
        msaccessdb.create(db_file)
        return db_file

    # SQL_CREATE_STATEMENT_FROM_DATAFRAME(SOURCE, TARGET)
    # SOURCE: source dataframe
    # TARGET: target table to be created in database
    def SQL_CREATE_STATEMENT_FROM_DATAFRAME(self, SOURCE, TARGET):
        sql_text = pd.io.sql.get_schema(SOURCE, TARGET)   
        return sql_text

    def SQL_INSERT_STATEMENT_FROM_DATAFRAME(self, SOURCE, TARGET):
        sql_texts = []
        for index, row in SOURCE.iterrows():
            # sql_texts.append('INSERT INTO '+TARGET+' ('+ str(', '.join(SOURCE.columns))+ ') VALUES '+ str(tuple(row.values)))
            sql_texts.append('INSERT INTO '+TARGET+' ('+ str(', '.join([re.sub('$',']',re.sub('^','[', el)) for el in list(SOURCE.columns)]))+ ') VALUES '+ str(tuple(row.values)))        
        return sql_texts

    def addCol(self, colName, eqn):
        if not self.context.empty:
            self.db[self.tblName][colName] = self.db[self.tblName].apply(eqn, axis=1)
            # self.context = self.db[self.tblName]
            return self.open(self.tblName)

    def renameCol(self, **kwargs):
        self.db[self.tblName] = self.db[self.tblName].rename(**kwargs)
        return self.open(self.tblName)

    def summBy(self,tblName,cols,agg_funcs=None,open=True):
        if not self.context.empty:
            if not agg_funcs:
                tmp = self.context.groupby(cols, as_index=False).size()
            else:
                tmp = self.context.groupby(cols).agg(agg_funcs)
                tmp.columns = ["_".join(x) for x in tmp.columns.ravel()]
                tmp = tmp.reset_index()
            self.add(tblName, tmp,open=open)
            if open:
                return self.open(tblName)
        
    def importSQL(self,cxn, table=None, query=None, open=True,tblName=None):

        if not tblName:
            tblName = table
        if not query:
            query = 'SELECT * FROM ' + table
            # Query into dataframe

        self.add(tblName,pd.io.sql.read_sql(query, cxn))

    def importFile(self, filename, sep, tblName=None):
        if not tblName:
            tblName = filename
        
        self.add(tblName, pd.read_csv(filename,sep = sep))

    def importExcel(self, filename, sheet=0, tblName=None):
        if not tblName:
            tblName = os.path.basename(filename)
     
        self.add(tblName, pd.read_excel(filename, sheet_name = sheet, engine = 'openpyxl'))

    def join(self, tblName, right, how='inner', on=None, left_on=None, right_on=None, left_index=False, right_index=False, sort=False, suffixes=('_x', '_y'), copy=True, indicator=False, validate=None):
        if not self.context.empty:
            self.add(tblName, self.context.merge(right, how, on, left_on, right_on, left_index, right_index, sort, suffixes, copy, indicator, validate))

    @staticmethod
    def drivers():
        drivers = pyodbc.drivers()
        for driver in drivers:
            print(driver)
