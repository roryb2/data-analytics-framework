import pandas as pd
import numpy as np
import pickle
import pyodbc
import os
import msaccessdb

class DataAnalytics:
    csv = 'csv';mdb = 'mdb';txt = 'txt';sql = 'sql'

    def __init__(self):
        self.db = {}
        self.context = None
        self.tblName = None
    
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

    # Add: Add table to database
    def add(self, tblName, df, open=True):
        self.db[tblName] = df
        if open:
            self.open(tblName)
    
    # Delete: Delete table from database
    def delete(self, tblName):
        if self.context == self.db[tblName]:
            print("Cannot delete open table '{}'".format(tblName))
        else:
            try:
                del self.db[tblName]
            except KeyError:
                print("Table \'{}\' does not exist.".format(tblName))

    # Open: Open table in database
    def open(self, tblName):
        try:
            self.context = self.db[tblName]
            self.tblName = tblName
            return self.db[tblName]
        except KeyError:
            print("Table \'{}\' does not exist.".format(tblName))
    
    # Close: Close the current table
    def close(self):
        self.context = None
    
    # Extract: Create a separate table from open table
    def extract(self, tblName, condition=None, open=True):
        if(condition != None):
            self.add(tblName,self.filter(condition))
        else:
            self.add(tblName,self.context)
        if open:
            self.open(tblName)
            

    # Filter: Define a series of conditions or criteria and apply to dataframe for results
    def filter(self, condition):
        return self.context.query(condition)

    # Export: Export dataframe values to a supported specific file format
    def export(self, format, filename=None):
        # Identify MS Access Drivers
        # [x for x in pyodbc.drivers() if x.startswith('Microsoft Access Driver')]
        if filename==None:
            filename = self.tblName

        if format == self.csv:
            self.context.to_csv(filename + '.csv',index=False)

        if format == self.mdb:
            mdb_file = self.createAccessMDB(filename=filename)
            conn = pyodbc.connect('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; DBQ=' + mdb_file)
            cur = conn.cursor()
            tbl = 'new'
            if cur.tables(table = tbl).fetchone():
                cur.execute('DROP TABLE ' + 'new')

            cur.execute(self.SQL_CREATE_STATEMENT_FROM_DATAFRAME(self.context,tbl))

            for sql in self.SQL_INSERT_STATEMENT_FROM_DATAFRAME(self.context,tbl):
                cur.execute(sql)

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

    def SQL_CREATE_STATEMENT_FROM_DATAFRAME(self, SOURCE, TARGET):

    # SQL_CREATE_STATEMENT_FROM_DATAFRAME(SOURCE, TARGET)
    # SOURCE: source dataframe
    # TARGET: target table to be created in database

        sql_text = pd.io.sql.get_schema(SOURCE, TARGET)   
        return sql_text

    def SQL_INSERT_STATEMENT_FROM_DATAFRAME(self, SOURCE, TARGET):
        sql_texts = []
        for index, row in SOURCE.iterrows():
            sql_texts.append('INSERT INTO '+TARGET+' ('+ str(', '.join(SOURCE.columns))+ ') VALUES '+ str(tuple(row.values)))        
        return sql_texts

    def addCol(self, colName, val):
        self.context[colName] = self.context.eval(val)
        return self.context

    def summBy(self,cols,agg_funcs=None):
        if not agg_funcs:
            return self.context.groupby(cols, as_index=False).size()
        
    def sqlCxn(self,driver,server,db,UID,pw=None):
        if not pw:
            pw = UID
        return pyodbc.connect(
                'DRIVER={' + driver + '};SERVER=' + server + 
                ';DATABASE=' + db + 
                '; UID = ' + UID + 
                '; PWD = ' + UID + 'Trusted_Connection=yes')

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

    def importExcel(self, filename, sheet=None, tblName=None):
        if not tblName:
            tblName = filename
        
        if not sheet:
            self.add(tblName, pd.read_excel(filename, engine = 'openpyxl'))
        
        self.add(tblName, pd.read_excel(filename, sheet_name = sheet, engine = 'openpyxl'))
    
    @staticmethod
    def available_drivers():
        drivers = pyodbc.drivers()
        for driver in drivers:
            print(driver)
