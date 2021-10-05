import pandas as pd
import pyodbc
import os
import msaccessdb

# 

class DataAnalytics:
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
                self.add(tblName, self.context)
        else:
            self.add(tblName,self.context)
        if open:
            self.open(tblName)
            
    def append(self, tblName, tbl2):
        self.add(tblName,self.context.append(tbl2))
        
    # Filter: Define a series of conditions or criteria and apply to dataframe for results
    def filter(self, condition):
        if not self.context.empty:
            return self.context.query(condition)
        else:
            return self.context

    # Export: Export dataframe values to a supported specific file format
    def export(self, format, filename=None):
        # Identify MS Access Drivers
        # [x for x in pyodbc.drivers() if x.startswith('Microsoft Access Driver')]
        if not self.context.empty:
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

    # SQL_CREATE_STATEMENT_FROM_DATAFRAME(SOURCE, TARGET)
    # SOURCE: source dataframe
    # TARGET: target table to be created in database
    def SQL_CREATE_STATEMENT_FROM_DATAFRAME(self, SOURCE, TARGET):
        sql_text = pd.io.sql.get_schema(SOURCE, TARGET)   
        return sql_text

    def SQL_INSERT_STATEMENT_FROM_DATAFRAME(self, SOURCE, TARGET):
        sql_texts = []
        for index, row in SOURCE.iterrows():
            sql_texts.append('INSERT INTO '+TARGET+' ('+ str(', '.join(SOURCE.columns))+ ') VALUES '+ str(tuple(row.values)))        
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
