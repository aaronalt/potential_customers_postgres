import psycopg2
import pandas as pd 
import numpy as np


#DATABASE_URL=$(heroku config:get DATABASE_URL -a brn-memory-bank) 
databasename = "d50qgfooclm179"
username = "snsrflpvwjeibk"
password = "720a6e3c99040c66d48e52425aa46ba126ffff5a3237ebe38a233dd69cc97659"
host = "ec2-46-137-170-51.eu-west-1.compute.amazonaws.com"
port = "5432"

def create_customers_table():

        """
        connects with Heroku Postgres database, creates a table for storing data relating to potential and current customers, and allows the user to store info 
        """

        heroku_conn = psycopg2.connect(dbname=databasename,user=username,password=password,host=host,port=port)

        cursor = heroku_conn.cursor()
        
        create_table = "CREATE TABLE IF NOT EXISTS potential_customers (index INTEGER,customer_name TEXT PRIMARY KEY, country TEXT, status TEXT, CompanyType TEXT, address TEXT, url TEXT, email TEXT, phone VARCHAR,source TEXT, contacted_y_n CHAR, contact_date VARCHAR,contact_results TEXT, notes TEXT);"

        cursor.execute(create_table)

        heroku_conn.commit()

        cursor.close()
        heroku_conn.close()
        return


def store_customers_in_table():

        heroku_conn = psycopg2.connect(dbname=databasename,user=username,password=password,host=host,port=port)
        cursor = heroku_conn.cursor()

        sheet='https://docs.google.com/spreadsheets/d/1qGnU-OE4mcVf-Gnc1iINpx2pqH5komEWk1_9shmX6nY/export?format=csv&id=1qGnU-OE4mcVf-Gnc1iINpx2pqH5komEWk1_9shmX6nY'

        df1 = pd.read_csv(sheet, engine='python',header=0, delimiter=",", error_bad_lines=False)

        to_drop = ['Company Size','Products','SalesRep','BRN potential(y/n)','lat','lon']
        df1.drop(to_drop,inplace=True,axis=1)           # drop unwanted columns
        df = df1.replace(np.nan, '', regex=True)        # replace NaN values with empty string
        
        a = []
        for x in range(len(df)):
                customer_name     = df.iloc[x,0]
                country     = df.iloc[x,1]
                status      = df.iloc[x,2]
                CompanyType = df.iloc[x,3]
                address     = df.iloc[x,4]
                url         = df.iloc[x,5]
                email       = df.iloc[x,6]
                phone       = df.iloc[x,7]
                source      = df.iloc[x,8]
                contacted_y_n = df.iloc[x,9]
                contact_date = df.iloc[x,10]
                contact_results  = df.iloc[x,11]
                notes       = df.iloc[x,12]

                a.append({
                        'country':country,
                        'customer_name':customer_name, 
                        'CompanyType'  :CompanyType, 
                        'status' :status, 
                        'url':url, 
                        'address':address, 
                        'phone'  :phone, 
                        'email'  :email, 
                        'source' :source,
                        'contacted_y_n' :contacted_y_n,
                        'contact_date' :contact_date,
                        'contact_results' :contact_results,
                        'notes' :notes
                        })
        
        b = pd.DataFrame(a)
        b['customer_namecu'] = b['customer_name'].str.replace(',','')
        b['country'] = b['country'].str.replace(',','')
        b['url'] = b['url'].str.rstrip('/')
        b['address'] = b['address'].str.replace(',','')
        b['address'] = b['address'].str.replace('\\n',' ') 
        b['phone'] = b['phone'].str.replace(';','')
        b['phone'] = b['phone'].str.replace(',','')
        b['phone'] = b['phone'].str.replace('\\n',' ')
        b['email'] = b['email'].str.replace(',','')
        b['email'] = b['email'].str.replace('\\n',' ')
        b['contact_results'] = b['contact_results'].str.replace(';','.')
        b['contact_results'] = b['contact_results'].str.replace(',','.')
        b['notes'] = b['notes'].str.replace(';','.')
        b['notes'] = b['notes'].str.replace(',','')
        
        b.to_csv('pot-cust.csv', columns=['customer_name','country','status','CompanyType','address','url','email','phone','source','contacted_y_n','contact_date','contact_results','notes'])
       
        with open('pot-cust.csv','r') as file:
                next(file)
                cursor.copy_from(file, 'potential_customers', sep=',')

        heroku_conn.commit()
        cursor.close()
        heroku_conn.close()
        return

print("creating new table")
try:
        create_customers_table()
        print("new table created")
except Exception as e:
        print(e)
        
print("Preparing to load \'potential_customers\' with data")
try:        
        store_customers_in_table()
        print("finished")
except Exception as e:
        print(e)





"""
2/3/19:
- credentials are hard-coded...needs to be fixed
- b dataframe has unecessary amount of fixes, could be more efficient 

"""


