#import pandas library to handle data
import pandas as pd

#import pyodbc to connect with database
import pyodbc

#load customer,product,sales data from csv files

customer_df=pd.read_csv('customers.csv')
print(customer_df.columns)
sales_df=pd.read_csv('sales_data.csv')
print(sales_df.columns)
product_df=pd.read_csv('products.csv')
print(product_df.columns)

#check for the missing values
print(sales_df['customer_id'].isna().sum())
print(customer_df['customer_id'].isna().sum())
print(sales_df['product_id'].isna().sum())
print(product_df['product_id'].isna().sum())

#print unique values in merge columns
print(sales_df['customer_id'].unique())
print(customer_df['customer_id'].unique())
print(sales_df['product_id'].unique())
print(product_df['product_id'].unique())

#merge sales data with the customer data

customer_sales=pd.merge(customer_df,sales_df, on='customer_id',how='left')
print(customer_sales)

#merge customer sales data with product data

full_data=pd.merge(customer_sales,product_df,on='product_id',how='left')
print(full_data.columns)

#Fill the missing values with zero

full_data.fillna(0,inplace=True)

#print sale_date
print(full_data['sale_date'].head())

#convert the date to the correct format

full_data['sale_date']=pd.to_datetime(full_data['sale_date'],format='%d-%m-%Y', errors='coerce')

#Drop the rows where the date cannot be converted

full_data=full_data.dropna(subset=['sale_date'])

#print the final data
print(full_data.head())
print(full_data['sale_date'])

#connect to ms sql using

conn=pyodbc.connect('DRIVER={SQL Server};'
                    'SERVER=NAVEEN_DEVARA;'
                    'DATABASE=sales;'
                    'Trusted_Connection=yes;')

#create a cursor to interact with the database
cursor=conn.cursor()

#Check if the table exists

cursor.execute('''
SELECT COUNT(*)
FROM information_schema.tables
where table_name='all_data'
''')

#if table  exists print 1 else 0
table_exists=cursor.fetchone()[0]

#Create a table if it doesn't exists

print(full_data['customer_email'].apply(len).max())

if not table_exists:
    cursor.execute('''
    CREATE TABLE all_data(
    customer_id INT,
    customer_name VARCHAR(250),
    customer_email VARCHAR(250),
    sale_id INT PRIMARY KEY,
    product_id INT,
    sales_amount FLOAT,
    sale_date DATE,
    product_name VARCHAR(250),
    product_category VARCHAR(250)
    )
''')
    conn.commit()


#insert the data into the SQL table

for index, row in full_data.iterrows():
    try:
        cursor.execute('''
        INSERT INTO all_data(customer_id, customer_name,customer_email,sale_id,product_id,sales_amount,sale_date,product_name,product_category)
        VALUES(?,?,?,?,?,?,?,?,?)
        ''',
row['customer_id'], row['customer_name'], row['customer_email'], row['sale_id'],
        row['product_id'], row['sales_amount'], row['sale_date'], row['product_name'], row['product_category'])
    except pyodbc.IntegrityError as e:
        print(f"Error inserting row{index}:{e}")

conn.commit()
cursor.close()
conn.close()
