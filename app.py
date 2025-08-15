import pandas as pd
import sqlite3


customers_df=pd.read_csv('customers.csv')
products_df = pd.read_csv('products.csv')
orders_df = pd.read_csv('orders.csv')


# print("Customers Data:")
# print(customers_df)
# print("\nProducts Data:")
print(products_df)
# print("\nOrders Data:")
# print(orders_df)


# एक इन-मेमोरी SQLite डेटाबेस से कनेक्ट करें
# ":memory:" का मतलब है कि डेटाबेस सिर्फ़ RAM में रहेगा और प्रोग्राम बंद होने पर मिट जाएगा।
conn=sqlite3.connect(':memory:')


# Pandas DataFrames को SQL टेबल्स में कन्वर्ट करें
# index=False यह सुनिश्चित करता है कि DataFrame का इंडेक्स एक कॉलम के रूप में सेव न हो


customers_df.to_sql('customers',conn,index=False)
products_df.to_sql('products', conn, index=False)
orders_df.to_sql('orders', conn, index=False)



print("--- Task 1: Top 3 Selling Products ---")


query_top_products = """
SELECT p.name, SUM(o.quantity) AS total_quantity
FROM orders o
INNER JOIN products p ON o.product_id = p.product_id
GROUP BY p.name
ORDER BY total_quantity DESC
LIMIT 3;
"""

result_top_products=pd.read_sql_query(query_top_products,conn)
print(result_top_products)
print("\n")



# Total sales
print("--- Task 2: Total Sales by City ---")

query_sales_by_city="""
select c.city,sum(p.price*o.quantity) as total_sales
from orders o
inner join customers  c on o.customer_id=c.customer_id
inner join products  p on o.product_id=p.product_id
group by c.city
order by total_sales desc;
"""

result_sales_by_city=pd.read_sql_query(query_sales_by_city,conn)
print(result_sales_by_city)
print("\n")


print("--- Task 3: Top Spending Customer ---")
query_top_customer = """
select c.name,sum(p.price*o.quantity)as total_spent
from orders o
inner join customers c on o.customer_id=c.customer_id
inner join products p on o.product_id=p.product_id
group by c.name
order by total_spent desc
limit 1
 """

result_top_customer = pd.read_sql_query(query_top_customer, conn)
print(result_top_customer)
print("\n")

# डेटाबेस कनेक्शन बंद करें
conn.close()