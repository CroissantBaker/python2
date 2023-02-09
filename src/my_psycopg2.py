# Preparation for this part
#   - pip install psycopg2-binary
#   - have Docker installed
#   - execute 'docker-compose up' in command line (folder of this project)
#   - https://www.psycopg.org/docs/

import psycopg2
import pandas as pd
import pandas.io.sql as sqlio

# Connect to your postgres DB
conn = psycopg2.connect(
    dbname="python_course", user="postgres", password="example", host="localhost"
)

# Open a cursor to perform database operations
cur = conn.cursor()

sql_create_table = """CREATE TABLE transactions (
            inv_id SERIAL PRIMARY KEY,
            InvoiceNo VARCHAR(255),
            StockCode VARCHAR(255),
            Description VARCHAR(255),
            Quantity real,
            InvoiceDate VARCHAR(255),
            UnitPrice real,
            CustomerID integer,
            Country VARCHAR(255)
        )"""

# Execute a query to create the table
# cur.execute("""DROP table transactions""")
try:
    cur.execute(sql_create_table)
except psycopg2.errors.DuplicateTable:
    print("Table already exists.")
except:
    print("CHECK connection from the beginning.")    

conn.commit()

# Execute the copy command to bulk load in data
copy_command = """COPY transactions(InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country) FROM '/mnt/data/data.csv' CSV HEADER;"""
cur.execute(copy_command)
conn.commit()
# query specific invoice
query_invoice = """Select * from transactions where InvoiceNo = '536370'"""

# get the results
cur.execute(query_invoice)
records = cur.fetchall()
# print the results
print(records)
# print(records[0])

# get results into pandas dataframe
data = sqlio.read_sql_query(query_invoice, conn)
print("#############################")
print(data)
#     inv_id invoiceno stockcode                          description  quantity     invoicedate  unitprice  customerid country
# 0       27    536370     22728            ALARM CLOCK BAKELIKE PINK      24.0  12/1/2010 8:45       3.75       12583  France
# 1       28    536370     22727            ALARM CLOCK BAKELIKE RED       24.0  12/1/2010 8:45       3.75       12583  France
# 2       29    536370     22726           ALARM CLOCK BAKELIKE GREEN      12.0  12/1/2010 8:45       3.75       12583  France
# 3       30    536370     21724      PANDA AND BUNNIES STICKER SHEET      12.0  12/1/2010 8:45       0.85       12583  France
# 4       31    536370     21883                     STARS GIFT TAPE       24.0  12/1/2010 8:45       0.65       12583  France
# 5       32    536370     10002          INFLATABLE POLITICAL GLOBE       48.0  12/1/2010 8:45       0.85       12583  France
# 6       33    536370     21791   VINTAGE HEADS AND TAILS CARD GAME       24.0  12/1/2010 8:45       1.25       12583  France
# 7       34    536370     21035      SET/2 RED RETROSPOT TEA TOWELS       18.0  12/1/2010 8:45       2.95       12583  France
# 8       35    536370     22326  ROUND SNACK BOXES SET OF4 WOODLAND       24.0  12/1/2010 8:45       2.95       12583  France
# 9       36    536370     22629                  SPACEBOY LUNCH BOX       24.0  12/1/2010 8:45       1.95       12583  France
# 10      37    536370     22659              LUNCH BOX I LOVE LONDON      24.0  12/1/2010 8:45       1.95       12583  France
# 11      38    536370     22631             CIRCUS PARADE LUNCH BOX       24.0  12/1/2010 8:45       1.95       12583  France
# 12      39    536370     22661      CHARLOTTE BAG DOLLY GIRL DESIGN      20.0  12/1/2010 8:45       0.85       12583  France
# 13      40    536370     21731        RED TOADSTOOL LED NIGHT LIGHT      24.0  12/1/2010 8:45       1.65       12583  France
# 14      41    536370     22900      SET 2 TEA TOWELS I LOVE LONDON       24.0  12/1/2010 8:45       2.95       12583  France
# 15      42    536370     21913       VINTAGE SEASIDE JIGSAW PUZZLES      12.0  12/1/2010 8:45       3.75       12583  France
# 16      43    536370     22540           MINI JIGSAW CIRCUS PARADE       24.0  12/1/2010 8:45       0.42       12583  France
# 17      44    536370     22544                 MINI JIGSAW SPACEBOY      24.0  12/1/2010 8:45       0.42       12583  France
# 18      45    536370     22492              MINI PAINT SET VINTAGE       36.0  12/1/2010 8:45       0.65       12583  France
# 19      46    536370      POST                              POSTAGE       3.0  12/1/2010 8:45      18.00       12583  France
# 20  541935    536370     22728            ALARM CLOCK BAKELIKE PINK      24.0  12/1/2010 8:45       3.75       12583  France
# 21  541936    536370     22727            ALARM CLOCK BAKELIKE RED       24.0  12/1/2010 8:45       3.75       12583  France
# 22  541937    536370     22726           ALARM CLOCK BAKELIKE GREEN      12.0  12/1/2010 8:45       3.75       12583  France
# 23  541938    536370     21724      PANDA AND BUNNIES STICKER SHEET      12.0  12/1/2010 8:45       0.85       12583  France
# 24  541939    536370     21883                     STARS GIFT TAPE       24.0  12/1/2010 8:45       0.65       12583  France
# 25  541940    536370     10002          INFLATABLE POLITICAL GLOBE       48.0  12/1/2010 8:45       0.85       12583  France
# 26  541941    536370     21791   VINTAGE HEADS AND TAILS CARD GAME       24.0  12/1/2010 8:45       1.25       12583  France
# 27  541942    536370     21035      SET/2 RED RETROSPOT TEA TOWELS       18.0  12/1/2010 8:45       2.95       12583  France
# 28  541943    536370     22326  ROUND SNACK BOXES SET OF4 WOODLAND       24.0  12/1/2010 8:45       2.95       12583  France
# 29  541944    536370     22629                  SPACEBOY LUNCH BOX       24.0  12/1/2010 8:45       1.95       12583  France
# 30  541945    536370     22659              LUNCH BOX I LOVE LONDON      24.0  12/1/2010 8:45       1.95       12583  France
# 31  541946    536370     22631             CIRCUS PARADE LUNCH BOX       24.0  12/1/2010 8:45       1.95       12583  France
# 32  541947    536370     22661      CHARLOTTE BAG DOLLY GIRL DESIGN      20.0  12/1/2010 8:45       0.85       12583  France
# 33  541948    536370     21731        RED TOADSTOOL LED NIGHT LIGHT      24.0  12/1/2010 8:45       1.65       12583  France
# 34  541949    536370     22900      SET 2 TEA TOWELS I LOVE LONDON       24.0  12/1/2010 8:45       2.95       12583  France
# 35  541950    536370     21913       VINTAGE SEASIDE JIGSAW PUZZLES      12.0  12/1/2010 8:45       3.75       12583  France
# 36  541951    536370     22540           MINI JIGSAW CIRCUS PARADE       24.0  12/1/2010 8:45       0.42       12583  France
# 37  541952    536370     22544                 MINI JIGSAW SPACEBOY      24.0  12/1/2010 8:45       0.42       12583  France
# 38  541953    536370     22492              MINI PAINT SET VINTAGE       36.0  12/1/2010 8:45       0.65       12583  France
# 39  541954    536370      POST                              POSTAGE       3.0  12/1/2010 8:45      18.00       12583  France
# close communication with the PostgreSQL database server
cur.close()
# commit the changes
conn.commit()
