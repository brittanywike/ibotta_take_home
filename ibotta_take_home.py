import sqlite3
from sqlite3 import Error
from sqlite3 import dbapi2 as sqlite
import csv

#Testing Python connection
def main():

    print("Hello Ibotta Take Home!")

#Run main
if __name__ == '__main__':
    main()

# PLEASE DESCRIBE -
# Creates file for sqlite db_file if it doesn't exist already and connects to the db
def create_connection(db_file):
    conn = None
    # Tries to connect to our db, if it fails it will print out the error in arguement e
    try:
        conn = sqlite3.connect(db_file)
        print("Connected")
    except Error as e:
        print(e)

    return conn

# Set conn as variable to be reused and calls function
conn = create_connection("ibotta.db")

# PLEASE DESCRIBE - This function loads our CSV into the DB with 3 arguments:
# connection, CSV file, and table name to insert into
def loadcsv(conn, file_name, tbl_name):
    # Sets our CSV file_name as CSV file variable
    csv_file = open(file_name)
    # Reads the CSV into a dictionary
    csv_reader = csv.DictReader(csv_file)
    # Insert statement for inserting the values into specified table name
    # .Join(csv_reader.fieldnames) concatenates the field names from CSV separated by commas into proper SQL insert statement format
    # .Join(['?'] * len(csv_reader.fieldnames creates placeholders of ? separated by commas for the # of field names for proper SQL insert statement format
    insert_sql = 'INSERT INTO ' + tbl_name + ' (' + ','.join(csv_reader.fieldnames) + ') VALUES (' + ','.join(['?'] * len(csv_reader.fieldnames))+ ')'
    #Prints our statement to the console
    print(insert_sql)
    # PLEASE DESCRIBE -
    # For each row in the CSV, insert the row_values into a list called values
    values = []
    for datarow in csv_reader:
        # For each field insert the values into a list called row_values which is used by values
        row_values = []
        for field in csv_reader.fieldnames:
            row_values.append(datarow[field])
        values.append(row_values)
    # Execute many executes the insert_sql and values above
    conn.executemany(insert_sql, values)
    # Saves our changes
    conn.commit()

# Calling the loadcsv function for our 4 files into their respective tables
loadcsv(conn, "customer_offers_296332.csv", "customer_offers")
loadcsv(conn, "offer_rewards_168083.csv", "offer_rewards")
loadcsv(conn, "customer_offer_rewards_144392.csv", "customer_offer_rewards")
loadcsv(conn, "customer_offer_redemptions_31025.csv", "customer_offer_redemptions")

#Returns list of column names from the specified table
def db_getinfo(conn, tbl_name):
    #Cursors allow us to execute SQLITE statements
    cur = conn.cursor()
    cur.execute("SELECT * FROM " + tbl_name)
    #Tuple[0] adds column names only to the list
    column_name_list = [tuple[0] for tuple in cur.description]

    return column_name_list

# Set column_names as variable to be printed and calls function
column_names = db_getinfo(conn, "customer_offers")
print column_names

# PLEASE DESCRIBE -
# Executes specified SQL query and prints the rows as well as prints the count of the rows
def db_query(conn, query):
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()

        for row in rows:
            print(row)

        print len(rows)

# Calling db_query function
db_query(conn, "SELECT * FROM customer_offers")
