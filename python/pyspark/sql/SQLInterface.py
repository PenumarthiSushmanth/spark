from pyspark.sql import SparkSession
import pandas as pd


spark = SparkSession.builder.appName("BigData").getOrCreate()

def custom_hive():
    try:
        while (1):
            print("Choose one option:\n1. create table 2. Insert data 3. Show table \n4. Delete table 5. Show all tables 6. Press any other key to quit")
            name = input()

            if name == "1":
                create_table()
            elif name == "2":
                insert_data()
            elif name == "3":
                show_table()
            elif name == "4":
                delete_table()
            elif name == "5":
                show_all_table()
            else:
                print("Quitting...")
                break
    except:
        print("Invalid Option!")


def create_table():
    try:
        tab_name = input('Enter the table name: ')

        if len(tab_name) == 0:
            raise Exception("Invalid Table name!")
        
        print('Enter the column name with their data type by space in between (Enter q when you are done)')
        print('Data types for reference: [int, string, float, double, boolean]')
        datatypes = ['int', 'string', 'float', 'double', 'boolean']
        col = ""
        final_col = []
        while (1):
            i = input()
            if i == 'q' and len(col) == 0:
                print('Table should contain atleast one column')
            elif i == 'q':
                break
            else:
                if i.split()[1] in datatypes:
                    final_col.append(i.split()[0])
                    col = col + i + ', '
                else:
                    print("Please enter valid datatype...")
        
        query = "CREATE TABLE " + tab_name + "("+col[0:len(col)-2]+")"

        spark.sql(query)
        print("Table created successfully!")
    except Exception as e:
        print("Exception: ", e)


def insert_data():
    try:
        tab_name = input("Enter the table name: ")
        tab_values = []
        if spark.catalog.tableExists(tab_name):
            print("Choose one option below:\n1. Enter data manually 2. Import Data from Dataset")
            opt = int(input())

            if opt == 1:
                df = spark.read.table(tab_name)
                dtype_list = df.dtypes

                for i in range(0, len(dtype_list)):
                    print("Enter the value for", dtype_list[i][0], "(", dtype_list[i][1], "): ")
                    
                    if dtype_list[i][1] == "int":
                        val = int(input())
                        tab_values.append(val)

                    elif dtype_list[i][1] == "float" or dtype_list[i][1] == "double":
                        val = float(input())
                        tab_values.append(val)

                    elif dtype_list[i][1] == "boolean":
                        val = bool(input())
                        tab_values.append(val)

                    else:
                        val = input()
                        tab_values.append(val)
                

                spark.sql(f"INSERT INTO {tab_name} VALUES {tuple(tab_values)}")
                print("Data Inserted successfully!")

            elif opt == 2:
                print("Enter the path of dataset: ")
                path_of_file = input()
                
                df = spark.read.csv(path_of_file, header=True, inferSchema=True)

                df.createOrReplaceTempView('temp_table')

                query = "INSERT INTO "+ tab_name + " SELECT * FROM temp_table"

                spark.sql(query)

                print("Data Inserted Successfully!")

            else:
                print("Invalid option!")

        else:
            print("Table does not exist.")

    except Exception as e:
        print("Excetion: ", e)   


def show_table():
    try:
        tab_name = input("Enter the table name:")
        if spark.catalog.tableExists(tab_name):
            print("Choose one option below:\n1. Show Schema 2. Show Data")
            opt = int(input())

            if opt == 1:
                df = spark.sql("SELECT * FROM "+tab_name)
                df.printSchema()
            elif opt == 2:
                df = spark.sql("SELECT * FROM "+tab_name)
                df.show()
            else:
                print("Invalid option!")
        else:
            print("Table does not exist.")
    except Exception as e:
        print("Exception: ", e)


def delete_table():
    try:
        tab_name = input("Enter the table name to delete:")
        if spark.catalog.tableExists(tab_name):
            spark.sql("DROP TABLE "+tab_name)
            print("Table Deleted succussfully!")
        else:
            print("Table does not exist.")
    except Exception as e:
        print("Exception: ", e)


def show_all_table():
    try:
        spark.sql("SHOW TABLES").show()
    except Exception as e:
        print("Exception: ", e)
