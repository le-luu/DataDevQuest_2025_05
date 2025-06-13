import pandas as pd
import time
import pantab as pt
from tableauhyperapi import HyperProcess, Connection, Telemetry, TableDefinition, SqlType, TableName, Inserter, CreateMode, Nullability, HyperException
from pathlib import Path
from config import parent_path

#Pantab
#write multi tables to a hyper file by Pantab
def write_multi_tables_to_hyper_pantab(dictionary_table, output_pantab):
	#Use function frames_to_hyper to write 2 tables from dictionary_table to a specified hyper file in output pantab
    pt.frames_to_hyper(dictionary_table,output_pantab)

	#Applied 2 SQL queries to print out the number of rows after wrote each table
    query1 = """
        SELECT COUNT(*)
        FROM "OrderList"
    """
	#use frame_from_hyper_query to retrieve data from SQL query
    orderList_row = pt.frame_from_hyper_query(output_pantab, query1)
	# print out the first index value of the count column
    print(f"\n=> {orderList_row['count'][0]} Rows were WRITTEN to OrderList table")
    
    query2 = """
        SELECT COUNT(*)
        FROM "Actuals"
    """
    actuals_row = pt.frame_from_hyper_query(output_pantab,query2)
    print(f"=> {actuals_row['count'][0]} Rows were WRITTEN to Actuals table")
	#print out where the output is located
    print(f"=> Located at: {output_pantab}")

#Retrieve data from SQL query
def read_sql_query_pantab(output_pantab):
	print("Write a simple SQL query to find total unit and avg weight of location site")
	try:
		#Assign a query with JOIN and SUM, AVG aggregation function
		query = """
			SELECT a."Location Site Name", SUM(o."Unit quantity"), AVG(o."Weight")
			FROM "OrderList" AS o
			JOIN "Actuals" AS a
			ON o."Order Date" = a."Date"
			GROUP BY 1
		"""
		#Use frame_from_hyper_query to retrieve data from SQL query and print out df
		df = pt.frame_from_hyper_query(output_pantab, query)
		print(df)
	except Exception as e:
		print("Error! Check your SQL query and table name ",e)

#Read a custom SQL query from user by Pantab
def read_custom_sql_query_pantab(output_pantab,input_query):
	try:
		#this function is used to read the custom SQL query from users and print out the screen
		df = pt.frame_from_hyper_query(output_pantab,input_query)
		print(df)
	except Exception as e:
		print("Invalid SQL Query: \n",e)

#Tableau HyperAPI

#convert to sqltype
#To write the columns to hyper file, should use the SqlType
#However, the data type from pandas and SqlType is different, so need to convert
def convert_to_sqltype(col_dtype):
    #https://pandas.pydata.org/docs/reference/api/pandas.api.types.is_integer_dtype.html
    #source: https://tableau.github.io/hyper-db/lang_docs/py/_modules/tableauhyperapi/sqltype.html
    if pd.api.types.is_integer_dtype(col_dtype):
		#convert all integer columns to SqlType int()
        return SqlType.int()
    elif pd.api.types.is_float_dtype(col_dtype):
        return SqlType.double()
    elif pd.api.types.is_datetime64_any_dtype(col_dtype):
		#the date column from pandas is converted to the date data type in SqlType
        return SqlType.date()
    else:
		#for all the other data types, it will be converted to text
        return SqlType.text()

#write multi tables to a hyper file by Hyper API
def write_multi_tables_to_hyper_HyperAPI(dictionary_table, path_to_database):
	print()
	#Documentation: https://tableau.github.io/hyper-db/docs/hyper-api/connection
	try:
		#Connect to the Hyper database server locally on computer by using HyperProcess
		#to send the usage data to Tableau, set telemetry to send or do not send
		#default_database_verison 3 supports storing and querying 128-bit numberics (supported in Tableau Desktop 2022.4 and server 2023.1 or newer)
		with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU,parameters={"default_database_version":"3"}) as hyper:
			#to connect to the HYperprocess, use the Connection class to interact with Hyper
			#by using SQL query or using catalog, Inserter class
			# the endpoint specifies how to connect to Hyper (protocol, port)
			with Connection(endpoint=hyper.endpoint,
				   			#set the path to the database (where it will be stored)
							database=path_to_database,
							#Create an empty database, if already exists, replace by a new one
							create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
                
				#Create a schema name
				connection.catalog.create_schema(schema="Extract")
				#iterate over each item in the dictionary table
				for table_name, df in zip(dictionary_table.keys(), dictionary_table.values()):
					#Define each table with TableDefinition function
					extract_table = TableDefinition(
						#Set the table_name which is the key in dictionary
						table_name = TableName("Extract",table_name),
						#Define columns for each table with data type after converted to SqlType
						columns = [
							TableDefinition.Column(name = col, type = convert_to_sqltype(df[col])) for col in df.columns
						]
					)
					#Create the table using catalog
					connection.catalog.create_table(table_definition = extract_table)
					#Add rows from the values in dictionary
					rows_to_insert = df.itertuples(index=False, name=None)
                    #Used Inserter class to insert data to the table just created
					with Inserter(connection, extract_table) as inserter:
						inserter.add_rows(rows=rows_to_insert)
						inserter.execute()
                        
						#Count how many rows after adding data to the table
						row_count = connection.execute_scalar_query(
						query = f"SELECT COUNT(*) FROM {extract_table.table_name}"
					)
                    #print out the screen
					print(f"=> {row_count} Rows were INSERTED to table {extract_table.table_name} by HyperAPI")
				print(f"=> Located at: {path_to_database}")
		#Because HyperProcess and Connection start with the with statement, so the connection is automatically closed
		print("=> Connection to HyperAPI CLOSED")
	except HyperException as e:
		print("Error during running: ",e)

#Read a custom SQL query by Hyper API
def read_sql_query_hyperAPI(path_to_database):
	print("Write a simple SQL query to find total unit and avg weight of location site")
	try: 
		#Connect to the HyperProcess again and use Connection class to contact to HyperProcess
		with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
			with Connection(endpoint=hyper.endpoint,
							database=path_to_database,
							#Only read from hyper file, so set CreateMode to None
							create_mode=CreateMode.NONE) as connection:
				#Define the custom query
				query = """
					SELECT a."Location Site Name", SUM(o."Unit quantity"), AVG(o."Weight")
					FROM "Extract"."OrderList" as o
					JOIN "Extract"."Actuals" as a
					ON o."Order Date" = a."Date"
					GROUP BY 1
				"""
				#Execute the query and store the result
				result = connection.execute_query(query)
				#get all column names from the result, clean the column name by remove double quotes
				column_names = [str(col.name).strip('"') for col in result.schema.columns]
				#store the data in the list before add them to the dataframe
				rows = list(result)
				df = pd.DataFrame(rows, columns = column_names)
		return df
	except Exception as e:
		print(f"Error occurred: {e}")
	return None

#Read the custom sql from the user by Hyper API
#Same as  the function above
def read_custom_sql_query_hyperapi(path_to_database,query):
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database,
                        create_mode=CreateMode.NONE) as connection:

            
            with connection.execute_query(query) as returned_data:
                col_names = [str(col.name).strip('"') for col in returned_data.schema.columns]
                rows = list(returned_data)
                df = pd.DataFrame(rows, columns=col_names)

            return df

#Check if the sheets exist in the file 		
def check_sheet_exists_in_file(file_path, sheet_name):
	try:
		excel_file = pd.ExcelFile(file_path)
		if sheet_name not in excel_file.sheet_names:
			print(f"Sorry! Sheet name {sheet_name} does not exist in {file_path.name}")
			print(f"how about other sheets? {excel_file.sheet_names}")
			return None
	except Exception as e:
		print(f"Error in reading file {file_path.name}: {e}")
		return None
	
def main():
	print("==========================================================")
	print("====== DataDev Quest 2025-05 Intermediate Challenge ======")
	print("======               (Bonus Challenge)              ======")
	print("====== Challenged By: Nik Dutra                     ======")
	print("====== Solved By: Le Luu                            ======")
	print("==========================================================\n")

	#assign the file path of each excel file
	excel_path1 = parent_path / "RWFD_Supply_Chain.xlsx"
	if not excel_path1.exists():
		print(f"Cannot find the file path: {excel_path1}")
	
	excel_path2 = parent_path / "RWFD_Solar_Energy.xlsx"
	if not excel_path2.exists():
		print(f"Cannot find the file path: {excel_path2}")

	#Set 2 sheetname
	sheetname1 = "OrderList"
	check_sheet_exists_in_file(excel_path1,sheetname1)
	sheetname2 = "Actuals"
	check_sheet_exists_in_file(excel_path2,sheetname2)
	#read excel files
	df1 = pd.read_excel(excel_path1, sheet_name=sheetname1)
	df2 = pd.read_excel(excel_path2, sheet_name=sheetname2)

	#Pantab
	output_pantab = parent_path / "Output" / "pantab_multi_tables_output"/ "RWFD_multi_tables_pantab.hyper"
	#Store 2 tables in the dictionary table
	dictionary_table = {
		"OrderList":df1,
		"Actuals":df2
	}

	print("================= Recording running time for Pantab =================")
	
	start_time_pantab = time.time()
	write_multi_tables_to_hyper_pantab(dictionary_table, output_pantab)
	print("\n~~~~~~~~~~~~~~~~~~~Reading hyper file and joining data with SQL query~~~~~~~~~~~~~~~~~~~")
	read_sql_query_pantab(output_pantab)
	end_time_pantab = time.time()
	elapsed_time_pantab = end_time_pantab-start_time_pantab
	print(f"\n====>> Elapsed time for pantab: {elapsed_time_pantab:.4f} seconds")

	#Tableau HyperAPI
	path_to_database = parent_path / "Output"/ "hyperAPI_multi_tables_output" / "RWFD_multi_tables_hyperAPI.hyper"

	print("\n\n================= Recording running time for HyperAPI ===============")

	start_time_hyperapi = time.time()
	write_multi_tables_to_hyper_HyperAPI(dictionary_table,path_to_database)
	print("\n~~~~~~~~~~~~~~~~~~~Reading hyper file and joining data with SQL query~~~~~~~~~~~~~~~~~~~")
	result = read_sql_query_hyperAPI(path_to_database)
	print(result)
	end_time_hyperapi = time.time()
	elapsed_time_hyperapi = end_time_hyperapi - start_time_hyperapi
	print(f"\n====>> Elapsed time for Tableau HyperAPI: {elapsed_time_hyperapi:.4f} seconds")

	#Print the result to the screen
	print("\n\n**********************************************************************")
	if elapsed_time_pantab < elapsed_time_hyperapi:
		print("=======>>>> Pantab is the WINNER !!!")
		print("**********************************************************************")
		print(f"\n*** Pantab is running faster than Hyper API: {elapsed_time_hyperapi - elapsed_time_pantab:.4f} seconds ***")
	elif elapsed_time_pantab > elapsed_time_hyperapi:
		print("=======>>>> Tableau HyperAPI is the WINNER!!!")
		print("**********************************************************************")
		print(f"\n*** Tableau Hyper API is running faster than Pantab: {elapsed_time_pantab - elapsed_time_hyperapi:.4f} seconds ***")
	else:
		print(f"\n*** Wow! they have the same speed! ***")

	#Program to let user enter their own SQL query
	print("\n\n~~~~~~~~~~~~~ Time to test your SQL Query :) ~~~~~~~~~~~~~")
	exit_program = False
	while True:
		while True:
			answer = input(
			    "Would you like to test your SQL query with: \n"
			    "1) Pantab \n"
			    "2) Tableau HyperAPI\n"
			    "3) I don't want to test anymore. Let me exit the program! \n"
			    "Choose one number: "
			)

			if answer == "1":
				input_query = input("Enter your SQL query for Pantab (No need schema, put table in \"[table_name]\"): \n")
				read_custom_sql_query_pantab(output_pantab, input_query)
				break

			elif answer == "2":
				input_query = input(f"Enter your SQL query for HyperAPI (please add \"Extract\".\"[table_name]\" ):\n")
				result = read_custom_sql_query_hyperapi(path_to_database, input_query)
				print(result)
				break

			elif answer == "3":
				print("Exiting the program. Have a nice day!")
				exit_program = True
				break
	
			else:
				print("Please select only 1, 2, or 3.")

		if exit_program:
			break

		test_again = input("\n Would you like to test another SQL query? (y/n): ").strip().lower()
		if test_again not in ["yes","y"]:
			print("Have a nice data day! See you next time!")
			break

if __name__ == "__main__":
	main()