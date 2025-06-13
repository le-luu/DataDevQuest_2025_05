import pandas as pd
import time
import pantab as pt
from tableauhyperapi import HyperProcess, Connection, Telemetry, TableDefinition, SqlType, TableName, Inserter, CreateMode, Nullability, HyperException
from pathlib import Path
from config import parent_path

#For Pantab
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

#Read the table and write it back to the same hyper file
def read_write_multi_tables_pantab(output_pantab):
	query1 = """
			SELECT *
			FROM "OrderList" AS o
			WHERE "Weight" > 50
	"""
	#Retrieve data from the query and store it in df1
	df1 = pt.frame_from_hyper_query(output_pantab, query1)
	print(f"=> {len(df1)} Rows were WRITTEN to OrderList table after filtered data!")
	query2 = """
			SELECT *
			FROM "Actuals"
			WHERE "Latitude" < 40
	"""
	df2 = pt.frame_from_hyper_query(output_pantab, query2)
	print(f"=> {len(df2)} Rows were WRITTEN to Actuals table after filetered data!")

	#Store data df1, df2 in a dictionary
	dictionary_table = {"OrderList_filtered":df1,"Actuals_filtered":df2}
	#Used frames_to_hyper function to write the dictionary to the same hyperfile (output_pantab)
	pt.frames_to_hyper(dictionary_table, output_pantab)
	print(f"=> Located at: {output_pantab}")

#For Tableau HyperAPI
#convert to sqltype
#To write the columns to hyper file, should use the SqlType
#However, the data type from pandas and SqlType is different, so need to convert
def convert_to_sqltype(col_dtype):
    #https://pandas.pydata.org/docs/reference/api/pandas.api.types.is_integer_dtype.html
    #source: https://tableau.github.io/hyper-db/lang_docs/py/_modules/tableauhyperapi/sqltype.html
    if pd.api.types.is_integer_dtype(col_dtype):
        return SqlType.int()
    elif pd.api.types.is_float_dtype(col_dtype):
        return SqlType.double()
    elif pd.api.types.is_datetime64_any_dtype(col_dtype):
        return SqlType.date()
    else:
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
                    
					print(f"=> {row_count} Rows were INSERTED to table {extract_table.table_name} by HyperAPI")
				print(f"=> Located at: {path_to_database}")
		#Because HyperProcess and Connection start with the with statement, so the connection is automatically closed
		print("=> Connection to HyperAPI CLOSED")
	except HyperException as e:
		print("Error during running: ",e)

#Read multiple tables from the hyper file by hyper api
def read_multitables_from_hyper_hyperAPI(path_to_database):
	#Connect to the HyperProcess class
	with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
		with Connection(endpoint=hyper.endpoint,
						database=path_to_database,
						#Only need to read, so don't need to create and replace table
						create_mode=CreateMode.NONE) as connection:
            #assign 2 SQL queries in a dictionary
			queries = {
                "OrderList": """
                    SELECT * 
                    FROM "Extract"."OrderList" 
                    WHERE "Weight" > 50
                """,
                "Actuals": """
                    SELECT * 
                    FROM "Extract"."Actuals"
                    WHERE "Latitude" < 40
				 """
			}
            
			#initialize an empty dictionary to store the result from both 2 queries later
			result_dict = {}
			 #Iterate to each query in queries above
			for table_name, query in queries.items():
				with connection.execute_query(query) as returned_data:
					#Get the column name after getting the result from SQL query
					col_names = [str(col.name).strip('"') for col in returned_data.schema.columns]
					#Get the data and store them in the list
					rows = list(returned_data)
					#Append to the dataframe to the result_dict dictionary initialized before.
					result_dict[table_name] = pd.DataFrame(rows, columns=col_names)
            #there is a problem that the date data type doesn't return correct.
			#So, I need to convert the date column to datetime data type
			for table, df in result_dict.items():
				for col in df.columns:
					if "date" in col.lower():
						df[col] = pd.to_datetime(df[col], format='ISO8601')
            #return the result_dict contains both tables
			return result_dict

#Check if the sheets exist in the Excel file
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
	print("====== Challenged By: Nik Dutra                     ======")
	print("====== Solved By: Le Luu                            ======")
	print("==========================================================\n")

	#get the Excel path of 2 tables and check if the path is correct
	excel_path1 = parent_path / "RWFD_Supply_Chain.xlsx"
	if not excel_path1.exists():
		print(f"Cannot find the file path: {excel_path1}")

	excel_path2 = parent_path / "RWFD_Solar_Energy.xlsx"
	if not excel_path2.exists():
		print(f"Cannot find the file path: {excel_path2}")

	#Assign the sheet name and check if the sheet name exist in the Excel file
	sheetname1 = "OrderList"
	check_sheet_exists_in_file(excel_path1,sheetname1)
	sheetname2 = "Actuals"
	check_sheet_exists_in_file(excel_path2,sheetname2)
	
	#Read Excel files and store them in the dataframe
	df1 = pd.read_excel(excel_path1, sheet_name=sheetname1)
	df2 = pd.read_excel(excel_path2, sheet_name=sheetname2)

	#Pantab
	output_pantab = parent_path / "Output" /"pantab_multi_tables_output"/ "RWFD_multi_tables_pantab.hyper"
	if not output_pantab.exists():
		print(f"Cannot find this path to store the Hyper file using Pantab: {output_pantab}")
	#Create a dictionary to store 2 datarfames
	dictionary_table = {
		"OrderList":df1,
		"Actuals":df2
	}

	print("================= Recording running time for Pantab =================")

	start_time_pantab = time.time()
	write_multi_tables_to_hyper_pantab(dictionary_table, output_pantab)
	print("\n~~~~~~~~~~~~~~~~~~~Reading hyper file and filter data with SQL query~~~~~~~~~~~~~~~~~~~")
	read_write_multi_tables_pantab(output_pantab)
	end_time_pantab = time.time()
	elapsed_time_pantab = end_time_pantab-start_time_pantab
	print(f"\n====>> Elapsed time for pantab: {elapsed_time_pantab:.4f} seconds")

	#Tableau HyperAPI
	path_to_database = parent_path / "Output" /"hyperAPI_multi_tables_output" / "RWFD_multi_tables_hyperAPI.hyper"
	if not path_to_database.exists():
		print(f"Cannot find this path to store Hyper File using HyperAPI: {path_to_database}")

	print("\n\n================= Recording running time for HyperAPI ===============")

	start_time_hyperapi = time.time()
	write_multi_tables_to_hyper_HyperAPI(dictionary_table,path_to_database)
	print("\n~~~~~~~~~~~~~~~~~~~Reading hyper file and filter data with SQL query~~~~~~~~~~~~~~~~~~~")
	result = read_multitables_from_hyper_hyperAPI(path_to_database)
	write_multi_tables_to_hyper_HyperAPI(result,path_to_database)
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

if __name__ == "__main__":
	main()