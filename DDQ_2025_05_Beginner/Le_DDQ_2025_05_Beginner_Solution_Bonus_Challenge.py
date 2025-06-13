import pandas as pd
import time
import pantab as pt
from tableauhyperapi import HyperProcess, Connection, Telemetry, TableDefinition, SqlType, TableName, Inserter, CreateMode, Nullability, HyperException
from config import parent_path

#write multi-sheets to a hyper file by using Pantab
def write_multisheet_to_hyper_by_pantab(sheet_dictionary, output_pantab):
	#Iterate to each item in dictionary (key is the sheet name and value is dataframe df)
	for i, (sheet_name, df) in enumerate(sheet_dictionary.items()):
		#for the first sheet to write to a hyper file the table mode is write
		if i==0:
			mode="w"
		else:
		#after the first sheet, to write to the same hyper file, set the table mode to append
			mode="a"
        # Write each sheet in the excel file into a separate table using frame_to_hyper with table mode
		pt.frame_to_hyper(df, output_pantab, table=sheet_name, table_mode=mode)
		print(f'=> {len(df)} Rows were WRITTEN to table {sheet_name} by Pantab')
	#print out the location where the hyper file located
	print(f"\n=> Located at: {output_pantab}")

#Convert data type of each column in pandas dataframe to Sqltype
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

#Write multi-sheets to a hyper file by using HyperAPI
def write_multi_sheet_to_hyper_by_hyperapi(sheet_dictionary, path_to_database):
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
				#Iterate to each table in the dictionary
				for table_name, df in sheet_dictionary.items():
					#Define each table with TableDefinition function
					extract_table = TableDefinition(
						#Set the table_name which is the key in dictionary
	                    table_name = TableName("Extract",table_name),
						#Define columns for each table with data type after converted to SqlType
	                    columns = [
	                        TableDefinition.Column(name=col, type = convert_to_sqltype(df[col])) for col in df.columns
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
					print(f"=> {row_count} Rows were INSERTED to table {extract_table.table_name} by Hyper API")
				#Print the location where the file is saved
				print(f"\n=> Located at : {path_to_database}")

			print("=> Connection to Hyper API CLOSED")
	except HyperException as e:
		print("Error during running: ",e)

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
	print("====== DataDev Quest 2025-05 Beginner Challenge     ======")
	print("======             (Bonus Challenges)               ======")
	print("====== Challenged By: Nik Dutra                     ======")
	print("====== Solved By: Le Luu                            ======")
	print("==========================================================\n")

	#Assign the file path for the Excel file
	excel_path = parent_path / "RWFD_Supply_Chain.xlsx"
	if not excel_path.exists():
		print(f"Cannot find the Excel file path: {excel_path}")
	#Store all sheets in the Excel file to a dictionary
	#The name of the sheet is the key, values are data of the sheet
	sheet_dictionary = pd.read_excel(excel_path,sheet_name=None)

	#Pantab
	output_pantab = parent_path / "Output" /"pantab_multi_sheets_output" / "RWFD_Supply_Chain_multi_sheet_pantab.hyper"
	if not output_pantab.exists():
		print(f"Cannot find the output file path to store hyper file from pantab at: {output_pantab}")
	print("================= Recording running time for Pantab =================")
	start_time_pantab = time.time()
	write_multisheet_to_hyper_by_pantab(sheet_dictionary,output_pantab)
	end_time_pantab = time.time()
	elapsed_time_pantab=end_time_pantab-start_time_pantab
	print(f"\n====>> Elapsed Time for pantab: {elapsed_time_pantab:.4f} seconds")

    #Tableau Hyper API
	path_to_database = parent_path / "Output" / "hyperAPI_multi_sheet_output" / "RWFD_Supply_Chain_multi_sheet_hyperapi.hyper"
	if not path_to_database.exists():
		print(f"Cannot find the output file path to store hyper file from HyperAPI at: {path_to_database} ")
	
	print("\n\n================= Recording running time for HyperAPI ===============")
	start_time_hyper_api = time.time()
	write_multi_sheet_to_hyper_by_hyperapi(sheet_dictionary,path_to_database)
	end_time_hyper_api = time.time()
	elapsed_time_hyper_api = end_time_hyper_api-start_time_hyper_api
	print(f"\n====>> Elapsed Time for Hyper API: {elapsed_time_hyper_api:.4f} seconds")
    
	#Print the result to the screen
	print("\n\n**********************************************************************")
	if elapsed_time_pantab < elapsed_time_hyper_api:
		print("=======>>>> Pantab is the WINNER !!!")
		print("**********************************************************************")
		print(f"\n*** Pantab is running faster than Hyper API: {elapsed_time_hyper_api - elapsed_time_pantab:.4f} seconds ***")
	elif elapsed_time_pantab > elapsed_time_hyper_api:
		print("=======>>>> Tableau HyperAPI is the WINNER!!!")
		print("**********************************************************************")
		print(f"\n*** Tableau Hyper API is running faster than Pantab: {elapsed_time_pantab - elapsed_time_hyper_api:.4f} seconds ***")
	else:
		print(f"\n*** Wow! they have the same speed! ***")
    
if __name__ == "__main__":
	main()