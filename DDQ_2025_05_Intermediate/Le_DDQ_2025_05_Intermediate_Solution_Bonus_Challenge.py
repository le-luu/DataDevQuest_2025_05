import pandas as pd
import time
import pantab as pt
from tableauhyperapi import HyperProcess, Connection, Telemetry, TableDefinition, SqlType, TableName, Inserter, CreateMode, Nullability, HyperException
from pathlib import Path
from datetime import datetime as dt

#Pantab
def write_multi_tables_to_hyper_pantab(dictionary_table, output_pantab):
    pt.frames_to_hyper(dictionary_table,output_pantab)
    query1 = """
        SELECT COUNT(*)
        FROM "OrderList"
    """
    orderList_row = pt.frame_from_hyper_query(output_pantab, query1)
    print(f"\n=> {orderList_row['count'][0]} Rows were WRITTEN to OrderList table")
    
    query2 = """
        SELECT COUNT(*)
        FROM "Actuals"
    """
    actuals_row = pt.frame_from_hyper_query(output_pantab,query2)
    print(f"=> {actuals_row['count'][0]} Rows were WRITTEN to Actuals table")
    print(f"=> Located at: {output_pantab}")

def read_sql_query_pantab(output_pantab):
    print("Write a simple SQL query to find total unit and avg weight of location site")
    query = """
        SELECT a."Location Site Name", SUM(o."Unit quantity"), AVG(o."Weight")
        FROM "OrderList" AS o
        INNER JOIN "Actuals" AS a
        ON o."Order Date" = a."Date"
        GROUP BY 1
    """
    df = pt.frame_from_hyper_query(output_pantab, query)
    print(df)


def read_custom_sql_query_pantab(output_pantab,input_query):
	try:
		df = pt.frame_from_hyper_query(output_pantab,input_query)
		print(df)
	except Exception as e:
		print("Invalid SQL Query: \n",e)

#Tableau HyperAPI
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

def write_multi_tables_to_hyper_HyperAPI(dictionary_table, path_to_database):
	print()
	try:
		with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU,parameters={"default_database_version":"3"}) as hyper:
			with Connection(endpoint=hyper.endpoint,
							database=path_to_database,
							create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
                
				connection.catalog.create_schema(schema="Extract")
				#iterate over each item in the dictionary table
				for sheet_name, df in zip(dictionary_table.keys(), dictionary_table.values()):
					extract_table = TableDefinition(
						table_name = TableName("Extract",sheet_name),
						columns = [
							TableDefinition.Column(name = col, type = convert_to_sqltype(df[col])) for col in df.columns
						]
					)
					connection.catalog.create_table(table_definition = extract_table)
					rows_to_insert = df.itertuples(index=False, name=None)
                    
					with Inserter(connection, extract_table) as inserter:
						inserter.add_rows(rows=rows_to_insert)
						inserter.execute()
                        
						row_count = connection.execute_scalar_query(
						query = f"SELECT COUNT(*) FROM {extract_table.table_name}"
					)
                    
					print(f"=> {row_count} Rows were INSERTED to table {extract_table.table_name} by HyperAPI")
				print(f"=> Located at: {path_to_database}")
		print("=> Connection to HyperAPI CLOSED")
	except HyperException as e:
		print("Error during running: ",e)


def read_sql_query_hyperAPI(path_to_database):
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database,
                        create_mode=CreateMode.NONE) as connection:
            query = """
                SELECT a."Location Site Name", SUM(o."Unit quantity"), AVG(o."Weight")
                FROM "Extract"."OrderList" as o
                JOIN "Extract"."Actuals" as a
                ON o."Order Date" = a."Date"
                GROUP BY 1
            """
            result = connection.execute_query(query)
            column_names = [str(col.name).strip('"') for col in result.schema.columns]
            rows = list(result)
            df = pd.DataFrame(rows, columns = column_names)
    return df

def read_custom_sql_query_hyperapi(path_to_database,query):
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database,
                        create_mode=CreateMode.NONE) as connection:

            result_dict = {}
            with connection.execute_query(query) as returned_data:
                col_names = [str(col.name).strip('"') for col in returned_data.schema.columns]
                rows = list(returned_data)
                result_dict['result'] = pd.DataFrame(rows, columns=col_names)

            return result_dict['result']

def main():
	print("==========================================================")
	print("====== DataDev Quest 2025-05 Intermediate Challenge ======")
	print("====== Challenged By: Nik Dutra                     ======")
	print("====== Solved By: Le Luu                            ======")
	print("==========================================================\n")

	parent_path = Path(r"C:\Users\LeLuu\Documents\TableauDevQuest\DataDevQuest_Challenges\2025_05")
	excel_path1 = parent_path / "RWFD_Supply_Chain.xlsx"
	excel_path2 = parent_path / "RWFD_Solar_Energy.xlsx"

	sheetname1 = "OrderList"
	sheetname2 = "Actuals"
	df1 = pd.read_excel(excel_path1, sheet_name=sheetname1)
	df2 = pd.read_excel(excel_path2, sheet_name=sheetname2)

	#Pantab
	output_pantab = parent_path / "pantab_multi_tables_output"/ "RWFD_multi_tables_pantab.hyper"

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
	path_to_database = parent_path / "hyperAPI_multi_tables_output" / "RWFD_multi_tables_hyperAPI.hyper"

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


	print("\n\n~~~~~~~~~~~~~ Time to test your SQL Query :) ~~~~~~~~~~~~~")
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
			    break

			else:
			    print("Please select only 1, 2, or 3.")

		test_again = input("\n Would you like to test another SQL query? (y/n): ").strip().lower()
		if test_again not in ["yes","y"]:
			print("Have a nice data day! See you next time!")
			break

if __name__ == "__main__":
	main()