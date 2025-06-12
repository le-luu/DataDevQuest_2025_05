import pandas as pd
import time
import pantab as pt
from tableauhyperapi import HyperProcess, Connection, Telemetry, TableDefinition, SqlType, TableName, Inserter, CreateMode, Nullability, HyperException
from pathlib import Path

#write multi-sheets to a hyper file by using Pantab
def write_multisheet_to_hyper_by_pantab(sheet_dictionary, output_pantab):
	#Iterate to each item in dictionary (key is the sheet name and value is dataframe df)
    for sheet_name, df in sheet_dictionary.items():
        # Write each sheet in the excel file into a separate table using frame_to_hyper
        pt.frame_to_hyper(df, output_pantab, table=sheet_name)
        print(f'=> {len(df)} Rows were WRITTEN to table {sheet_name} by Pantab')
    #print out the location where the hyper file located
    print(f"\n=> Located at: {output_pantab}")

#Convert data type of each column in pandas dataframe to Sqltype
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
    try:
	    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU,parameters={"default_database_version":"3"}) as hyper:
	        with Connection(endpoint=hyper.endpoint,
	                        database=path_to_database,
	                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
	            
	            connection.catalog.create_schema(schema="Extract")
	            for sheet_name, df in sheet_dictionary.items():
	                
	                extract_table = TableDefinition(
	                    table_name = TableName("Extract",sheet_name),
	                    columns = [
	                        TableDefinition.Column(name=col, type = convert_to_sqltype(df[col])) for col in df.columns
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
	                print(f"=> {row_count} Rows were INSERTED to table {extract_table.table_name} by Hyper API")
	            print(f"\n=> Located at : {path_to_database}")

	        print("=> Connection to Hyper API CLOSED")
    except HyperException as e:
        print("Error during running: ",e)


def main():

	print("==========================================================")
	print("====== DataDev Quest 2025-05 Beginner Challenge     ======")
	print("======             (Bonus Challenges)               ======")
	print("====== Challenged By: Nik Dutra                     ======")
	print("====== Solved By: Le Luu                            ======")
	print("==========================================================\n")

	parent_path = Path(r"C:\Users\LeLuu\Documents\TableauDevQuest\DataDevQuest_Challenges\2025_05")
	excel_path = parent_path / "RWFD_Supply_Chain.xlsx"

	sheet_dictionary = pd.read_excel(excel_path,sheet_name=None)

	#Pantab
	output_pantab = parent_path / "pantab_multi_sheets_output" / "RWFD_Supply_Chain_multi_sheet_hyperapi.hyper"

	print("================= Recording running time for Pantab =================")
	start_time_pantab = time.time()
	write_multisheet_to_hyper_by_pantab(sheet_dictionary,output_pantab)
	end_time_pantab = time.time()
	elapsed_time_pantab=end_time_pantab-start_time_pantab
	print(f"\n====>> Elapsed Time for pantab: {elapsed_time_pantab:.4f} seconds")

    #Tableau Hyper API
	path_to_database = parent_path / "hyperAPI_multi_sheet_output" / "RWFD_Supply_Chain_multi_sheet_hyper_file.hyper"
    
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