import pandas as pd
import time
import pantab as pt
from tableauhyperapi import HyperProcess, Connection, Telemetry, TableDefinition, SqlType, TableName, Inserter, CreateMode, Nullability, HyperException
from config import parent_path

#Write to a hyper file using Pantab
def write_hyper_by_pantab(df,output_pantab):
    #by default, the schema name is public
    #Use the fram_to_hyper function to write dataframe df to hyper file at output_pantab path
    pt.frame_to_hyper(df,output_pantab,table="OrderList",process_params={"default_database_version":"3"})
    #Count the number of row written to hyper file
    print(f"=> {len(df)} Rows were WRITTEN by Pantab")
    print(f"=> Located at: {output_pantab}")

#Write the data frame to a hyper file using hyper api
def write_hyper_by_hyperapi(df,path_to_database):
    try:
        #Defind the table using Table Definition function
        extract_table = TableDefinition(
            #Set the table name with the schema name Extract
            table_name=TableName("Extract", "OrderList"),
            #Assign the column data type for each column (There is another better way I did in bonus challenge)
            columns=[
                TableDefinition.Column(name="Order ID", type=SqlType.double()),
                TableDefinition.Column(name="Order Date", type=SqlType.date()),
                TableDefinition.Column(name="Origin Port", type=SqlType.varchar(8)),
                TableDefinition.Column(name="Carrier", type=SqlType.varchar(6)),
                TableDefinition.Column(name="TPT", type=SqlType.int()),
                TableDefinition.Column(name="Service Level", type=SqlType.varchar(6)),
                TableDefinition.Column(name="Ship ahead day count", type=SqlType.int()),
                TableDefinition.Column(name="Ship Late Day count", type=SqlType.int()),
                TableDefinition.Column(name="Customer", type=SqlType.varchar(20)),
                TableDefinition.Column(name="Product ID", type=SqlType.int()),
                TableDefinition.Column(name="Plant Code", type=SqlType.varchar(10)),
                TableDefinition.Column(name="Destination Port", type=SqlType.varchar(8)),
                TableDefinition.Column(name="Unit quantity", type=SqlType.int()),
                TableDefinition.Column(name="Weight", type=SqlType.double())
            ]
        )
        
        #Iterate into each row in data frame df. Will use this to insert data later
        rows_to_insert = df.itertuples(index=False, name=None)
        #Documentation: https://tableau.github.io/hyper-db/docs/hyper-api/connection
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
                connection.catalog.create_schema(schema=extract_table.table_name.schema_name)
                #Create the table from the Table definition above
                connection.catalog.create_table(table_definition=extract_table)
                #Insert each row into the table
                with Inserter(connection,extract_table) as inserter:
                    inserter.add_rows(rows=rows_to_insert)
                    inserter.execute()

                #execute a SQL query to count how many rows inserted
                row_count = connection.execute_scalar_query(
                    query = f"SELECT COUNT(*) FROM {extract_table.table_name}"
                )
                print(f"=> {row_count} Rows were INSERTED in table {extract_table.table_name}")
                print(f"=> Located at: {path_to_database}")

            print("=> Connection to Hyper file CLOSED")
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
    print("====== Challenged By: Nik Dutra                     ======")
    print("====== Solved By: Le Luu                            ======")
    print("==========================================================\n")
    
    excel_path = parent_path / "RWFD_Supply_Chain.xlsx"
    if not excel_path.exists():
        print(f"Cannot find the Excel path at: {excel_path}")
    sheet_name = "OrderList"
    #check if that sheet name exist in the Excel file
    check_sheet_exists_in_file(excel_path,sheet_name)
    #Read the excel file and store it in the pandas dataframes
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    
    #Pantab section
    output_pantab = parent_path / "Output" /"pantab_output" / "RWFD_Supply_Chain_pantab.hyper"
    if not output_pantab.exists():
        print(f"Cannot find the output file path for Hyper file using Pantab at: {output_pantab}")

    print("================= Recording running time for Pantab =================")
    start_time_pantab = time.time()
    write_hyper_by_pantab(df,output_pantab)
    end_time_pantab = time.time()
    elapsed_time_pantab=end_time_pantab-start_time_pantab
    print(f"\n====>> Elapsed Time for pantab: {elapsed_time_pantab:.4f} seconds")
    
    
    # Hyper API Section
    path_to_database = parent_path / "Output" /"hyperAPI_output" / "RWFD_Supply_Chain_hyperapi.hyper"
    if not path_to_database.exists():
        print(f"Cannot find the output file path for Hyper file using Hyper API at: {path_to_database}")
        
    print("\n\n================= Recording running time for HyperAPI ===============")
    start_time_hyper_api = time.time()
    write_hyper_by_hyperapi(df,path_to_database)
    end_time_hyper_api = time.time()
    elapsed_time_hyper_api = end_time_hyper_api-start_time_hyper_api
    print(f"\n====>> Elapsed Time for Hyper API: {elapsed_time_hyper_api:.4f} seconds")
    
    #Print the result to the screen
    print("\n\n**********************************************************************")
    if elapsed_time_pantab<elapsed_time_hyper_api:
        print("=======>>>> Pantab is the WINNER !!!")
        print("**********************************************************************")
        print(f"\n*** Pantab is running faster than Hyper API: {elapsed_time_hyper_api - elapsed_time_pantab:.4f} seconds ***")
    elif elapsed_time_pantab>elapsed_time_hyper_api:
        print("=======>>>> Tableau HyperAPI is the WINNER!!!")
        print("**********************************************************************")
        print(f"\n*** Tableau Hyper API is running faster than Pantab: {elapsed_time_pantab - elapsed_time_hyper_api:.4f} seconds ***")
    else:
        print(f"\n*** Wow! they have the same speed! ***")
    
if __name__ == "__main__":
    main()