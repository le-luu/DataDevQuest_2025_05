import pandas as pd
import time
import pantab as pt
from tableauhyperapi import HyperProcess, Connection, Telemetry, TableDefinition, SqlType, TableName, Inserter, CreateMode, Nullability, HyperException
from pathlib import Path

def write_hyper_by_pantab(df,output_pantab):
    #by default, the schema name is public
    pt.frame_to_hyper(df,output_pantab,table="OrderList",process_params={"default_database_version":"3"})
    print(f"=> {len(df)} Rows were WRITTEN by Pantab")
    print(f"=> Located at: {output_pantab}")

def write_hyper_by_hyperapi(df,path_to_database):
    try:
        
        extract_table = TableDefinition(
            table_name=TableName("Extract", "OrderList"),
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
        
        
        rows_to_insert = df.itertuples(index=False, name=None)
        with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU,parameters={"default_database_version":"2"}) as hyper:
            with Connection(endpoint=hyper.endpoint,
                            database=path_to_database,
                            create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
                
                connection.catalog.create_schema(schema=extract_table.table_name.schema_name)
                connection.catalog.create_table(table_definition=extract_table)

                with Inserter(connection,extract_table) as inserter:
                    inserter.add_rows(rows=rows_to_insert)
                    inserter.execute()

                row_count = connection.execute_scalar_query(
                    query = f"SELECT COUNT(*) FROM {extract_table.table_name}"
                )
                print(f"=> {row_count} Rows were INSERTED in table {extract_table.table_name}")
                print(f"=> Located at: {path_to_database}")

            print("=> Connection to Hyper file CLOSED")
    except HyperException as e:
        print("Error during running: ",e)


def main():
    print("==========================================================")
    print("====== DataDev Quest 2025-05 Beginner Challenge     ======")
    print("====== Challenged By: Nik Dutra                     ======")
    print("====== Solved By: Le Luu                            ======")
    print("==========================================================\n")
    
    parent_path = Path(r"C:\Users\LeLuu\Documents\TableauDevQuest\DataDevQuest_Challenges\2025_05")
    excel_path = parent_path / "RWFD_Supply_Chain.xlsx"
    sheet_name = "OrderList"
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    
    #Pantab section
    output_pantab = parent_path / "pantab_output" / "RWFD_Supply_Chain_hyper_file.hyper"
    
    print("================= Recording running time for Pantab =================")
    start_time_pantab = time.time()
    write_hyper_by_pantab(df,output_pantab)
    end_time_pantab = time.time()
    elapsed_time_pantab=end_time_pantab-start_time_pantab
    print(f"\n====>> Elapsed Time for pantab: {elapsed_time_pantab:.4f} seconds")
    
    
    # Hyper API Section
    path_to_database = parent_path / "hyperAPI_output" / "RWFD_Supply_Chain_hyper_file.hyper"
    
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