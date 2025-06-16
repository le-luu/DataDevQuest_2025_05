# DataDev Quest Challenge 2025_05

![image](https://github.com/le-luu/DataDevQuest_2025_03/blob/main/img/logo.svg)

### Challenged By: 
Nik Dutra

### Objective
- Convert the Excel files into Tableau Hyper format in Python using Tableau Hyper API and Pantab. 
- Compare the speed of read/ write from/to hyper file of both methods. 
- Test the speed of using SQL query to retrieve data from hyper files.
- Test your own SQL query from both methods.

### Solution Video


### Beginner Challenge
Link to the Beginner Challenge: https://datadevquest.com/ddq2025-05-convert-excel-to-tableau-hyper-files-beginner/

**Solution Expectation:**
- Successfully turn your Excel data into Hyper files both ways
- Include some timing info so we can see which method wins the race
- Work with any similar Excel file (because reusable code rocks!)
- Handle different data types appropriately

**Output:**
**Beginner Output**
![image](https://github.com/le-luu/DataDevQuest_2025_05/blob/main/img/DDQ_202505_Beginner.png)
To write an Excel to Hyper file with 599 Rows, Tableau Hyper API was running faster than Pantab 0.37 seconds (from the image above).
Checking the data and data type in Tableau Desktop:
![image](https://github.com/le-luu/DataDevQuest_2025_05/blob/main/img/Test_result_Tableau_Beginner.png)
All Hyper output files can be read in Tableau Desktop. All data types are correct. On the left image, the schema of the OrderList table is set to public by default by Pantab (599 rows in the table). On the right image, the schema of the OrderList table is Extract written by HyperAPI (599 rows in the table).

### Beginner Bonus Challenge
- Handle multiple Excel sheets 
- Create a command-line tool for easy use 
- Add proper error handling

For the Beginner Bonus Challenge, I did 3 bullet points above. To optimize the big dataset, I think of splitting the dataset into small chunks then write the first chunk into the hyper file and append the rest into the same hyper file. Another idea is creating the timestamp when storing the data in the data warehouse and write to the hyper file by the timestamp.

To update existing Hyper file instead of starting from scratch, it's related to the big dataset I mentioned above. I will write a separate program to do that.

![image](https://github.com/le-luu/DataDevQuest_2025_05/blob/main/img/DDQ_202505_Beginner_Bonus.png)

This is the output result from handling multiple sheets by using Pantab and Hyper API. For Pantab, I also wrote the first sheet into the hyper file and append the second sheet to the same hyper file. From the output result, Tableau Hyper API was running faster than Pantab.
Check the hyper file with Tableau Desktop:

![image](https://github.com/le-luu/DataDevQuest_2025_05/blob/main/img/Test_Result_Tableau_Beginner_Bonus_Pantab.png)

Import the hyper file written by Pantab, Tableau Desktop can load both tables (Customer and OrderList). By default, the schema is public. All data types from this hyper file are correct.

![image](https://github.com/le-luu/DataDevQuest_2025_05/blob/main/img/Test_result_Tableau_Beginner_Bonus.png)

Import the hyper file written by Hyper API, Tableau Desktop can load both tables. The schema here is "Extract". All data types loaded from this hyper file are correct.

### Intermediate Challenge
Link to the challenge: https://datadevquest.com/ddq2025-05-multi-source-excel-data-to-tableau-hyper-with-sql-queries-intermediate/
**Solution Expectations:**
- Successfully create Hyper files containing both tables using both methods
- Run SQL queries against both tables and save filtered results
- Include timing information for performance comparison
- Handle different data types appropriately in both approaches
- Follow good practices for resource management (closing connections, etc.)

**Output:**
**Intermediate Output**

![image](https://github.com/le-luu/DataDevQuest_2025_05/blob/main/img/DDQ_202505_Intermediate.png)

It was successfully write both tables into a hyper file and applied the SQL query to filter the data and write back to the same hyper file. The Tableau Hyper API was running faster than Pantab.
Check the hyper file with Tableau Desktop:

![image](https://github.com/le-luu/DataDevQuest_2025_05/blob/main/img/Test_result_Tableau_Intermediate_pantab.png)

Import the hyper file written by Pantab, Tableau Desktop loaded both tables from the hyper file. All data values are correct after filtering data. The data type of all columns is also correct.

![image](https://github.com/le-luu/DataDevQuest_2025_05/blob/main/img/Test_result_Tableau_Intermediate_HyperAPI.png)

Same as the result from Pantab. Tableau Desktop is also able to load the hyper file written by Hyper API. All filtered data in both tables are correct. The data types of all fields are correct.

### Intermediate Bonus Challenge
In the Intermediate Bonus Challenge, I did:
- Add a join or union query that combines data from both tables
- Implement aggregate queries (COUNT, SUM, AVG) against your data
- Create a simple command-line tool that allows users to specify their own SQL filters

![image](https://github.com/le-luu/DataDevQuest_2025_05/blob/main/img/DDQ_202505_Intermediate_Bonus.png)

I applied both 2 methods to write the data from 2 tables into a hyper file. Then, I applied the same SQL query to join both tables together and used aggregate function (SUM, AVG) in both methods. However, Pantab returns 2 rows of data, while Hyper API returns 9 rows. I also measured the running time. Tableau Hyper API was running faster than Pantab a half second. 

Then, I wrote a function to let the user enter their own custom SQL query. The user can select the method they want to try the SQL Query. For Pantab, doesn't need to type the schema name. For Tableau Hyper API, need to add the schema name with the table name. Both methods returns the same result.

### Instructions
### Instructions
- You need to install Python in your local computer or following the instructions from the DataDevQuest Challenge in Postman
- Fork the repository and clone it to your local computer
- Open the Command Prompt (for Windows) and Terminal (for Mac), change the directory to the DataDevQuest_2025_05
    ```
    cd DataDevQuest_2025_05
    ```
- Install and activate the virtual environment
    ```
    pip install virtualenv
    virtualenv venv
    venv\Scripts\activate
    ```    
- Install the packages in the Command Prompt
    ```
    pip install -r requirements.txt
    ```
    It may takes a few seconds to install all packages:
    - pandas
    - pantab
    - tableauhyperapi
    - pathlib
- The solution of the Beginner challenge is in DDQ_2025_05_Beginner folder (contains config.py, Le_DDQ_2025_05_Beginner_Solution.py, and Le_DDQ_2025_05_Beginner_Solution_Bonus_Challenge.py)
- The solution of the Intermediate challenge is in DDQ_2025_05_Intermediate folder (contains config, Le_DDQ_2025_05_Intermediate_Solution.py, and Le_DDQ_2025_05_Intermediate_Solution_Bonus_Challenge.py)
- Open the config.py (in both 2 folders) by any Python IDE or your Text Editor to change the directory (Parent Path) where you cloned this project and save it.
- Run this script for the Beginner Challenge:
    ```
    python Le_DDQ_2025_05_Beginner_Solution.py
    ```
- Run this script for the Beginner Bonus Challenge
    ```
    python Le_DDQ_2025_05_Beginner_Solution_Bonus_Challenge.py
    ```
- Run this script for the Intermediate Challenge:
    ```
    python Le_DDQ_2025_05_Intermediate_Solution.py
    ```
- Run this script for the Intermediate Bonus Challenge:
    ```
    python Le_DDQ_2025_05_Intermediate_Solution_Bonus_Challenge.py
    ```
### Resources:
Pantab Documentation: https://pantab.readthedocs.io/en/latest/

Tableau Hyper API Documentation: https://tableau.github.io/hyper-db/docs/

Tableau Hyper API Samples Github: https://github.com/tableau/hyper-api-samples/tree/main
