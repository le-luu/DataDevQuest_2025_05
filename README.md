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
**Beginner Solution**
![image](https://github.com/le-luu/DataDevQuest_2025_05/blob/main/img/DDQ_202505_Beginner.png)
To write an Excel to Hyper file with 599 Rows, Tableau Hyper API was running faster than Pantab 0.37 seconds (from the image above).
Checking the data and data type in Tableau Desktop:
![image](https://github.com/le-luu/DataDevQuest_2025_05/blob/main/img/Test_result_Tableau_Beginner.png)
All Hyper output files can be read in Tableau Desktop. All data types are correct. On the left image, the schema of the OrderList table is set to public by default by Pantab (599 rows in the table). On the right image, the schema of the OrderList table is Extract written by HyperAPI (599 rows in the table).

### Resources:
Pantab Documentation: https://pantab.readthedocs.io/en/latest/
Tableau Hyper API Documentation: https://tableau.github.io/hyper-db/docs/
Tableau Hyper API Samples Github: https://github.com/tableau/hyper-api-samples/tree/main
