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

**Beginner Bonus Challenge**
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

**Intermediate Output**
### Resources:
Pantab Documentation: https://pantab.readthedocs.io/en/latest/
Tableau Hyper API Documentation: https://tableau.github.io/hyper-db/docs/
Tableau Hyper API Samples Github: https://github.com/tableau/hyper-api-samples/tree/main
