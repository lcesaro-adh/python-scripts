# Script Test Data Generation
## Requirements
In order to run the script you need the base tables to start from
## Usage
Check that the reading folder and tables are correct and that match with the keys you'll take to create the dummy data 
> python test_data_generation
### Description
- Asks how many time you want to enlarge the base tables (Note that the enlargement gets compounded for the table having the foreign keys 1+2+4+8)
- Asks if the size is fine after the enlargement
- If the final size was not fine you define the reduction in percentage and after it takes place saves the outcome
### Different enlargement
You can edit the input tables and the key match to enlarge any table