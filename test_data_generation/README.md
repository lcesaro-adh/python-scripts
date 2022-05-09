# Script Test Data Generation
## Requirements
In order to run the script you need the base tables to start from
## Usage
Check that the reading folder and tables are correct and that match with the keys you'll take for creating the dummy data 
> python test_data_generation
- Asks how many time you want to enlarge the base tables (Note that the enlargement gets compounded 1+2+4+8)
- Asks if the size is fine after the enlargement
- If the final size was not fine you define the reduction in percentage and after it takes place saves the outcome
### Different enlargement
You can edit the input tables and the primary keys to enlarge whatever table you want