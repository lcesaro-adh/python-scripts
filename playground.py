import pandas as pd

data = [['tom', 10], ['nick', 15], ['juli', 14]]
# Create the pandas DataFrame
input_df = pd.DataFrame(data, columns = ['Name', 'Age'])

combination_columns = [col for col in input_df.columns]
print(combination_columns)