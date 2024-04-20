import pandas as pd
import json

# Load the JSON file
with open('test_input.json', 'r') as file:
    data = json.load(file)

# Extract users, transactions, and companies into separate lists
users = []
transactions = []
companies = []

for item in data['data']['customers']:
    users.append(item)

for item in data['data']['transactions']:
    transactions.append(item)

for item in data['data']['companies']:
    companies.append(item)

# Create DataFrames for users, transactions, and companies
df_users = pd.DataFrame(users)
df_transactions = pd.DataFrame(transactions)
df_companies = pd.DataFrame(companies)

# Rename the 'id' column to 'customer_id' in the users DataFrame
df_users.rename(columns={'id': 'customer_id'}, inplace=True)

# Convert the 'date' column to datetime format in the transactions DataFrame
df_transactions['date'] = pd.to_datetime(df_transactions['date'])

# Convert the 'amount' column to float format in the transactions DataFrame
df_transactions['amount'] = df_transactions['amount'].astype(float)

# Create a new column 'ano_mes' to represent the year and month of the transaction in the transactions DataFrame
df_transactions['ano_mes'] = df_transactions['date'].dt.strftime('%Y-%m')

# Merge the transactions DataFrame with the users DataFrame on 'customer_id'
df = pd.merge(df_transactions, df_users, on='customer_id', how='left')

# Calculate cashback based on transaction type and company's cashback schedule
df.loc[df['type'] == '1', 'cash_back'] = df['amount'] * 0.01
df.loc[df['type'] != '1', 'cash_back'] = 0

# Iterate over transactions to calculate cashback and excess amount
for index, row in df.iterrows():
    transaction_date = row['date']
    transaction_amount = row['amount']
    transaction_type = row['type']
    cashback_amount = 0
    excess_amount = 0
    
    # Iterate over companies
    for idx, company_row in df_companies.iterrows():
        for schedule in company_row['cashback_schedule']:
            start_date = pd.to_datetime(schedule['start_date'])
            end_date = pd.to_datetime(schedule['end_date'])
            ceiling_cap = float(schedule['ceiling_cap'])
            
            # Check if transaction date is within the cashback schedule
            if start_date <= transaction_date <= end_date:
                if transaction_type == '1':
                    if transaction_amount >= 50:
                        cashback_amount += transaction_amount * 0.02
                    else:
                        cashback_amount += transaction_amount * 0.01
                
                    if transaction_amount > ceiling_cap:
                        cashback_amount += ceiling_cap * 0.02  
                        excess_amount = transaction_amount - ceiling_cap
                break  

    # Update cashback and excess amount in the DataFrame
    df.at[index, 'cash_back'] += cashback_amount
    df.at[index, 'excess_amount'] = excess_amount

# Calculate partial cashback for each transaction
df.loc[df['type'] == '1', 'partial_cashback'] =  df['amount'] - df['cash_back']
df.loc[df['type'] != '1', 'partial_cashback'] = 0

# Group transactions by customer and year-month to calculate totals
monthly_totals = df.groupby(['customer_id', df['date'].dt.strftime('%Y-%m')]).agg(
    total_cashback=('cash_back', 'sum'),
    total_amount=('amount', 'sum'),
    total_amount_with_discount=('partial_cashback', 'sum'),
    total_amount_not_eligible=('excess_amount', 'sum')
).reset_index()

# Sort the DataFrame by date
monthly_totals_sorted = monthly_totals.sort_values(by='date')

# Print the output sorted by month #
for index, row in monthly_totals_sorted.iterrows():
    print("Month:", row['date'])
    print("Customer:", row['customer_id'])
    print("Total Cashback:", round(row['total_cashback'], 2))
    print("Total Amount:", round(row['total_amount'], 2))
    print("Total Amount with Discount:", round(row['total_amount_with_discount'], 2))
    print("Total Amount not Eligible for Cashback:", round(row['total_amount_not_eligible'], 2))
    print()