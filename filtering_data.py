import polars as pl
import xlsxwriter

unmatched_gl = pl.read_csv('unmatched_gl.csv', ignore_errors=True).with_columns( pl.col('Account_Number').cast(pl.Utf8))
  
unmatched_swift = pl.read_csv('unmatched_swift.csv', ignore_errors=True).with_columns(pl.col('Nostro Account').cast(pl.Utf8))

gl_unmatched = unmatched_gl.filter(pl.col('Nostro/Vostro/ Sett Entity Cur') == 'USD')
swift_unmatched = unmatched_swift.filter(pl.col('Account Currency') == 'USD')

usd_account_numbers = pl.concat([
    swift_unmatched.select(pl.col('Nostro Account').alias('Account_Number')),gl_unmatched.select('Account_Number')
]).unique().drop_nulls()

output_file = '(USD)unmatched_transactions_report.xlsx'
workbook = xlsxwriter.Workbook(output_file)
worksheet = workbook.add_worksheet('Unmatched Data (USD)')

header_format = workbook.add_format({'bold': True, 'bg_color': 'yellow', 'align': 'center', 'valign': 'vcenter'})
column_header_format = workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1})
data_format = workbook.add_format({'border': 1})

row_counter = 0
detailed_columns = ['Side', 'Value_Date', 'Account_Number', 'Currency', 'Dr/Cr', 'Amount', 'Debit_Amount', 'Credit_Amount']

for account in usd_account_numbers['Account_Number'].to_list():
    account_gl = gl_unmatched.filter(pl.col('Account_Number') == account).with_columns(
        pl.lit('NOSTRO_GL').alias('Side'),
        pl.when(pl.col('Cash Amt') < 0).then(pl.lit('Dr')).otherwise(pl.lit('Cr')).alias('Dr/Cr'),
        pl.col('Cash Amt').abs().alias('Amount'),
        pl.col('Nostro/Vostro/ Sett Entity Cur').alias('Currency'),
        pl.col('Val/Settle Date').alias('Value_Date')
    )
    
    account_swift = swift_unmatched.filter(pl.col('Nostro Account') == account).with_columns(
        pl.lit('NOSTRO_SWIFT').alias('Side'),
        pl.when(pl.col('Amount') < 0).then(pl.lit('Dr')).otherwise(pl.lit('Cr')).alias('Dr/Cr'),
        pl.col('Amount').abs().alias('Amount'),
        pl.col('Account Currency').alias('Currency'),
        pl.col('Value Date').alias('Value_Date')
    )
    
   
    combined_transactions = pl.concat([
        account_gl.select(['Side', 'Value_Date', 'Account_Number', 'Currency', 'Dr/Cr', 'Amount']),
        account_swift.select(['Side', 'Value_Date', pl.col('Nostro Account').alias('Account_Number'), 'Currency', 'Dr/Cr', 'Amount'])
    ]).with_columns(
        pl.when(pl.col('Dr/Cr') == 'Dr').then(pl.col('Amount')).otherwise(0).alias('Debit_Amount'),
        pl.when(pl.col('Dr/Cr') == 'Cr').then(pl.col('Amount')).otherwise(0).alias('Credit_Amount')
    )

    credit_total = combined_transactions.filter(pl.col('Dr/Cr') == 'Cr')['Credit_Amount'].sum()
    debit_total = combined_transactions.filter(pl.col('Dr/Cr') == 'Dr')['Debit_Amount'].sum()
    credit_count = combined_transactions.filter(pl.col('Dr/Cr') == 'Cr').height
    debit_count = combined_transactions.filter(pl.col('Dr/Cr') == 'Dr').height

    account_name = (gl_unmatched.filter(pl.col('Account_Number') == account)['Account Name'].head(1).to_list() or
                    swift_unmatched.filter(pl.col('Nostro Account') == account)['Account Name'].head(1).to_list() or
                    ['N/A'])[0]

    header_string = (
        f'Account_Number: {account}  '
        f'Account_Name: {account_name}  '
        f'Total_Credit_Count: {credit_count}  '
        f'Total_Credit_Amount: {credit_total}  '
        f'Total_Debit_Count: {debit_count}  '
        f'Total_Debit_Amount: {debit_total}'
    )
    worksheet.merge_range(row_counter, 0, row_counter, len(detailed_columns) - 1, header_string, header_format)
    row_counter += 2

    for col_num, header_name in enumerate(detailed_columns):
        worksheet.write(row_counter, col_num, header_name, column_header_format)
    row_counter += 1

    for row in combined_transactions.iter_rows():
        for col_num, cell_value in enumerate(row):
            worksheet.write(row_counter, col_num, cell_value, data_format)
        row_counter += 1

    row_counter += 2 

workbook.close()
