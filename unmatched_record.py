import polars as pl
import xlsxwriter

class unmatchedtransactionsreport:

    def __init__(self, gl_file, swift_file, output_file):
        self.output_file = output_file
        self.gl_unmatched = self.load_data(gl_file)
        self.swift_unmatched = self.load_data(swift_file)
        self.account_numbers = self.unique_account_numbers()
        
    def load_data(self, file_path):
        return pl.read_csv(file_path, ignore_errors=True).with_columns(pl.col('GL_NUMBER').cast(pl.Utf8))

    def unique_account_numbers(self):
        return pl.concat([
            self.swift_unmatched.select(pl.col('GL_NUMBER')),
            self.gl_unmatched.select(pl.col('GL_NUMBER'))
        ]).unique().drop_nulls()
    
    def create_report(self):
        workbook = xlsxwriter.Workbook(self.output_file)
        worksheet = workbook.add_worksheet('Unmatched Data')

        header_format = workbook.add_format({'bold': True, 'bg_color': 'yellow', 'align': 'center', 'valign': 'vcenter'})
        column_header_format = workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter'})
        data_format = workbook.add_format({'border': 1})

        row_counter = 0
        detailed_columns = ['Side', 'Value_Date', 'GL_NUMBER', 'Currency','Dr/Cr', 'DC_AMOUNT','Debit_Amount', 'Credit_Amount']

        for account in self.account_numbers['GL_NUMBER'].to_list():
            combined_transactions = self.combined_transactions(account)
            credit_total = combined_transactions.filter(pl.col('Dr/Cr') == 'Cr')['Credit_Amount'].sum()
            debit_total = combined_transactions.filter(pl.col('Dr/Cr') == 'Dr')['Debit_Amount'].sum()
            credit_count = combined_transactions.filter(pl.col('Dr/Cr') == 'Cr').height
            debit_count = combined_transactions.filter(pl.col('Dr/Cr') == 'Dr').height

            account_name = self.get_account_name(account)

            self.write_header(worksheet, row_counter, account, account_name ,credit_count, credit_total, debit_count, debit_total, header_format, detailed_columns)
            row_counter += 2

            self.write_column_headers(worksheet, row_counter, detailed_columns, column_header_format)
            row_counter += 1

            self.write_data(worksheet, row_counter, combined_transactions, data_format)
            row_counter += len(combined_transactions) + 2
        workbook.close()

    def combined_transactions(self,account):
        account_gl = self.gl_unmatched.filter(pl.col('GL_NUMBER') == account).select([
            pl.lit('NOSTRO_GL').alias('Side'),
            pl.col('Val/Settle Date').alias('Value_Date'),
            pl.col('GL_NUMBER'),
            pl.col('CURRENCY').alias('Currency'),
            pl.col('Dr/Cr Ind').alias('Dr/Cr'),
            pl.col('DC_AMOUNT').alias('Amount')
        ])
        account_swift = self.swift_unmatched.filter(pl.col('GL_NUMBER') == account).select([
            pl.lit('NOSTRO_SWIFT').alias('Side'),
            pl.col('Value Date').alias('Value_Date'),
            pl.col('GL_NUMBER'),
            pl.col('CURRENCY').alias('Currency'),
            pl.col('Dr/Cr Ind').alias('Dr/Cr'),
            pl.col('DC_AMOUNT').alias('Amount')
        ])
        return pl.concat([account_gl, account_swift]).with_columns(
            pl.when(pl.col('Dr/Cr') == 'Dr').then(pl.col('Amount')).otherwise(0).alias('Debit_Amount'),
            pl.when(pl.col('Dr/Cr') == 'Cr').then(pl.col('Amount')).otherwise(0).alias('Credit_Amount')
        )
    def get_account_name(self, account):
        return (
        self.gl_unmatched.filter(pl.col('Sierra Account Numbers').cast(pl.Utf8) == account)['GL_NAME'].head(1).to_list() or
        self.swift_unmatched.filter(pl.col('Account_Number').cast(pl.Utf8) == account)['GL_NAME'].head(1).to_list() or
        ['N/A']
    )[0]

    def write_header(self, worksheet, row_counter, account, account_name, credit_count, credit_total, debit_count, debit_total, header_format, detailed_columns):
        header_string = (
            f'Account_Number: {account}  '
            f'Account_Name: {account_name}  '
            f'Total_Credit_Count: {credit_count}  '
            f'Total_Credit_Amount: {credit_total}  '
            f'Total_Debit_Count: {debit_count}  '
            f'Total_Debit_Amount: {debit_total}'
        )
        worksheet.merge_range(row_counter, 0, row_counter,len(detailed_columns) - 1, header_string, header_format)

    def write_column_headers(self, worksheet, row_counter, detailed_columns, column_header_format):
        for col_num, header_name in enumerate(detailed_columns):
            worksheet.write(row_counter, col_num, header_name, column_header_format)

    def write_data(self, worksheet, row_counter, combined_transactions, data_format):
        for row in combined_transactions.iter_rows():
            for col_num, cell_value in enumerate(row):
                worksheet.write(row_counter, col_num, cell_value, data_format)
            row_counter += 1


if __name__ == "__main__":
    report = unmatchedtransactionsreport('NOSTRO_GL_UnMatched.csv', 'NOSTRO_SWIFT_UnMatched.csv', 'unmatchedtransactionsreport.xlsx')
    report.create_report()
