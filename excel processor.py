import polars as pl

class CombinedReport:
    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file

    def load_data(self):
        gl_df = pl.read_excel(self.input_file, sheet_name="NOSTRO_GL")
        swift_df = pl.read_excel(self.input_file, sheet_name="NOSTRO_SWIFT")
        return gl_df, swift_df

    def process_data(self, gl_df, swift_df):
        columns = [
            "GL_NAME", "GL_NUMBER", "SOURCE", "EXECUTION_DATE_TIME",
            "CURRENCY", "EXECUTION_STATEMENTDATE", "DC_AMOUNT",
            "Dr/Cr Ind", "Total Debit", "Total Credit",
            "CARRY_FORWARD", "MATCHING_STATUS"
        ]
        combined_df = pl.concat([gl_df.select(columns), swift_df.select(columns)], how="vertical")

        numeric_columns = ["DC_AMOUNT", "Total Debit", "Total Credit"]
        for col in numeric_columns:
            combined_df = combined_df.with_columns(
                pl.col(col).cast(pl.Float64)  
            )

        unique_accounts = combined_df.select("GL_NUMBER").unique().to_series().to_list()
        results = []

        for account in unique_accounts:
            for source in ["NOSTRO_GL", "NOSTRO_SWIFT"]:
                unmatched_df = combined_df.filter(
                    (pl.col("GL_NUMBER") == account) &
                    (pl.col("SOURCE") == source) &
                    (pl.col("CARRY_FORWARD") == "N") &
                    (pl.col("MATCHING_STATUS") == "UNMATCHED")
                )
                
                unmatched_dc_amount = unmatched_df["DC_AMOUNT"].sum() 
                unmatched_dc_count = unmatched_df.height
                
                debit_unmatched_amount = unmatched_df["Total Debit"].sum() 
                credit_unmatched_amount = unmatched_df["Total Credit"].sum() 
                debit_unmatched_count = unmatched_df.filter(pl.col("Total Debit") > 0).height
                credit_unmatched_count = unmatched_df.filter(pl.col("Total Credit") > 0).height
                
                matched_df = combined_df.filter(
                    (pl.col("GL_NUMBER") == account) &
                    (pl.col("SOURCE") == source) &
                    (pl.col("CARRY_FORWARD") == "N") &
                    (pl.col("MATCHING_STATUS") == "MATCHED")
                )
                
                matched_dc_amount = matched_df["DC_AMOUNT"].sum() 
                matched_dc_count = matched_df.height
                
                debit_matched_amount = matched_df["Total Debit"].sum() 
                credit_matched_amount = matched_df["Total Credit"].sum()
                debit_matched_count = matched_df.filter(pl.col("Total Debit") > 0).height
                credit_matched_count = matched_df.filter(pl.col("Total Credit") > 0).height

                reversal_amount = 0
                reversal_count = 0
                debit_reversal_amount = 0
                debit_reversal_count = 0
                credit_reversal_amount = 0
                credit_reversal_count = 0

                if source == "NOSTRO_GL":
                    reversal_df = combined_df.filter(
                        (pl.col("GL_NUMBER") == account) &
                        (pl.col("SOURCE") == "NOSTRO_SWIFT") &
                        (pl.col("MATCHING_STATUS") == "Reversal")
                    )
                else:
                    reversal_df = combined_df.filter(
                        (pl.col("GL_NUMBER") == account) &
                        (pl.col("SOURCE") == "NOSTRO_GL") &
                        (pl.col("MATCHING_STATUS") == "Reversal")
                    )

                reversal_amount = reversal_df["DC_AMOUNT"].sum()
                reversal_count = reversal_df.height
                debit_reversal_amount = reversal_df["Total Debit"].sum()
                debit_reversal_count = reversal_df.filter(pl.col("Total Debit") > 0).height
                credit_reversal_amount = reversal_df["Total Credit"].sum()
                credit_reversal_count = reversal_df.filter(pl.col("Total Credit") > 0).height

                carry_forward_matched_df = combined_df.filter(
                    (pl.col("GL_NUMBER") == account) &
                    (pl.col("SOURCE") == source) &
                    (pl.col("CARRY_FORWARD") == "Y") &
                    (pl.col("MATCHING_STATUS") == "MATCHED")
                )
                carry_forward_matched_amount = carry_forward_matched_df["DC_AMOUNT"].sum()
                carry_forward_matched_count = carry_forward_matched_df.height

                carry_forward_unmatched_df = combined_df.filter(
                    (pl.col("GL_NUMBER") == account) &
                    (pl.col("SOURCE") == source) &
                    (pl.col("CARRY_FORWARD") == "Y") &
                    (pl.col("MATCHING_STATUS") == "UNMATCHED")
                )
                carry_forward_unmatched_amount = carry_forward_unmatched_df["DC_AMOUNT"].sum()
                carry_forward_unmatched_count = carry_forward_unmatched_df.height

                results.append({
                    "GL_NUMBER": account,
                    "SOURCE": source,
                    "Matched_DC_Amount": matched_dc_amount,
                    "Matched_DC_Count": matched_dc_count,
                    "Unmatched_DC_Amount": unmatched_dc_amount,
                    "Unmatched_DC_Count": unmatched_dc_count,
                    "Debit_Matched_Amount": debit_matched_amount,
                    "Debit_Matched_Count": debit_matched_count,
                    "Debit_Unmatched_Amount": debit_unmatched_amount,
                    "Debit_Unmatched_Count": debit_unmatched_count,
                    "Credit_Matched_Amount": credit_matched_amount,
                    "Credit_Matched_Count": credit_matched_count,
                    "Credit_Unmatched_Amount": credit_unmatched_amount,
                    "Credit_Unmatched_Count": credit_unmatched_count,
                    "ReversalAmount": reversal_amount,
                    "ReversalCount": reversal_count,
                    "Debit-ReversalAmount": debit_reversal_amount,
                    "Debit-ReversalCount": debit_reversal_count,
                    "Credit-ReversalAmount": credit_reversal_amount,
                    "Credit-ReversalCount": credit_reversal_count,
                    "Carry-ForwardMatchedAmount": carry_forward_matched_amount,
                    "Carry-ForwardMatchedCount": carry_forward_matched_count,
                    "Carry-ForwardUnmatchedAmount": carry_forward_unmatched_amount,
                    "Carry-ForwardUnmatchedCount": carry_forward_unmatched_count,
                })

        results_df = pl.DataFrame(results)
        return results_df

    def save_to_excel(self, combined_df):
        combined_df.write_excel(self.output_file)

    def generate_report(self):
        gl_df, swift_df = self.load_data()
        processed_data = self.process_data(gl_df, swift_df)
        self.save_to_excel(processed_data)

if __name__ == "__main__":
    report = CombinedReport('ConsolidatedReport.xlsx', 'finalreport.xlsx')
    report.generate_report()
