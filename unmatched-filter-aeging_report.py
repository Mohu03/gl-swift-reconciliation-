import polars as pl

class AegingReport:
    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file

    def load_data(self):
        gl_df = pl.read_excel(self.input_file, sheet_name="NOSTRO_GL")
        swift_df = pl.read_excel(self.input_file, sheet_name="NOSTRO_SWIFT")
        return gl_df, swift_df

    def process_data(self, gl_df, swift_df):
        columns = ["GL_NUMBER", "SOURCE", "CURRENCY", "AGEING", "MATCHING_STATUS","DC_AMOUNT"]
        
        combined_df = pl.concat([gl_df.select(columns), swift_df.select(columns)], how="vertical")
        unique_accounts = combined_df.select("GL_NUMBER").unique().to_series().to_list()
        results = []

        for account in unique_accounts:
            account_df = combined_df.filter(pl.col("GL_NUMBER") == account)
            currencies = account_df.select("CURRENCY").unique().to_series().to_list()
            
            for currency in currencies:
                for source in ["NOSTRO_GL", "NOSTRO_SWIFT"]:
                    unmatched_df = combined_df.filter(
                        (pl.col("GL_NUMBER") == account) &
                        (pl.col("SOURCE") == source) &
                        (pl.col("CURRENCY") == currency) &

                        (pl.col("MATCHING_STATUS") == "UNMATCHED")
                    )
                    
                    aging_counts = {
                        "0-5 day No.": unmatched_df.filter(pl.col("AGEING").is_between(0, 5)).height,
                        "0-5 day Value.": unmatched_df.filter(pl.col("AGEING").is_between(0, 5)).select(pl.sum("DC_AMOUNT")).item(),
                        "6-27 day No.": unmatched_df.filter(pl.col("AGEING").is_between(6, 27)).height,
                        "6-27 day Value.": unmatched_df.filter(pl.col("AGEING").is_between(6, 27)).select(pl.sum("DC_AMOUNT")).item(),
                        "28-59 day No.": unmatched_df.filter(pl.col("AGEING").is_between(28, 59)).height,
                        "28-59 day Value.": unmatched_df.filter(pl.col("AGEING").is_between(28, 59)).select(pl.sum("DC_AMOUNT")).item(),
                        "60+ day No.": unmatched_df.filter(pl.col("AGEING") >= 60).height,
                        "60+ day Value.": unmatched_df.filter(pl.col("AGEING") >= 60).select(pl.sum("DC_AMOUNT")).item(),
                    }
                    total_no = sum(
                        aging_counts[key] for key in aging_counts if key.endswith("day No.")
                    )
                    total_value = sum(
                        aging_counts[key] for key in aging_counts if key.endswith("day Value.")
                    )

                    aging_counts["Total No."] = total_no
                    aging_counts["Total Value."] = total_value

                    
                    results.append({
                        "GL_NUMBER": account,
                        "CURRENCY": currency,
                        "SOURCE": source,
                        **aging_counts
                        
                    })

        return pl.DataFrame(results)
                
    def save_to_excel(self, combined_df):
        combined_df.write_excel(self.output_file)

    def generate_report(self):
        gl_df, swift_df = self.load_data()
        processed_data = self.process_data(gl_df, swift_df)
        self.save_to_excel(processed_data)

if __name__ == "__main__":
    report = AegingReport('ConsolidatedReport.xlsx', 'AegingReport.xlsx')
    report.generate_report()
