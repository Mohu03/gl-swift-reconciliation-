import polars as pl

def merged(df, mapping_df, left_on, right_on, selected_columns):
    return df.join(
        mapping_df.select(selected_columns),
        left_on=left_on,
        right_on=right_on,
        how="left"
    )

def apply_filters(df, amount_col, currency_col):
    return (
        df.with_columns([
            pl.col(amount_col).cast(pl.Float64, strict=False),
            pl.when(pl.col(amount_col) < 0).then(pl.lit("Dr")).otherwise(pl.lit("Cr")).alias("Dr/Cr"),
            pl.col(amount_col).abs().alias("DC_AMOUNT")
        ])
        
    )

def process_rules(gl_df, swift_df, rules):
    
    all_matched_gl = []
    all_matched_swift = []
    
    unmatched_gl = gl_df.clone()
    unmatched_swift = swift_df.clone()

    for gl_cols, swift_cols, rule_name in rules:
        if not all(col in unmatched_gl.columns for col in gl_cols):
            print(f"Skipping rule '{rule_name}': GL columns {gl_cols} not found.")
            continue
        if not all(col in unmatched_swift.columns for col in swift_cols):
            print(f"Skipping rule '{rule_name}': SWIFT columns {swift_cols} not found.")
            continue

        matched_keys = unmatched_gl.select(gl_cols).join(
            unmatched_swift.select(swift_cols),
            left_on=gl_cols,
            right_on=swift_cols,
            how="inner"
        ).unique()
        
        if not matched_keys.is_empty():
            matched_gl_current = unmatched_gl.join(matched_keys, on=gl_cols, how='inner').with_columns(
                pl.lit(rule_name).alias("Matching_Rule")
            )

            rename_mapping = {gl_col: swift_col for gl_col, swift_col in zip(gl_cols, swift_cols)}
            renamed_matched_keys = matched_keys.rename(rename_mapping)
            print(f"DEBUG: Columns in renamed_matched_keys for rule '{rule_name}': {renamed_matched_keys.columns}")

            matched_swift_current = unmatched_swift.join(renamed_matched_keys, on=swift_cols, how='inner').with_columns(
                pl.lit(rule_name).alias("Matching_Rule")
            )

            all_matched_gl.append(matched_gl_current)
            all_matched_swift.append(matched_swift_current)

            unmatched_gl = unmatched_gl.join(matched_keys, on=gl_cols, how="anti")
            unmatched_swift = unmatched_swift.join(renamed_matched_keys, on=swift_cols, how="anti")
    
    if all_matched_gl:
        final_matched_gl = pl.concat(all_matched_gl)
    else:
        final_matched_gl = unmatched_gl.clear()
    
    if all_matched_swift:
        final_matched_swift = pl.concat(all_matched_swift)
    else:
        final_matched_swift = unmatched_swift.clear()
    
    final_unmatched_gl = unmatched_gl.with_columns(pl.lit("Unmatched").alias("Matching_Rule"))
    final_unmatched_swift = unmatched_swift.with_columns(pl.lit("Unmatched").alias("Matching_Rule"))

    return final_matched_gl, final_unmatched_gl, final_matched_swift, final_unmatched_swift

def process_data(gl_file, mapping_file, swift_file, output_file):
    
    gl_df = pl.read_csv(gl_file, schema_overrides={
        "Account_Number": pl.Utf8,
        "Nostro/Vostro/ Sett Entity ID": pl.Utf8,
        "Cash Amt": pl.Float64
    })

    mapping_df = pl.read_csv(mapping_file, schema_overrides={
        "Account_Number": pl.Utf8,
        "Sierra Account Numbers": pl.Utf8,
        "Acc_Num": pl.Utf8
    })

    swift_df = pl.read_csv(swift_file, schema_overrides={
        "Account_Number": pl.Utf8,
        "Amount": pl.Float64,
        "Account Currency": pl.Utf8,
        "Institution Reference": pl.Utf8
    }, ignore_errors=True)

    swift_df = swift_df.with_columns([
        pl.col("Nostro Account").cast(pl.Utf8)  
    ])

    gl_df = merged(
        gl_df,
        mapping_df,
        "Nostro/Vostro/ Sett Entity ID",
        "Sierra Account Numbers",
        ["Sierra Account Numbers", "Account Name", "Account Currency", "Account_Number", "Swift Code", "Country"]
    )

    swift_df = merged(
        swift_df,
        mapping_df,
        "Nostro Account",
        "Account_Number",
        ["Account_Number", "Account Name", "Account Currency", "Swift Code", "Country"]
    )

    gl_df = apply_filters(gl_df, "Cash Amt", "Account Currency")
    swift_df = apply_filters(swift_df, "Amount", "Account Currency")

    rules = [
        (["Trans Num", "DC_AMOUNT", "Account Currency"], ["Transation Reference", "DC_AMOUNT", "Account Currency"], "Rule 1"),
        (["ExternalTxNum", "DC_AMOUNT", "Account Currency"], ["Institution Reference", "DC_AMOUNT", "Account Currency"], "Rule 2"),
        (["ExternalTxNum", "DC_AMOUNT", "Account Currency"], ["Transation Reference", "DC_AMOUNT", "Account Currency"], "Rule 3"),
    ]

    matched_gl, unmatched_gl, matched_swift, unmatched_swift = process_rules(gl_df, swift_df, rules)

    final_gl_df = pl.concat([matched_gl, unmatched_gl])
    final_swift_df = pl.concat([matched_swift, unmatched_swift])

    final_gl_df.write_csv(output_file + "_gl.csv")
    final_swift_df.write_csv(output_file + "_swift.csv")

    print("Final GL DataFrame Columns:", final_gl_df.columns)
    print("Final SWIFT DataFrame Columns:", final_swift_df.columns)

process_data(
    gl_file="NOSTRO_GL.csv",
    mapping_file="Nostro_Mapping.csv",
    swift_file="NOSTRO_SWIFT.csv",
    output_file="matched_data"
)
