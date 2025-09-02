import psycopg2


conn = psycopg2.connect(
    host="you host",
    database="your db",
    user="user_name",
    password="password",
    port=5432
)
cur = conn.cursor()

create_gl = """
DROP TABLE IF EXISTS nostro_gl_raw;
CREATE TABLE nostro_gl_raw (
    "Nostro/Vostro/ Sett Entity ID" TEXT,
    "Nostro/Vostro/ Sett Entity Cur" TEXT,
    "Val/Settle Date" TEXT,
    "ExternalTxNum" TEXT,
    "MAPS_TRDVERIFY-IMPORT" TEXT,
    "Trade Remarks 1" TEXT,
    "Cash Amt" TEXT,
    "Trans Num" TEXT,
    "FEED_FILE_NAME" TEXT
    
);
"""
cur.execute(create_gl)

create_swift = """
DROP TABLE IF EXISTS nostro_swift_raw;
CREATE TABLE nostro_swift_raw (
    "Value Date" TEXT,
    "Entry Date" TEXT,
    "Amount" TEXT,
    "Transaction Id" TEXT,
    "Transation Reference" TEXT,
    "Institution Reference" TEXT,
    "Custumer Reference" TEXT,
    "Description" TEXT,
    "Opening Balance" TEXT,
    "Opening Balance Date" TEXT,
    "Opening Balance Currency" TEXT,
    "Intermidiate start Balance" TEXT,
    "intermidiate Balance Date" TEXT,
    "intermidiate Balance Currency" TEXT,
    "intermidiate End Balance" TEXT,
    "intermidiate End Date" TEXT,
    "intermidiate End Currency" TEXT,
    "Closing Balance" TEXT,
    "Closing Balance Date" TEXT,
    "Closing Balance Currency" TEXT,
    "Nostro Account" TEXT,
    "Bank Transaction Reference" TEXT,
    "Information" TEXT,
    "FEED_FILE_NAME" TEXT,
    "Currency Dr/Cr" TEXT
);
"""
cur.execute(create_swift)

create_mapping = """
DROP TABLE IF EXISTS nostro_mapping_raw;
CREATE TABLE nostro_mapping_raw (
    "Account Name" TEXT,
    "Account Currency" TEXT,
    "Account_Number" TEXT,
    "Swift Code" TEXT,
    "Sierra Account Numbers" TEXT,
    "Country" TEXT,
    "ReconName" TEXT,
    "Reconid" TEXT,
    "ReconProcess" TEXT,
    "Source" TEXT,
    "ClosingBalance" TEXT,
    "Account Name Curr" TEXT,
    "Acc_Num" TEXT
);
"""
cur.execute(create_mapping)

conn.commit()

files = {
    "nostro_gl_raw": "/Users/mac/CascadeProjects/pl_nostro/NOSTRO_GL.csv",
    "nostro_swift_raw": "/Users/mac/CascadeProjects/pl_nostro/NOSTRO_SWIFT.csv",
    "nostro_mapping_raw": "/Users/mac/CascadeProjects/pl_nostro/Nostro_Mapping.csv"
}

for table, file_path in files.items():
    try:
        with open(file_path, "r") as f:
            sql = f"""COPY {table} FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '"'"""
            cur.copy_expert(sql, f)
        print(f"Loaded {file_path} into {table}")
    except Exception as e:
        print(f"Error loading {file_path} into {table}: {e}")

conn.commit()
  
cur.execute("""
ALTER TABLE nostro_gl_raw
ADD COLUMN "Account Name" TEXT,
ADD COLUMN "Account Currency" TEXT,
ADD COLUMN "Account_Number" TEXT,
ADD COLUMN "Swift Code" TEXT,
ADD COLUMN "Country" TEXT;
""")
conn.commit()

# cur.execute("""
# UPDATE nostro_gl_raw n
# SET 
#     "Account Name" = m."Account Name",
#     "Account Currency" = m."Account Currency",
#     "Account_Number" = m."Account_Number",
#     "Swift Code" = m."Swift Code",
#     "Country" = m."Country"
# FROM nostro_mapping_raw m
# WHERE n."Nostro/Vostro/ Sett Entity ID" = m."Sierra Account Numbers"
# """)
# conn.commit()


cur.execute("""
WITH joined AS (
    SELECT 
        n."Nostro/Vostro/ Sett Entity ID" AS id,
        m."Account Name",
        m."Account Currency",
        m."Account_Number",
        m."Swift Code",
        m."Country"
    FROM nostro_gl_raw n
    LEFT JOIN nostro_mapping_raw m
        ON n."Nostro/Vostro/ Sett Entity ID" = m."Sierra Account Numbers"
    )
UPDATE nostro_gl_raw n
SET 
    "Account Name"     = j."Account Name",
    "Account Currency" = j."Account Currency",
    "Account_Number"   = j."Account_Number",
    "Swift Code"       = j."Swift Code",
    "Country"          = j."Country"
FROM joined j
WHERE n."Nostro/Vostro/ Sett Entity ID" = j.id;
""")
conn.commit()



cur.execute("""
ALTER TABLE nostro_gl_raw 
    ADD COLUMN IF NOT EXISTS "Dr/Cr" TEXT,
    ADD COLUMN IF NOT EXISTS "DC_Amount" NUMERIC;
    """)
conn.commit()

cur.execute("""
UPDATE nostro_gl_raw 
SET "Dr/Cr" = CASE 
                  WHEN CAST("Cash Amt" AS NUMERIC) < 0 THEN 'Dr'
                  WHEN CAST("Cash Amt" AS NUMERIC) > 0 THEN 'Cr'
                  
              END,
    "DC_Amount" = ABS(CAST("Cash Amt" AS NUMERIC))


""")
conn.commit()


cur.execute("""
ALTER TABLE nostro_swift_raw
ADD COLUMN "Account Name" TEXT,
ADD COLUMN "Account Currency" TEXT,
ADD COLUMN "Account_Number" TEXT,
ADD COLUMN "Swift Code" TEXT,
ADD COLUMN "Country" TEXT;
""")
# conn.commit()
# cur.execute("""
# UPDATE nostro_swift_raw s
# SET 
#     "Account Name" = m."Account Name",
#     "Account Currency" = m."Account Currency",
#     "Account_Number" = m."Account_Number",
#     "Swift Code" = m."Swift Code",
#     "Country" = m."Country"
# FROM nostro_mapping_raw m
# WHERE s."Nostro Account" = m."Account_Number"
# """)
# conn.commit()

cur.execute("""
WITH joined AS (
    SELECT 
        s."Nostro Account" AS id,
        m."Account Name",
        m."Account Currency",
        m."Account_Number",
        m."Swift Code",
        m."Country"
    FROM nostro_swift_raw s
    LEFT JOIN nostro_mapping_raw m
        ON s."Nostro Account" = m."Account_Number"
)
UPDATE nostro_swift_raw s
SET 
    "Account Name"     = j."Account Name",
    "Account Currency" = j."Account Currency",
    "Account_Number"   = j."Account_Number",
    "Swift Code"       = j."Swift Code",
    "Country"          = j."Country"
FROM joined j
WHERE s."Nostro Account" = j.id;
""")
conn.commit()

# colnames = [desc[0] for desc in cur.description]
# print(colnames)
# exit()

cur.execute("""
ALTER TABLE nostro_swift_raw 
    ADD COLUMN IF NOT EXISTS "Dr/Cr" TEXT,
    ADD COLUMN IF NOT EXISTS "DC_Amount" NUMERIC;
    """
)
conn.commit()

cur.execute("""
UPDATE nostro_swift_raw 
SET "Dr/Cr" = CASE 
                  WHEN CAST("Amount" AS NUMERIC) < 0 THEN 'Dr'
                  WHEN CAST("Amount" AS NUMERIC) > 0 THEN 'Cr'
                  
              END,
    "DC_Amount" = ABS(CAST("Amount" AS NUMERIC))

""")
conn.commit()

cur.execute("""
DELETE FROM nostro_gl_raw 
WHERE "Account Currency" <> 'USD';
""")
conn.commit()

cur.execute("""
DELETE FROM nostro_swift_raw 
WHERE "Account Currency" IS NULL OR "Account Currency" <> 'USD';
""")
conn.commit()


cur.execute("""
DROP TABLE IF EXISTS gl_with_status;
CREATE TABLE gl_with_status AS
SELECT g.*, 
       
       CASE 
           WHEN s."Nostro Account" IS NOT NULL THEN 'Matched' 
           ELSE 'Unmatched' 
       END AS "Match_Status"

FROM nostro_gl_raw g
LEFT JOIN nostro_swift_raw s ON (
    (g."Account_Number" = s."Nostro Account" AND 
     g."Trans Num" = s."Transation Reference" AND
     g."Cash Amt" = s."Amount" AND
     g."Nostro/Vostro/ Sett Entity Cur" = s."Account Currency")
    OR (g."Account_Number" = s."Nostro Account" AND 
        g."ExternalTxNum" = s."Institution Reference" AND
        g."Cash Amt" = s."Amount" AND
        g."Nostro/Vostro/ Sett Entity Cur" = s."Account Currency")
    OR (g."Account_Number" = s."Nostro Account" AND 
        g."ExternalTxNum" = s."Transation Reference" AND
        g."Cash Amt" = s."Amount" AND
        g."Nostro/Vostro/ Sett Entity Cur" = s."Account Currency")
);
""")

cur.execute("""
DROP TABLE IF EXISTS swift_with_status;
CREATE TABLE swift_with_status AS
SELECT s.*, 
       
       CASE 
           WHEN g."Account_Number" IS NOT NULL THEN 'Matched' 
           ELSE 'Unmatched' 
       END AS "Match_Status"
FROM nostro_swift_raw s
LEFT JOIN nostro_gl_raw g ON (
    (g."Account_Number" = s."Nostro Account" AND 
     g."Trans Num" = s."Transation Reference" AND
     g."Cash Amt" = s."Amount" AND
     g."Nostro/Vostro/ Sett Entity Cur" = s."Account Currency")
    OR (g."Account_Number" = s."Nostro Account" AND 
        g."ExternalTxNum" = s."Institution Reference" AND
        g."Cash Amt" = s."Amount" AND
        g."Nostro/Vostro/ Sett Entity Cur" = s."Account Currency")
    OR (g."Account_Number" = s."Nostro Account" AND 
        g."ExternalTxNum" = s."Transation Reference" AND
        g."Cash Amt" = s."Amount" AND
        g."Nostro/Vostro/ Sett Entity Cur" = s."Account Currency")
);
""")

conn.commit()

cur.close()
conn.close()
