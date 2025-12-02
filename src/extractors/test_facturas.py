import sqlite3
import pandas as pd

conn = sqlite3.connect(r"C:\Proyectos\DataPulse\db\datapulse.sqlite")

df = pd.read_sql_query('SELECT * FROM "excel_6_control_servicios" LIMIT 5', conn)

print("\n--- COLUMNAS ---")
print(df.columns.tolist())

print("\n--- HEAD ---")
print(df.head())
