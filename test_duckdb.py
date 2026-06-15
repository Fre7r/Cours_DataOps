import duckdb

con = duckdb.connect("warehouse.duckdb")

print(con.execute("SELECT * FROM main.customer_revenue LIMIT 10").df())
print()
print(con.execute("SELECT * FROM main.category_revenue").df())

con.close()

print("DuckDb out")