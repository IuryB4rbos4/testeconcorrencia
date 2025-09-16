import os
import pandas as pd
import psycopg2
from pymongo import MongoClient

base_path = "/app/dataset"

# ---------------------------
# 1. Carregar CSVs
# ---------------------------
csv_files = [
    "olist_customers_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "product_category_name_translation.csv",
    "olist_geolocation_dataset.csv"
]

dfs = {}
for file in csv_files:
    table_name = os.path.splitext(file)[0]
    dfs[table_name] = pd.read_csv(f"{base_path}/{file}")
    print(f"✔ Carregado {file} -> {table_name}")

# ---------------------------
# 2. Conectar PostgreSQL
# ---------------------------
print("📦 Conectando ao PostgreSQL...")
pg_conn = psycopg2.connect(
    dbname="sbtest",
    user="pguser",
    password="pgpass",
    host="postgres",
    port=5432
)
pg_cur = pg_conn.cursor()

# ---------------------------
# 3. Helpers para tipos e PKs
# ---------------------------
def infer_column_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):
        return "NUMERIC"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "VARCHAR"

primary_keys = {
    "olist_customers_dataset": "customer_id",
    "olist_orders_dataset": "order_id",
    "olist_order_items_dataset": ["order_id", "order_item_id"],
    "olist_products_dataset": "product_id",
    "olist_sellers_dataset": "seller_id",
    "olist_order_payments_dataset": ["order_id", "payment_sequential"],
    "olist_order_reviews_dataset": "review_id",
    "product_category_name_translation": "product_category_name",
    "olist_geolocation_dataset": "geolocation_zip_code_prefix"
}

foreign_keys = {
    "olist_orders_dataset": [("customer_id", "olist_customers_dataset", "customer_id")],
    "olist_order_items_dataset": [
        ("order_id", "olist_orders_dataset", "order_id"),
        ("product_id", "olist_products_dataset", "product_id"),
        ("seller_id", "olist_sellers_dataset", "seller_id")
    ],
    "olist_order_payments_dataset": [("order_id", "olist_orders_dataset", "order_id")],
    "olist_order_reviews_dataset": [("order_id", "olist_orders_dataset", "order_id")]
}

# ---------------------------
# 4. Criar tabelas sem FKs
# ---------------------------
for table_name, df in dfs.items():
    col_defs = []
    for col, dtype in zip(df.columns, df.dtypes):
        col_type = infer_column_type(dtype)
        col_defs.append(f"{col} {col_type}")
    
    pk = primary_keys.get(table_name)
    if isinstance(pk, list):
        pk_def = f", PRIMARY KEY ({', '.join(pk)})"
    else:
        pk_def = f", PRIMARY KEY ({pk})" if pk else ""
    
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(col_defs)}{pk_def});"
    pg_cur.execute(sql)
    print(f"✔ Tabela {table_name} criada no PostgreSQL")
pg_conn.commit()

# ---------------------------
# 5. Adicionar foreign keys
# ---------------------------
for table_name, fks in foreign_keys.items():
    for fk_col, ref_table, ref_col in fks:
        sql = f"""
        ALTER TABLE {table_name}
        ADD CONSTRAINT fk_{table_name}_{fk_col}
        FOREIGN KEY ({fk_col}) REFERENCES {ref_table}({ref_col})
        ON DELETE SET NULL
        ON UPDATE CASCADE;
        """
        try:
            pg_cur.execute(sql)
            print(f"✔ FK adicionada em {table_name}: {fk_col} -> {ref_table}({ref_col})")
        except psycopg2.errors.DuplicateObject:
            pg_conn.rollback()  # FK já existe
pg_conn.commit()

# ---------------------------
# 6. Inserir dados no PostgreSQL
# ---------------------------

def insert_df_no_fk(table, df):
    """Insere dados em tabelas sem foreign keys"""
    cols = df.columns
    placeholders = ','.join(['%s'] * len(cols))
    colnames = ','.join(cols)
    sql = f"INSERT INTO {table} ({colnames}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

    for _, row in df.iterrows():
        pg_cur.execute(sql, tuple(row[col] for col in cols))
    pg_conn.commit()
    print(f"✔ Dados inseridos em {table} (sem FK)")

def insert_df_with_fk(table, df, fk_cols):
    """Insere dados em tabelas com foreign keys, ignorando linhas inválidas"""
    cols = df.columns
    placeholders = ','.join(['%s'] * len(cols))
    colnames = ','.join(cols)
    sql = f"INSERT INTO {table} ({colnames}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

    for _, row in df.iterrows():
        # Verifica se FK existe antes de inserir
        valid = True
        for fk_col, ref_table, ref_col in fk_cols.get(table, []):
            pg_cur.execute(f"SELECT 1 FROM {ref_table} WHERE {ref_col} = %s", (row[fk_col],))
            if not pg_cur.fetchone():
                valid = False
                break
        if valid:
            pg_cur.execute(sql, tuple(row[col] for col in cols))
    pg_conn.commit()
    print(f"✔ Dados inseridos em {table} (com FK)")

# Inserção em ordem: primeiro sem FK, depois com FK
tables_no_fk = ["olist_customers_dataset", "olist_products_dataset", "olist_sellers_dataset",
                "product_category_name_translation", "olist_geolocation_dataset"]
tables_with_fk = ["olist_orders_dataset", "olist_order_items_dataset",
                  "olist_order_payments_dataset", "olist_order_reviews_dataset"]

for table in tables_no_fk:
    insert_df_no_fk(table, dfs[table])

for table in tables_with_fk:
    insert_df_with_fk(table, dfs[table], foreign_keys)

print("✅ PostgreSQL carregado com sucesso!")

# ---------------------------
# 7. Conectar MongoDB
# ---------------------------
print("📦 Conectando ao MongoDB...")
mongo = MongoClient("mongodb://mongoadmin:mongopass@mongo:27017/")
db = mongo["olist"]

for table_name, df in dfs.items():
    records = df.to_dict(orient="records")
    db[table_name].insert_many(records)
    print(f"✔ Dados inseridos em MongoDB: {table_name}")

print("✅ MongoDB carregado com sucesso! ETL finalizado.")