import streamlit as st
import duckdb
import pandas as pd

st.set_page_config(page_title="E-commerce Dashboard", layout="wide")

DUCKDB_PATH = "warehouse.duckdb"

# --- Chargement des données (en lecture seule pour éviter le verrou) ---
@st.cache_data
def load_data(query: str) -> pd.DataFrame:
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    df = con.execute(query).df()
    con.close()
    return df

# --- Titre ---
st.title("📊 Dashboard E-commerce")

# --- KPIs ---
customer_rev = load_data("SELECT * FROM main.customer_revenue")
category_rev = load_data("SELECT * FROM main.category_revenue")

col1, col2, col3 = st.columns(3)
col1.metric("Chiffre d'affaires total", f"{customer_rev['total_revenue'].sum():,.2f} €")
col2.metric("Nombre de clients", f"{customer_rev['customer_id'].nunique()}")
col3.metric("Commandes totales", f"{int(customer_rev['nb_orders'].sum())}")

# --- CA par catégorie ---
st.subheader("Chiffre d'affaires par catégorie")
st.bar_chart(category_rev.set_index("category")["total_revenue"])

# --- Top clients ---
st.subheader("Top 10 clients")
st.dataframe(
    customer_rev.sort_values("total_revenue", ascending=False).head(10),
    use_container_width=True,
)

# --- Filtre par segment ---
st.subheader("Clients par segment")
segments = customer_rev["segment"].unique().tolist()
choix = st.multiselect("Segment", segments, default=segments)
filtre = customer_rev[customer_rev["segment"].isin(choix)]
st.dataframe(filtre, use_container_width=True)