import pandas as pd
from sqlalchemy import create_engine
import construir_database

# Fazer a importação dos datasets
data_orders = pd.read_csv('./input/olist_orders_dataset.csv')
data_reviews = pd.read_csv('./input/olist_order_reviews_dataset.csv')
data_payments = pd.read_csv('./input/olist_order_payments_dataset.csv')
data_items = pd.read_csv('./input/olist_order_items_dataset.csv')
data_products = pd.read_csv('./input/olist_products_dataset.csv')
data_customers = pd.read_csv('./input/olist_customers_dataset.csv')
data_geolocation = pd.read_csv('./input/olist_geolocation_dataset.csv')
data_sellers = pd.read_csv('./input/olist_sellers_dataset.csv')

senha = input('Digite sua senha ')
construir_database.criar_database(senha=senha)
lista_dados = [data_payments, data_geolocation, data_customers, data_orders, data_reviews, data_products,
               data_sellers, data_items]
lista_colunas = ['payment', 'geolocation', 'customer', 'order', 'review', 'product', 'seller', 'item']


def inserir_valores(data, nome_tabela):
    engine = create_engine(f'postgresql://postgres:{senha}@127.0.0.1/ecommerce')
    data.to_sql(nome_tabela, con=engine, if_exists='append', chunksize=1000, index=False)


for num, item in enumerate(lista_dados):
    inserir_valores(data=item, nome_tabela=lista_colunas[num])
    




