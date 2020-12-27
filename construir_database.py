import psycopg2


def criar_database(senha):
    # Estabelecendo conexao ao database postgres
    conn = psycopg2.connect(
        database="postgres", user='postgres', password=senha, host='127.0.0.1', port='5432'
    )
    conn.autocommit = True
    cursor = conn.cursor()
    
    # Verificar a Existencia do database
    cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'ecommerce'")
    exists = cursor.fetchone()
    
    # Se nao existir, criar o database
    if not exists:
        cursor.execute('CREATE DATABASE ecommerce')
    
    # Conectando ao database criado
    conn1 = psycopg2.connect(
        database="ecommerce", user='postgres', password=senha, host='127.0.0.1', port='5432'
    )
    conn1.autocommit = True
    cursor1 = conn1.cursor()

    sql = \
        ('''
            CREATE TABLE IF NOT EXISTS payment(
            order_id VARCHAR(50),
            payment_sequential INT NOT NULL,
            payment_type VARCHAR(50) NOT NULL,
            payment_installments INT NOT NULL,
            payment_value FLOAT NOT NULL
            )''','''
            CREATE TABLE IF NOT EXISTS geolocation(
            geolocation_zip_code_prefix INT NOT NULL,
            geolocation_lat FLOAT NOT NULL,
            geolocation_lng FLOAT NOT NULL,
            geolocation_city VARCHAR(50) NOT NULL,
            geolocation_state VARCHAR(2) NOT NULL
            )''', '''
            CREATE TABLE IF NOT EXISTS customer(
            customer_id VARCHAR(50) PRIMARY KEY,
            customer_unique_id VARCHAR(50) NOT NULL,
            customer_zip_code_prefix INT REFERENCES geolocation(geolocation_zip_code_prefix),
            customer_city VARCHAR(30) NOT NULL,
            customer_state VARCHAR(3) NOT NULL
            )
            ''', '''
            CREATE TABLE IF NOT EXISTS order(
            order_id VARCHAR(50) NOT NULL REFERENCES payment (order_id),
            customer_id VARCHAR(50) NOT NULL REFERENCES customer (customer_id),
            order_status VARCHAR(20) NOT NULL,
            order_purchase_timestamp TIMESTAMP with time zone,
            order_approved_at TIMESTAMP with time zone,
            order_delivered_carrier_date TIMESTAMP with time zone,
            order_delivered_customer_date TIMESTAMP with time zone,
            order_estimated_delivery_date TIMESTAMP with time zone
            )
            ''', '''
            CREATE TABLE IF NOT EXISTS review(
            review_id VARCHAR(50) PRIMARY KEY,
            order_id VARCHAR(50) NOT NULL REFERENCES payment (order_id),
            review_score INT NOT NULL,
            review_comment_title VARCHAR(50),
            review_comment_message VARCHAR(50),
            review_creation_date TIMESTAMP with time zone,
            review_answer_timestamp TIMESTAMP with time zone
            )''', '''
            CREATE TABLE IF NOT EXISTS product(
            product_id VARCHAR(50) PRIMARY KEY,
            product_category_name VARCHAR(50),
            product_name_lenght FLOAT,
            product_description_lenght FLOAT,
            product_photos_qty FLOAT,
            product_weight_g FLOAT,
            product_length_cm FLOAT,
            product_height_cm FLOAT,
            product_width_cm FLOAT
            )''', '''
            CREATE TABLE IF NOT EXISTS seller(
            seller_id VARCHAR(50) PRIMARY KEY,
            seller_zip_code_prefix INT NOT NULL,
            seller_city VARCHAR(50) NOT NULL,
            seller_state VARCHAR(2) NOT NULL
            )''', '''
            CREATE TABLE IF NOT EXISTS item(
            order_id VARCHAR(50) NOT NULL REFERENCES payment (order_id),
            order_item_id INT NOT NULL,
            product_id VARCHAR(50) NOT NULL REFERENCES product (product_id),
            seller_id VARCHAR(50) REFERENCES seller(seller_id),
            shipping_limit_date TIMESTAMP with time zone NOT NULL,
            price FLOAT NOT NULL,
            freight_value FLOAT NOT NULL
            )''')

    # Executar o comando SQL
    try:
        for command in sql:
            cursor1.execute(command)
    
        # Encerrar comunicação com o banco de dados
        cursor1.close()
    
        # Executar as alterações
        conn1.commit()
        print('Database e tabelas criados com sucesso!')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn1.close()