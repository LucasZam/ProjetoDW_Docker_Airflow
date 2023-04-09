# Lab 5 - Job ETL

# Imports
import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

# Argumentos
args = {'owner': 'airflow'}

# Argumentos default
default_args = {
    'owner': 'airflow',    
    #'start_date': airflow.utils.dates.days_ago(2),
    #'end_date': datetime(),
    #'depends_on_past': False,
    #'email': [''],
    #'email_on_failure': True,
    #'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes = 5),
}

# Cria a DAG
dag_dw_dsa = DAG(dag_id = "Projeto1_DW",
                   default_args = default_args,
                   # schedule_interval='0 0 * * *',
                   schedule_interval = '@once',  
                   dagrun_timeout = timedelta(minutes = 60),
                   description = 'Job ETL de Carga no DW com Airflow',
                   start_date = airflow.utils.dates.days_ago(1)
)



# SQL para truncar a tabela tempo
sql_trunca_tempo = 'TRUNCATE TABLE schema3.dim_tempo CASCADE;'

# SQL de insert na tabela tempo
sql_insere_tempo = '''INSERT INTO schema3.dim_tempo (ano, mes, dia, data_completa)
                        SELECT EXTRACT(YEAR FROM d)::INT, 
                            EXTRACT(MONTH FROM d)::INT, 
                            EXTRACT(DAY FROM d)::INT, d::DATE
                        FROM generate_series('2020-01-01'::DATE, '2024-12-31'::DATE, '1 day'::INTERVAL) d;'''


# Tarefa para truncar a tabela tempo
trunca_dados_tempo = PostgresOperator(sql = sql_trunca_tempo,
                                task_id = 'tarefa_trunca_dados_tempo',
                                postgres_conn_id = 'postgresDB',
                                dag = dag_dw_dsa
)

# Tarefa de insert na tabela tempo
insere_dados_tempo = PostgresOperator(sql = sql_insere_tempo,
                                task_id = 'tarefa_insere_dados_tempo',
                                postgres_conn_id = 'postgresDB',
                                dag = dag_dw_dsa
)


# SQL para truncar a tabela cliente
sql_trunca_cliente = 'TRUNCATE TABLE schema3.dim_cliente CASCADE;'

# SQL para insert a tabela cliente
sql_insere_cliente = '''INSERT INTO schema3.dim_cliente (id_cliente, nome, tipo)
                            SELECT id_cliente, 
                                nome_cliente, 
                                nome_tipo
                            FROM schema2.st_ft_clientes tb1, schema2.st_ft_tipo_cliente tb2
                            WHERE tb2.id_tipo = tb1.id_tipo;'''


# Tarefa de insert na tabela
trunca_dados_cliente = PostgresOperator(sql = sql_trunca_cliente,
                                task_id = 'tarefa_trunca_dados_cliente',
                                postgres_conn_id = 'postgresDB',
                                dag = dag_dw_dsa
)

# Tarefa de insert na tabela
insere_dados_cliente = PostgresOperator(sql = sql_insere_cliente,
                                task_id = 'tarefa_insere_dados_cliente',
                                postgres_conn_id = 'postgresDB',
                                dag = dag_dw_dsa
)


# SQL para truncar a tabela produto
sql_trunca_produto = 'TRUNCATE TABLE schema3.dim_produto CASCADE;'

# SQL para truncar a tabela produto
sql_insere_produto = '''INSERT INTO schema3.dim_produto (id_produto, nome_produto, categoria, subcategoria)
                            SELECT id_produto, 
                                nome_produto, 
                                nome_categoria, 
                                nome_subcategoria
                            FROM schema2.st_ft_produtos tb1, schema2.st_ft_subcategorias tb2, schema2.st_ft_categorias tb3
                            WHERE tb3.id_categoria = tb2.id_categoria
                            AND tb2.id_subcategoria = tb1.id_subcategoria;'''


# Tarefa de insert na tabela
trunca_dados_produto = PostgresOperator(sql = sql_trunca_produto,
                                task_id = 'tarefa_trunca_dados_produto',
                                postgres_conn_id = 'postgresDB',
                                dag = dag_dw_dsa
)

# Tarefa de insert na tabela
insere_dados_produto = PostgresOperator(sql = sql_insere_produto,
                                task_id = 'tarefa_insere_dados_produto',
                                postgres_conn_id = 'postgresDB',
                                dag = dag_dw_dsa
)


# SQL para truncar a tabela localidade
sql_trunca_localidade = 'TRUNCATE TABLE schema3.dim_localidade CASCADE;'

# SQL para insert a tabela localidade
sql_insere_localidade = '''INSERT INTO schema3.dim_localidade (id_localidade, pais, regiao, estado, cidade)
                            SELECT id_localidade, 
                                pais, 
                                regiao, 
                                CASE
                                    WHEN nome_cidade = 'Natal' THEN 'Rio Grande do Norte'
                                    WHEN nome_cidade = 'Rio de Janeiro' THEN 'Rio de Janeiro'
                                    WHEN nome_cidade = 'Belo Horizonte' THEN 'Minas Gerais'
                                    WHEN nome_cidade = 'Salvador' THEN 'Bahia'
                                    WHEN nome_cidade = 'Blumenau' THEN 'Santa Catarina'
                                    WHEN nome_cidade = 'Curitiba' THEN 'Paraná'
                                    WHEN nome_cidade = 'Fortaleza' THEN 'Ceará'
                                    WHEN nome_cidade = 'Recife' THEN 'Pernambuco'
                                    WHEN nome_cidade = 'Porto Alegre' THEN 'Rio Grande do Sul'
                                    WHEN nome_cidade = 'Manaus' THEN 'Amazonas'
                                END estado, 
                                nome_cidade
                            FROM schema2.st_ft_localidades tb1, schema2.st_ft_cidades tb2
                            WHERE tb2.id_cidade = tb1.id_cidade;'''


# Tarefa de insert na tabela
trunca_dados_localidade = PostgresOperator(sql = sql_trunca_localidade,
                                task_id = 'tarefa_trunca_dados_localidade',
                                postgres_conn_id = 'postgresDB',
                                dag = dag_dw_dsa
)

# Tarefa de insert na tabela
insere_dados_localidade = PostgresOperator(sql = sql_insere_localidade,
                                task_id = 'tarefa_insere_dados_localidade',
                                postgres_conn_id = 'postgresDB',
                                dag = dag_dw_dsa
)



# Instrução SQL para truncar a tabela fato
sql_trunca_fato = 'TRUNCATE TABLE schema3.fato_vendas;'

# Instrução SQL de insert na tabela fato
sql_insere_dados_fato = """
INSERT INTO schema3.fato_vendas (sk_produto, 
                                sk_cliente, 
                                sk_localidade, 
                                sk_tempo, 
                                quantidade, 
                                preco_venda, 
                                custo_produto, 
                                receita_vendas,
                                resultado)
                            SELECT sk_produto,
                                sk_cliente,
                                sk_localidade,
                                sk_tempo, 
                                SUM(quantidade) AS quantidade, 
                                SUM(preco_venda) AS preco_venda, 
                                SUM(custo_produto) AS custo_produto, 
                                SUM(ROUND((CAST(quantidade AS numeric) * CAST(preco_venda AS numeric)), 2)) AS receita_vendas,
                                SUM(ROUND((CAST(quantidade AS numeric) * CAST(preco_venda AS numeric)), 2) - custo_produto) AS resultado 
                            FROM schema2.st_ft_vendas tb1, 
                                schema2.st_ft_clientes tb2, 
                                schema2.st_ft_localidades tb3, 
                                schema2.st_ft_produtos tb4,
                                schema3.dim_tempo tb5,
                                schema3.dim_produto tb6,
                                schema3.dim_localidade tb7,
                                schema3.dim_cliente tb8
                            WHERE tb2.id_cliente = tb1.id_cliente
                            AND tb3.id_localidade = tb1.id_localizacao
                            AND tb4.id_produto = tb1.id_produto
                            AND to_char(tb1.data_transacao, 'YYYY-MM-DD') = to_char(tb5.data_completa, 'YYYY-MM-DD')
                            AND to_char(tb1.data_transacao, 'HH') = tb5.hora
                            AND tb2.id_cliente = tb8.id_cliente
                            AND tb3.id_localidade = tb7.id_localidade
                            AND tb4.id_produto = tb6.id_produto
                            GROUP BY sk_produto, sk_cliente, sk_localidade, sk_tempo;"""

# Tarefa de insert na tabela fato
trunca_dados_fato = PostgresOperator(sql = sql_trunca_fato,
                                task_id = 'tarefa_trunca_dados_fato',
                                postgres_conn_id = 'postgresDB',
                                dag = dag_dw_dsa
)

# Tarefa de insert na tabela fato
insere_dados_fato = PostgresOperator(sql = sql_insere_dados_fato,
                                task_id = 'tarefa_insere_dados_fato',
                                postgres_conn_id = 'postgresDB',
                                dag = dag_dw_dsa
)



# Fluxo da DAG
trunca_dados_tempo >> insere_dados_tempo >> trunca_dados_cliente >> insere_dados_cliente >> trunca_dados_produto >> insere_dados_produto >> trunca_dados_localidade >> insere_dados_localidade >>trunca_dados_fato >> insere_dados_fato

# Bloco main
if __name__ == "__main__":
    dag_dw_dsa.cli()



