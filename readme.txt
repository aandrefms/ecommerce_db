Database para Olist e-commerce

- Este script foi desenvolvido com o intuito de criar um database em PostgreSQL para receber um grande número de tabelas
e dados provenientes do e-commerce brasileiro chamado Olist.
- O esquema escolhido foi o snow flakes (floco de neve) pela complexidade e forma como os dados foram distribuidos, afim
de evitar redundância e melhorar desempenho
- O script criará automaticamente o banco de dados (caso não exista), as tabelas (caso não existam) e inserirá os dados.