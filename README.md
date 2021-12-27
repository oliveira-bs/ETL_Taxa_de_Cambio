## Projeto: ETL via API para Taxa de Cambio

No mundo todo, são diversas moedas correntes e elas podem afetar a nossa vida.  

Para entender a economia de uma região precisamos entender a precificação da nossa moeda em relação a outra no mundo, em um mundo globalizado,
é necessário estabelecer um parâmetro de conversão de uma moeda estrangeira para aquela corrente em um determinado país. As principais moedas 
para estabelecermos esse conhecimento são: dolar, euro e o bitcoin.  

Obter o historico de valores dessas moedas comparadas ao Real ajuda a planejar algumas tomadas de decisão em nosso cotidiano, por exemplo:
- O preço de commodities do mercado internacional
- O preço dos alimentos no supermecado local
- A variação do preço de produtos importados/exportados
- O poder de compra do cliente de lojas tradicionais e e-commerce
- O reajusto da margem de lucro sobre o produto para manter a competitividade no mercado
- Avaliar o investimento em ações e titulos

Para adquirir os histórico monetários criamos um mecanismo de ETL via API para armazenar esses dados no formato parquet. A orquestração do
ETL é realizada com o Apache Airflow em conjunto com Spark

![image](https://user-images.githubusercontent.com/68130436/147495712-254d4fdd-764b-483d-a0d1-cbe72c1e0763.png)
#### Processo de ETL com Apache Airflow

