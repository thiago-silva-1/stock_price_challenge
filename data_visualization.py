import matplotlib.pyplot as plt
import pyspark.sql.functions as F
from spark import spark

gold_path = 'gold_parquet'

df = spark.read.parquet(gold_path)

stocks = [row.asDict() for row in df.select('codigo_acao').distinct().collect()]

for acao in stocks:
    df_plot = df.filter(F.col('codigo_acao') == acao["codigo_acao"])\
                .sort('date', ascending=True)\
                .toPandas()

    plt.plot(df_plot["date"], df_plot["preco"], label=acao["codigo_acao"])

plt.legend(title='stock_name', loc='upper left', bbox_to_anchor=(1.05, 1))
plt.xticks(rotation=45)
plt.subplots_adjust(right=0.75)

plt.xlabel('Date')
plt.ylabel('Preço')
plt.title('Preco x Data (por stock)')

plt.tight_layout()
plt.savefig('stockXdate.png')