from pyspark.sql import DataFrame

def save_df_into_csv(df:DataFrame, destination:str):
    df.write.options(header='True', delimiter=',', encoding='utf-8', quotes='"') \
        .format('csv') \
        .mode('overwrite') \
        .save(destination)