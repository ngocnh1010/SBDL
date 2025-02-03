from pyspark.sql.functions import lit, struct, col


def get_insert_operation(col, alias):
    return struct(lit("INSERT").alias("operation"),
            col.alias("newValue"),
            lit(None).alias("oldValue")).alias(alias)

def get_address(df):
    address = struct(col('address_line_1').alias('addressLine1'),
                     col('address_line_2').alias('addressLine2'),
                     col('city').alias('city')
                     )
    return df.select("party_id", get_insert_operation(address,"partyAddress"))