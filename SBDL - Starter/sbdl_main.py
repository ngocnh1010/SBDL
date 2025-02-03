import sys
from configparser import ConfigParser

from lib import Utils,ConfigLoader
from lib.Transformation import get_address,get_insert_operation
from lib.logger import Log4j

if __name__ == '__main__':

    if len(sys.argv) < 3:
        print("Usage: sbdl {local, qa, prod} {load_date} : Arguments are missing")
        sys.exit(-1)

    job_run_env = sys.argv[1].upper()
    load_date = sys.argv[2]

    spark = Utils.get_spark_session(job_run_env)
    logger = Log4j(spark)
    conf = ConfigLoader.get_config("LOCAL")

    logger.info("Finished creating Spark Session")
    account_csv = 'test_data/accounts/account_samples.csv'
    party_csv = 'test_data/parties/party_samples.csv'
    address_csv = 'test_data/party_address/address_samples.csv'
    account_df = spark.read.csv(account_csv,header= True)
    party_df = spark.read.csv(party_csv,header=True)
    address_df = spark.read.csv(address_csv,header=True)
    # account_df.show(5)
    address_transformed_df = get_address(address_df)

    join_df = account_df\
            .join( party_df, account_df["account_id"] == party_df["account_id"])\
            .join(address_transformed_df, party_df["party_id"] == address_df["party_id"])\
            .select(get_insert_operation(account_df.account_id,'contractIdentifier'),
                    address_transformed_df.partyAddress)



    final_df = join_df.selectExpr("cast(contractIdentifier as String) as key","to_json(struct(*)) as value")\
    # .write\
    # .format("kafka")\
    # .option("kafka.bootstrap.servers","localhost:9092")\
    # .option("topic",conf["kafka.topic"])\
    # .save()

    final_df.show()
    # final_df.show()


