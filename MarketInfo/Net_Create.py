from map_table import create_t

create_t(table='NET_AGREEMENT_MANUAL',
         rule="ID VARCHAR(32) PRIMARY KEY, "
              "PARTITION_KEY VARCHAR(10),"
              "DATA_DATE VARCHAR(10),"
              "CUST_CODE VARCHAR(128),"
              "CUST_NAME VARCHAR(128),"
              "NET_FLAG VARCHAR(2),"
              "PORTFOLIO_NAME VARCHAR(256),"
              "PRODUCT_CODE VARCHAR(32)"
         )
