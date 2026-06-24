

#QLIR_MANIFEST_LOG=1


poetry run data_server --endpoint klines --symbol SOLUSDT --interval "1s" --log-profile "qlir-info"