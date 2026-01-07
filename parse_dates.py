def get_settlement_amount(premium: DataFrame, group_cols: list):
    managerial = conn.get_table_from_snowflake('GERENCIAL','SILVER')\
                 .withColumn("CD_APLC",regexp_replace('CODIGO_CONTRATO', r'^[0]*', ''))\
                 .withColumnRenamed("DATA_REFERENCIA","DT_REFR")\
                 .withColumnRenamed("CODIGO_OPERADORA","CD_OPRD")
                 
    cte_prem = (
        premium
        .groupBy(group_cols)
        .agg(functions.sum(functions.coalesce("VL_PREM", functions.lit(0))).alias("VL_PREM"),
             functions.sum(functions.coalesce(
                 "VL_CO_PART", functions.lit(0))).alias("VL_CO_PART"),
             functions.sum(functions.coalesce(
                 "VL_ACRT", functions.lit(0))).alias("VL_ACRT"),
             )
    )
                
    df_acrt = cte_prem.join(managerial, group_cols)\
                 .select(*group_cols,
                          when((coalesce(col("VL_PREM"),lit(0)) + 
                                coalesce(col("VL_CO_PART"),lit(0))  + 
                                coalesce(col("VL_ACRT"),lit(0))) != 0,
                                (col("VALOR_RECEITA") - (coalesce(col("VL_PREM"),lit(0)) 
                                                         + coalesce(col("VL_CO_PART"),lit(0)) 
                                                         + coalesce(col("VL_ACRT"),lit(0)))))\
                         .otherwise(lit(0)).alias("VL_ACRT_AJUSTAR")
                        ).distinct()
    return df_acrt


def distribute_adjustments(
    df_main,
    df_ajustes,
    keys,
    col_acrt="VL_ACRT",
    col_ajuste="VL_ACRT_AJUSTAR"
):
    original_cols = df_main.columns
    df = (
        df_main
        .join(df_ajustes.select(*keys, col_ajuste), keys, "left")
        .withColumn(col_ajuste, functions.coalesce(functions.col(col_ajuste), functions.lit(0.0)))
        .withColumn(col_acrt, functions.coalesce(functions.col(col_acrt), functions.lit(0.0)))
    )
    w_grp = Window.partitionBy(*keys)

    df = df.withColumn(
        "_has_positive",
        functions.max(functions.when(functions.col(
            col_acrt) > 0, 1).otherwise(0)).over(w_grp)
    )

    df = df.withColumn(
        "_peso_base",
        functions.when(
            functions.col("_has_positive") == 1,
            functions.when(functions.col(col_acrt) > 0, functions.col(
                col_acrt)).otherwise(functions.lit(1.0))
        ).otherwise(functions.lit(1.0))
    )

    df = df.withColumn("_sum_peso", functions.sum("_peso_base").over(w_grp))
    df = df.withColumn("_peso", functions.col(
        "_peso_base") / functions.col("_sum_peso"))

    df = df.withColumn(
        "_vl_distrib_bruto",
        functions.col(col_ajuste) * functions.col("_peso")
    )
    df = df.withColumn(
        "_vl_distrib",
        functions.floor(functions.col("_vl_distrib_bruto") * 100) / 100
    )
    df = df.withColumn("_sum_distrib", functions.sum(
        "_vl_distrib").over(w_grp))
    df = df.withColumn(
        "_dif",
        functions.round(functions.col(col_ajuste) -
                        functions.col("_sum_distrib"), 2)
    )
    w_fix = Window.partitionBy(*keys).orderBy(functions.col("_peso").desc())
    df = df.withColumn("_rn", functions.row_number().over(w_fix))
    df = df.withColumn(
        "_vl_distrib_final",
        functions.when(functions.col("_rn") == 1, functions.col(
            "_vl_distrib") + functions.col("_dif"))
        .otherwise(functions.col("_vl_distrib"))
    )
    df = df.withColumn(
        col_acrt,
        functions.round(functions.col(col_acrt) +
                        functions.col("_vl_distrib_final"), 2)
    )
    return df.select(*original_cols)
