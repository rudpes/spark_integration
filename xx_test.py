from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler, StringIndexer, VectorIndexer, OneHotEncoderEstimator

import constants
from pyspark.sql import SparkSession

warehouseLocation = "C:/Users/Spark/spark-warehouse"
spark = SparkSession \
  .builder \
  .appName("app_name") \
  .config("spark.sql.warehouse.dir", warehouseLocation) \
  .enableHiveSupport() \
  .getOrCreate()

for const in constants.tables:
    minimal = spark.sql("SELECT * FROM minimal_" + const)
    drop_cols = ['player_slot', 'matchId']

    minimal = minimal.drop(*drop_cols)

    core_cols = ['time']
    hero_cols = ['hero_name']
    main_cols = constants.main_columns_reduced_2
    win_col = ['win']

    core_assembler = VectorAssembler(inputCols=core_cols, outputCol="core_features")
    main_assembler = VectorAssembler(inputCols=main_cols, outputCol="main_features")

    heroIndexer = StringIndexer(inputCol='hero_name', outputCol='heroIndex')
    hero_encoder = OneHotEncoderEstimator(inputCols=[heroIndexer.getOutputCol()], outputCols=['hero_features'])

    assemblingPipeline = Pipeline(stages=[heroIndexer, hero_encoder, core_assembler, main_assembler])
    minimal = assemblingPipeline.fit(minimal).transform(minimal)

    winIndexer = StringIndexer(inputCol="win", outputCol="winLabel").fit(minimal)

    core_indexer = VectorIndexer(inputCol="core_features", outputCol="core_IndexedFeatures",
                                       maxCategories=2).fit(minimal)
    hero_Indexer = VectorIndexer(inputCol="hero_features", outputCol="hero_IndexedFeatures",
                                       maxCategories=120).fit(minimal)
    main_indexer = VectorIndexer(inputCol="main_features", outputCol="main_IndexedFeatures",
                                       maxCategories=2).fit(minimal)

    indexingPipeline = Pipeline(stages=[winIndexer, core_indexer, hero_Indexer, main_indexer])

    model = indexingPipeline.fit(minimal)
    completeData = model.transform(minimal)

    completeAssembler = VectorAssembler(inputCols=["core_IndexedFeatures" \
        , "hero_IndexedFeatures" \
        , "main_IndexedFeatures"], outputCol="completeIndexedFeatures")

    completeData = completeAssembler.transform(completeData)

    (trainingData, testData) = completeData.randomSplit([0.3, 0.7])

    numberOfTrees = 20
    numberOfMaxDepth = 25

    rf = RandomForestClassifier(labelCol="winLabel", \
                                featuresCol="completeIndexedFeatures", numTrees=numberOfTrees, maxBins=120,
                                maxDepth=numberOfMaxDepth)

    pipeline = Pipeline(stages=[rf])
    model = pipeline.fit(trainingData)

    predictions = model.transform(testData)

    accuracyEvaluator = MulticlassClassificationEvaluator(labelCol="winLabel", predictionCol="prediction",
                                                          metricName="accuracy")
    f1Evaluator = MulticlassClassificationEvaluator(labelCol="winLabel", predictionCol="prediction", metricName="f1")
    weightedPrecisionEvaluator = MulticlassClassificationEvaluator(labelCol="winLabel", \
                                                                   predictionCol="prediction",
                                                                   metricName="weightedPrecision")
    weightedRecallEvaluator = MulticlassClassificationEvaluator(labelCol="winLabel", \
                                                                predictionCol="prediction", metricName="weightedRecall")

    accuracy = accuracyEvaluator.evaluate(predictions)
    f1 = f1Evaluator.evaluate(predictions)
    weightedPrecision = weightedPrecisionEvaluator.evaluate(predictions)
    weightedRecall = weightedRecallEvaluator.evaluate(predictions)

    print('testing  ' + str(const))
    print("Results for numOfTrees:" + str(numberOfTrees) + " depth: " + str(numberOfMaxDepth))
    print("Accuracy = %g" % (accuracy))
    print("Test Error = %g" % (1.0 - accuracy))
    print("F-Score = %g" % (f1))
    print("Weighted Precision = %g" % (weightedPrecision))
    print("Weighted Recall = %g" % (weightedRecall))