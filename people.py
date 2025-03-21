from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("animals")\
        .getOrCreate()

    print("Leyendo animals.csv ... ")
    path_animals = "dataset.csv"  # Asegúrate de que este es el nombre correcto del archivo CSV
    df_animals = spark.read.csv(path_animals, header=True, inferSchema=True, sep=';')
    
    # Renombrar columnas con espacios o caracteres especiales si es necesario
    df_animals = df_animals.withColumnRenamed("Tierart", "animal")
    df_animals = df_animals.withColumnRenamed("ist bedroht?", "endangered")
    df_animals = df_animals.withColumnRenamed("Chipnummer", "chip_number")
    df_animals = df_animals.withColumnRenamed("Lebensraum", "habitat")
    df_animals = df_animals.withColumnRenamed("Größe", "size")
    df_animals = df_animals.withColumnRenamed("Herkunft", "origin")
    df_animals = df_animals.withColumnRenamed("Fressverhalten", "food")
    
    df_animals.createOrReplaceTempView("animals")
    
    # Descripción de la tabla
    query = 'DESCRIBE animals'
    spark.sql(query).show(20)
    
    # Consulta: Mostrar nombre y hábitat de los animales en peligro
    query = """
    SELECT *
    FROM animals
    ORDER BY Name
    """
    df_endangered = spark.sql(query)
    df_endangered.show(20)
    
    # Guardar los resultados en JSON
    results = df_endangered.toJSON().collect()
    with open('results/animals_endangered.json', 'w') as file:
        json.dump(results, file)
    
    # Contar cuántos animales hay por tipo
    query = "SELECT Tierart, COUNT(*) FROM animals GROUP BY Tierart"
    df_species_count = spark.sql(query)
    df_species_count.show()
    
    spark.stop()
