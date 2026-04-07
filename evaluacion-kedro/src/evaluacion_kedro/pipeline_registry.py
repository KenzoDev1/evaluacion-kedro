from  evaluacion_kedro.pipelines import data_cleaning # Importamos el módulo (la carpeta)

def register_pipelines():
    # 1. Creamos la variable que guarda el pipeline real
    # Usamos el módulo 'data_cleaning' para llamar a su función 'create_pipeline'
    pipeline_de_limpieza = data_cleaning.create_pipeline()

    # 2. Retornamos el diccionario donde la LLAVE es lo que escribes en la terminal
    return {
        "cleaning": pipeline_de_limpieza, # 'cleaning' es el alias para la terminal
        "__default__": pipeline_de_limpieza # Esto hace que 'kedro run' a secas también limpie
    }