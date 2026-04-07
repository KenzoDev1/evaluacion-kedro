from kedro.pipeline import Pipeline, node, pipeline
# Importamos tus funciones de limpieza desde nodes.py
from .nodes import limpiar_pacientes, limpiar_consultas 

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=limpiar_pacientes,
            inputs="pacientes_raw",
            outputs="pacientes_cleaned",
            name="nodo_limpiar_pacientes",
        ),
        node(
            func=limpiar_consultas,
            inputs="consultas_raw",
            outputs="consultas_cleaned",
            name="nodo_limpiar_consultas",
        ),
    ])