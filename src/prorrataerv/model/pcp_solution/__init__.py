from typing import Optional

import sqlalchemy as sa

from prorrataerv.model.pcp_solution import models, queries


class Querier:
    def __init__(self, conn: sa.engine.Connection):
        self._conn = conn

    def get_obj_relationship(
        self, *, collection_id: int
    ) -> Optional[list[models.ObjectRelationshipSchema]]:
        """obtiene la relacion entre dos tipos de objetos, por ejemplo
        generador y barra de conexión (collection_id = 12).
        Para tener una lista de posibles ids de relación por favor verificar en
        la documentación de plexos o utilizar la tabla t_collection, en donde
        parent_class_id es distintos de 1.

        Args:
            collection_id (int): identificador único del tipo de relación que se busca.

        Returns:
            Optional[list[models.ObjectRelationshipSchema]]: lista de objetos de relación. Al ser tipo opcional
            puede retornar un None. Verificar salida.
        """
        query = sa.text(queries.GET_OBJ_RELATIONSHIP).bindparams(
            sa.bindparam("collection_id", type_=sa.Integer, required=True)
        )
        results = self._conn.execute(query, {"collection_id": collection_id}).all()

        if results is None:
            return None

        return [
            models.ObjectRelationshipSchema(parent_obj=row[0], child_obj=row[1])
            for row in results
        ]

    def get_collection_data(
        self, *, collection_id: int
    ) -> Optional[list[models.CollectionDataSchema]]:
        """obtiene todos los resultados del modelo para una colección en específico, por ejemplo
        para obtener todos los resultados para la colección de generadores, la cual contiene generación,
        capacidad máxima, curtailment, entre otros, se debe usar el collection_id = 1.
        Para tener una lista de posibles ids de colecciones de datos por favor verificar en
        la documentación de plexos o utilizar la tabla t_collection.

        Args:
            collection_id (int): identificador único de la colección a extraer datos.

        Returns:
            Optional[list[models.CollectionDataSchema]]: lista de objetos de datos. Al ser tipo opcional
            puede retornar un None. Verificar salida.
        """
        query = sa.text(queries.GET_COLLECTION_DATA).bindparams(
            sa.bindparam("collection_id", type_=sa.Integer, required=True)
        )
        results = self._conn.execute(query, {"collection_id": collection_id}).all()

        if results is None:
            return None

        return [
            models.CollectionDataSchema(
                name=row[0], property=row[1], datetime=row[2], value=row[3]
            )
            for row in results
        ]
