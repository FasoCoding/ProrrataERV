from enum import Enum
from typing import Optional

import sqlalchemy as sa

from prorrataerv.model.accdb import models, queries


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

    def get_category_list(self, *, category_type: int) -> list[models.CategorySchema]:
        query = sa.text(queries.GET_CATEGORY_LIST).bindparams(
            sa.bindparam("category_type", type_=sa.Integer, required=True),
        )
        results = self._conn.execute(
            query,
            {
                "category_type": category_type,
            },
        ).all()

        if results is None:
            return None

        return [
            models.CategorySchema(category_id=row[0], category_name=row[1])
            for row in results
        ]

    def get_property_data(
        self, *, query_enum: Enum, category_list: list[str]
    ) -> Optional[list[models.ResultSchema]]:
        """obtiene los resultados para el set de colección y propiedad proporcionado en
        query_enum. Un ejemplo para obtener la generación de la central sería:

        query_param = QuerySchema.GENERATOR.GENERATION

        Args:
            query_enum (Enum): Enum con las colecciones y propiedades disponibles
            para consultar.

        Returns:
            Optional[list[models.ResultDataSchema]]: Lista de fila en esquema ResultDataSchema, contiene:
                1. nombre de colección (str)
                2. nombre de la propiedad (str)
                3. nombre del objeto (str)
                4. fecha y hora (datetime)
                5. valor (float)
        """
        collection_id, _, property_name = query_enum.value

        query = sa.text(queries.GET_PROPERTY_DATA).bindparams(
            sa.bindparam("collection_id", type_=sa.Integer, required=True),
            sa.bindparam("property_name", type_=sa.String, required=True),
            sa.bindparam("category_list", expanding=True, required=True),
        )
        results = self._conn.execute(
            query,
            {
                "collection_id": collection_id,
                "property_name": property_name,
                "category_list": category_list,
            },
        ).all()

        if results is None:
            return None

        return [
            models.ResultSchema(
                collection=row[0],
                property=row[1],
                category=row[2],
                name=row[3],
                datetime=row[4],
                value=row[5],
            )
            for row in results
        ]

    def get_property_t_data(
        self, *, query_enum: Enum, category_list: list[str]
    ) -> Optional[list[models.ResultDataSchema]]:
        """obtiene todos los resultados del modelo para la propiedad indicada de una colección en específico
        en el día 1, por ejemplo para obtener todos los resultados de generación para la colección de generadores,
        se debe usar el collection_id = 1 y property_name = "generation". Se cambia el esquema de solución.

        Para tener una lista de posibles ids de colecciones de datos por favor verificar en
        la documentación de plexos o utilizar la tabla t_collection.

        Args:
            query_enum (Enum): Enum con las colecciones y propiedades disponibles
            para consultar.

        Returns:
            Optional[list[models.ResultDataSchema]]: Lista de fila en esquema ResultDataSchema, contiene:
                1. nombre de colección (str)
                2. nombre de la propiedad (str)
                3. nombre del objeto (str)
                4. fecha y hora (datetime)
                5. key_id (int)
                6. period_id (int)
                7. valor (float)
        """
        collection_id, _, property_name = query_enum.value

        query = sa.text(queries.GET_TDATA_PROPERTY).bindparams(
            sa.bindparam("collection_id", type_=sa.Integer, required=True),
            sa.bindparam("property_name", type_=sa.String, required=True),
            sa.bindparam("category_list", expanding=True, required=True),
        )
        results = self._conn.execute(
            query,
            {
                "collection_id": collection_id,
                "property_name": property_name,
                "category_list": category_list,
            },
        ).all()

        if results is None:
            return None

        return [
            models.ResultDataSchema(
                collection=row[0],
                property=row[1],
                category=row[2],
                name=row[3],
                datetime=row[4],
                key_id=row[5],
                period_id=row[6],
                value=row[7],
            )
            for row in results
        ]
