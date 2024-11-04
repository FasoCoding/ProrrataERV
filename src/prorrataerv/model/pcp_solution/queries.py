# Query para extraer la relación entre barra y central.
# los filtros utilizados son:
# 1. child_obj.class_id = 22 filtra para que el objeto sea de tipo barra
# 2. parent_obj.class_id = 2 filtra para que el objeto sea de tipo central
# 3. category_id IN (95, 96, 99, 100) son centrales de tipo eólico, solar, hydro B y hydro C.
# 4. collection_id = 12 colección de datos entre generador y barra.
GET_OBJ_RELATIONSHIP = """
SELECT 
    parent_obj.name AS parent_obj_name, 
    child_obj.name AS child_obj_name
FROM ((t_membership
INNER JOIN t_object as child_obj ON t_membership.child_object_id = child_obj.object_id)
INNER JOIN t_object as parent_obj ON t_membership.parent_object_id = parent_obj.object_id)
WHERE 1=1 
    AND t_membership.collection_id = :collection_id
;
"""

# Query para extraer las barras con costos marginales negativos.
# los filtros utilizados son:
# 1. collection_id = 245 es la colección de datos de barras.
# 2. property_id = 1233 es la propiedad de precio para la colección de barra.
# 3. value < 0 filtra para que solo aparezcan las barras con costos marginales menor a cero.
# 4. day_id = 1 solo extrae datos del día 1.
GET_COLLECTION_DATA = """
SELECT 
    t_collection.name as collection,
    t_property.name AS property,
    t_child.name AS name,
    t_period_0.datetime,
    t_data_0.value
FROM ((((((((t_membership
INNER JOIN t_collection ON t_membership.collection_id = t_collection.collection_id)
INNER JOIN t_object AS t_parent ON t_membership.parent_object_id = t_parent.object_id)
INNER JOIN t_object AS t_child ON t_membership.child_object_id = t_child.object_id)
INNER JOIN t_property ON t_collection.collection_id = t_property.collection_id)
INNER JOIN t_key ON t_membership.membership_id = t_key.membership_id AND t_property.property_id = t_key.property_id)
INNER JOIN t_data_0 ON t_key.key_id = t_data_0.key_id)
INNER JOIN t_phase_3 ON t_data_0.period_id = t_phase_3.period_id)
INNER JOIN t_period_0 ON t_phase_3.interval_id = t_period_0.interval_id)
INNER JOIN t_category ON t_child.category_id = t_category.category_id
WHERE 1=1
    AND t_collection.collection_id = :collection_id
    AND t_period_0.day_id = 1
;
"""

GET_PROPERTY_DATA = """
SELECT 
    t_collection.name as collection,
    t_property.name AS property,
    t_child.name AS name,
    t_period_0.datetime,
    t_data_0.value
FROM ((((((((t_membership
INNER JOIN t_collection ON t_membership.collection_id = t_collection.collection_id)
INNER JOIN t_object AS t_parent ON t_membership.parent_object_id = t_parent.object_id)
INNER JOIN t_object AS t_child ON t_membership.child_object_id = t_child.object_id)
INNER JOIN t_property ON t_collection.collection_id = t_property.collection_id)
INNER JOIN t_key ON t_membership.membership_id = t_key.membership_id AND t_property.property_id = t_key.property_id)
INNER JOIN t_data_0 ON t_key.key_id = t_data_0.key_id)
INNER JOIN t_phase_3 ON t_data_0.period_id = t_phase_3.period_id)
INNER JOIN t_period_0 ON t_phase_3.interval_id = t_period_0.interval_id)
INNER JOIN t_category ON t_child.category_id = t_category.category_id
WHERE 1=1
    AND t_collection.collection_id = :collection_id 
    AND t_property.name = :property_name
    AND t_period_0.day_id = 1
;
"""

# Query para extraer los datos referente a las centrales.
# los filtros utilizados son:
# 1. collection_id = 1 es la colección de datos para centrales.
# 2. property_id IN (1, 6, 28, 200, 219) propiedades de Generation, Units generating, Capacity Curtailed, Max Capacity, Available Capacity.
# 3. category_id IN (95, 96, 99, 100) son centrales de tipo eólico, solar, hydro B y hydro C.
# 4. day_id = 1 solo extrae datos del día 1.
GET_TDATA_PROPERTY = """
SELECT 
    t_collection.name as collection,
    t_property.name AS property,
    t_child.name AS generator,
    t_period_0.datetime,
    t_data_0.key_id AS key_id,
    t_data_0.period_id AS period_id,
    t_data_0.value
FROM ((((((((t_membership
INNER JOIN t_collection ON t_membership.collection_id = t_collection.collection_id)
INNER JOIN t_object AS t_parent ON t_membership.parent_object_id = t_parent.object_id)
INNER JOIN t_object AS t_child ON t_membership.child_object_id = t_child.object_id)
INNER JOIN t_property ON t_collection.collection_id = t_property.collection_id)
INNER JOIN t_key ON t_membership.membership_id = t_key.membership_id AND t_property.property_id = t_key.property_id)
INNER JOIN t_data_0 ON t_key.key_id = t_data_0.key_id)
INNER JOIN t_phase_3 ON t_data_0.period_id = t_phase_3.period_id)
INNER JOIN t_period_0 ON t_phase_3.interval_id = t_period_0.interval_id)
INNER JOIN t_category ON t_child.category_id = t_category.category_id
WHERE 1=1
    AND t_collection.collection_id = 1 
    AND t_property.name = :property_name
    AND t_category.category_id IN :category_ids 
    AND t_period_0.day_id = 1
;
"""
