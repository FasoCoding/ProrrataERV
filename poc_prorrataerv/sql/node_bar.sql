-- CTE:
-- node_obj: Objetos de tipo "node" (barras del sistema)
-- gen_obj: Objetos de tipo "generator" (generadores del sistema), 
--          filtrados por categorías: 95, 96, 99, 100 (Hydro A, Hydro B, Wind, Solar)
WITH node_obj AS (
    SELECT 
        t_object.object_id AS node_id,
        t_object.name AS node,
    FROM t_object
    INNER JOIN t_class ON t_object.class_id = t_class.class_id
    WHERE t_class.name = 'Node'
), gen_obj AS (
    SELECT 
        t_object.object_id AS gen_id,
        t_object.name AS generator,
    FROM t_object
    INNER JOIN t_class ON t_object.class_id = t_class.class_id
    WHERE t_class.name = 'Generator' AND t_object.category_id IN (95, 96, 99, 100)
)

-- Query:
-- extrae relación entre barras y generadores asociados por collection 12 (Node-Gen)
SELECT
    node_obj.node,
    gen_obj.generator,
FROM t_membership
INNER JOIN node_obj ON t_membership.child_object_id = node_obj.node_id
INNER JOIN gen_obj ON t_membership.parent_object_id = gen_obj.gen_id
WHERE t_membership.collection_id = 12