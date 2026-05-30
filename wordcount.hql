-- =====================================================================
-- WORDCOUNT EN APACHE HIVE (HiveQL)
-- =====================================================================

-- 1. Crear la tabla externa o interna que apunta a los archivos de texto
CREATE TABLE IF NOT EXISTS documentos (
    linea STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\n'
STORED AS TEXTFILE;

-- 2. Cargar tus datos (por ejemplo, desde un directorio en HDFS)
-- LOAD DATA INPATH 'hdfs:///user/hadoop/recetas_masivas2/' INTO TABLE documentos;

-- 3. Ejecutar la consulta WordCount utilizando SQL
-- Explicación: 
-- SPLIT: Divide cada línea por espacios/caracteres no alfanuméricos en un array.
-- EXPLODE: Convierte cada elemento del array (palabra) en una fila propia.
-- LATERAL VIEW: Permite asociar las filas explotadas a la fila de origen.
SELECT 
    palabra, 
    COUNT(1) AS total
FROM documentos
LATERAL VIEW EXPLODE(SPLIT(LOWER(linea), '[^a-zA-Z0-9]+')) t AS palabra
WHERE palabra != ''
GROUP BY palabra
ORDER BY total DESC;
