# Flink Jev: decisiones semánticas en Flink SQL

Proveedor comunitario de modelos para `ML_PREDICT` de Apache Flink 2.3. Formula a TypeSafe Jev una pregunta de sí/no por fila y devuelve una probabilidad, una ruta (`yes`, `no`, `review`, `failure`) y una huella SHA-256 del texto. Las respuestas inciertas van a `review`; los errores de red y las respuestas inválidas van a `failure`.

## Instalación

Compile con Java 17 y Maven 3.9.6 o posterior:

```sh
mvn -B verify
```

Coloque `target/flink-jev-0.1.1.jar` en el directorio `lib` de cada JobManager y TaskManager y reinicie el clúster. Defina `JEV_API_KEY` en el entorno de los TaskManagers. No ponga la clave en opciones SQL ni en archivos seguidos por Git.

```sql
CREATE MODEL jev_review
INPUT (content STRING)
OUTPUT (probability DOUBLE, route STRING, state_sha256 STRING)
WITH (
  'provider' = 'jev',
  'question' = '¿La reseña recomienda la película?',
  'model' = 'jev-1.13.0',
  'threshold' = '0.8',
  'max-input-bytes' = '32768',
  'timeout-ms' = '10000',
  'max-calls-per-task' = '10000'
);

SELECT review_id, route, probability
FROM ML_PREDICT(TABLE reviews, MODEL jev_review, DESCRIPTOR(content));
```

El ejemplo supone una tabla `reviews` con `review_id` y `content`. `endpoint` es `https://api.typesafe.ai/v1/systemone` por defecto; HTTP solo se admite para `localhost` y `127.0.0.1` en pruebas.

## Ejemplo: supervisar las rutas

Después de crear el modelo anterior, el [ejemplo SQL](examples/route-monitoring.sql) agrupa las filas por ruta. `review` requiere una decisión humana y `failure` un tratamiento de errores; ninguna debe interpretarse como `no`. El SQL supone la tabla `reviews` descrita antes y realiza una llamada remota por cada fila apta. Empiece con una muestra pequeña y autorizada.

## Comportamiento y límites

- Cada fila admisible realiza una solicitud remota. Filtre antes y limite las llamadas por tarea.
- Las entradas vacías o demasiado grandes van a `review`. El contenido se envía a TypeSafe; use solo flujos autorizados.
- `failure` se distingue de `no` para que las interrupciones nunca se conviertan en decisiones negativas.
- El conector no devuelve el texto de entrada; la huella ayuda a auditar las decisiones sin copiarlo.
- Este primer conector espera una columna de texto y un flujo append-only. Valide el coste y la latencia con su carga real.

Licencia MIT. Proyecto comunitario independiente, sin afiliación con Apache Flink ni TypeSafe AI.

[Français](README.md) · [English](README.en.md)
