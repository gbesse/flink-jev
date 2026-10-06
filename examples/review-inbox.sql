-- FR : Exemple de boîte de revue. Exécuter sur un petit échantillon autorisé : chaque ligne peut déclencher un appel distant.
-- EN: Review-inbox example. Run on a small permitted sample: each row may trigger a remote call.
-- ES: Ejemplo de bandeja de revisión. Ejecútelo con una muestra pequeña autorizada: cada fila puede generar una llamada remota.
-- Assumes the jev_review model and reviews(review_id, content) from the README.
-- Keep review and failure distinct from no; do not silently treat either as a rejection.
WITH decisions AS (
  SELECT review_id, content, route, probability
  FROM ML_PREDICT(TABLE reviews, MODEL jev_review, DESCRIPTOR(content))
)
SELECT review_id, content, route, probability
FROM decisions
WHERE route IN ('review', 'failure');
