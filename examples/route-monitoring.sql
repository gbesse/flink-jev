-- Assumes the jev_review model and reviews(review_id, content) from the README.
-- Run on a small, permitted sample: each eligible row can cause a remote call.
-- Keep review and failure separate from no in downstream handling.
SELECT route, COUNT(*) AS rows_in_route
FROM (
  SELECT route
  FROM ML_PREDICT(TABLE reviews, MODEL jev_review, DESCRIPTOR(content))
) AS decisions
GROUP BY route;
