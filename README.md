# Flink Jev : décisions sémantiques dans Flink SQL

Connecteur communautaire pour `ML_PREDICT` d'Apache Flink 2.3. Il pose une question oui/non à TypeSafe Jev pour chaque ligne et renvoie une probabilité, une route (`yes`, `no`, `review`, `failure`) et l'empreinte SHA-256 du texte. Les réponses incertaines vont vers `review` ; les erreurs réseau et les réponses invalides vont vers `failure`.

## Installer

Construisez avec Java 17 et Maven 3.9.6 ou plus récent :

```sh
mvn -B verify
```

Placez `target/flink-jev-0.1.0.jar` dans le répertoire `lib` de chaque JobManager et TaskManager, puis redémarrez le cluster. Définissez `JEV_API_KEY` dans l'environnement des TaskManagers. Ne placez jamais la clé dans les options SQL ou un fichier suivi par Git.

```sql
CREATE MODEL jev_review
INPUT (content STRING)
OUTPUT (probability DOUBLE, route STRING, state_sha256 STRING)
WITH (
  'provider' = 'jev',
  'question' = 'La critique recommande-t-elle le film ?',
  'model' = 'jev-1.13.0',
  'threshold' = '0.8',
  'max-input-bytes' = '32768',
  'timeout-ms' = '10000',
  'max-calls-per-task' = '10000'
);

SELECT review_id, route, probability
FROM ML_PREDICT(TABLE reviews, MODEL jev_review, DESCRIPTOR(content));
```

L'exemple suppose une table `reviews` avec `review_id` et `content`. `endpoint` vaut `https://api.typesafe.ai/v1/systemone` par défaut ; une URL HTTP n'est admise que pour `localhost` et `127.0.0.1` lors des tests.

## Comportement et limites

- Chaque ligne éligible effectue un appel distant. Filtrez les lignes en amont et limitez les appels par tâche.
- Les entrées vides ou trop grandes vont vers `review`. Le contenu est envoyé à TypeSafe ; n'utilisez que les flux autorisés.
- `failure` est distinct de `no` pour que les pannes ne deviennent jamais des décisions négatives.
- Le connecteur ne conserve pas les textes dans le résultat ; l'empreinte aide à auditer les décisions sans les recopier.
- Ce premier connecteur attend une colonne texte et un flux append-only. Validez les coûts et la latence avec votre charge réelle.

Licence MIT. Projet communautaire indépendant, sans affiliation avec Apache Flink ou TypeSafe AI.

[English](README.en.md) · [Español](README.es.md)
