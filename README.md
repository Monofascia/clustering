# REPOSITORY INPS-ADV-ETL


# Esecuzione
Per eseguire il progetto esistono due modalità: tramite **file eseguibile** e tramite **spark-submit**

I parametri accettati sono:

* SCOPE: all, explore, results
  * TASK (explore): rules, top_pcs, elbow, silhouette, distance_matrix, hierarchical_distance_matrix, dendrogram
  * TASK (results): archives_frequencies, trend_vector

## File eseguibile

Le opzioni spark sono configurabili nel file *spark.conf*

~~~
./runAdvancedAnalytics.sh --scope <SCOPE> [--task <TASK>[ <TASK>]]
~~~
Esempio:
~~~
./runAdvancedAnalytics.sh --scope explore --task rules
~~~

## spark-submit

### Creazione pacchetto
Lo zip è necessario in caso di esecuzione su **yarn**, per la distribuzione dei moduli necessari all'esecuzione dello script in modalità distribuita
~~~
(cd advanced_analytics && zip -r ../src.zip  src/ -x \*.pyc* \*\_\_pycache__*)
~~~

### Local
~~~
spark-submit [SPARK-OPTIONS] advanced_analytics/main.py --scope <SCOPE> [--task <TASK>[ <TASK>]]
~~~
Esempio:
~~~
spark-submit --driver-memory 8G --conf spark.sql.shuffle.partitions=32 advanced_analytics/main.py --scope explore --task rules
~~~

### Yarn - client
~~~
spark-submit --master yarn [SPARK-OPTIONS] --py-files src.zip advanced_analytics/main.py --scope <scope> [--task <task>[ <task>]]
~~~
Esempio:
~~~
spark-submit --master yarn --num-executors 6 --executor-cores 5 --executor-memory 17G --conf spark.sql.shuffle.partitions=30 --py-files src.zip advanced_analytics/main.py --scope explore --task rules
~~~

### Yarn - cluster
~~~
spark-submit --master yarn --deploy-mode cluster [SPARK-OPTIONS] --files resources/config.ini --py-files src.zip advanced_analytics/main.py --scope <scope> [--task <task>[ <task>]]
~~~
Esempio:
~~~
spark-submit --master yarn --deploy-mode cluster --driver-cores 1 --driver-memory 1G --num-executors 6 --executor-cores 5 --executor-memory 17G --conf spark.sql.shuffle.partitions=30 --py-files src.zip --files resources/config.ini advanced_analytics/main.py --scope explore --task rules
~~~
