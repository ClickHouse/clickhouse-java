ClickHouse JDBC driver
===============

Пока что в состоянии "как-то работает".

### URL

Пока нет фейловера и лоадбаланса, урл простой.

`jdbc:clickhouse:host:port`

Например: `jdbc:clickhouse:localhost:8123`

### Сборка

jar можно собрать через

`mvn package assembly:single`

И забрать в `target/jdbc-1.0-SNAPSHOT-jar-with-dependencies.jar`

Собираться будет только если стоит jdk 1.6 и если она прописана в $JAVA_HOME при сборке (или 1.6 это дефолт).

Позже сделаю заливку последней версии куда-нибудь.
