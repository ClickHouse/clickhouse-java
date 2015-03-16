ClickHouse JDBC driver
===============

Пока что в состоянии "как-то работает".

### Сборка

jar можно собрать через

`mvn package assembly:single`

И забрать в `target/jdbc-1.0-SNAPSHOT-jar-with-dependencies.jar`

Собираться будет только если стоит jdk 1.6 и если она прописана в $JAVA_HOME при сборке (или 1.6 это дефолт).

Позже сделаю заливку последней версии куда-нибудь.
