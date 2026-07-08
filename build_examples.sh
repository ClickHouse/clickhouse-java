#!/bin/sh
LIB_VER=$(grep '<revision>' pom.xml | sed -e 's|[[:space:]]*<[/]*revision>[[:space:]]*||g')
find `pwd`/examples -type f -name pom.xml -exec sed -i -e "s|\(<clickhouse-java.version>\).*\(<\)|\1$LIB_VER\2|g" {} \;
for d in $(ls -d `pwd`/examples/*/); do \
    if [ -e $d/pom.xml ]; then cd $d && mvn  --batch-mode --no-transfer-progress clean compile; fi;
    if [ -e $d/gradlew ]; then cd $d && ./gradlew clean build; fi;
done

