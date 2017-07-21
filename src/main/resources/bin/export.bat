java -Xms256m -Xmx512m -server -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=logs -Dlogging.config=config/log4j2.xml -Dspring.config.location=config/application.yml -Dfile.encoding=UTF-8 -Xloggc:logs/gc.log -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -jar elasticsearch-export.jar %*