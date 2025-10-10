#!/bin/bash
set -e # Salir inmediatamente si un comando falla.

# --- CORRECCIÓN DEFINITIVA: Autodescubrir la ruta de Java 11 ---
# Busca el directorio de OpenJDK 11 y lo exporta como JAVA_HOME
export JAVA_HOME=$(find /usr/lib/jvm/ -name "java-11-openjdk-*" -type d | head -n 1)
export PATH=$JAVA_HOME/bin:$PATH

# Añadimos la ruta de los binarios de Spark al PATH.
export PATH=$PATH:/home/glue_user/spark/bin

# Verifica que estamos usando la versión correcta
echo "--- Versión de Java en uso ---"
java -version
echo "----------------------------"

# Inicia el servidor Tika en segundo plano
echo "Iniciando Tika Server en segundo plano..."
java -jar /opt/tika/tika-server.jar --noFork > /tmp/tika.log 2>&1 &
TIKA_PID=$!

# ... (el resto del script de cleanup y ejecución no cambia) ...
cleanup() {
    echo "--- Mostrando logs de Tika ---"
    cat /tmp/tika.log
    echo "----------------------------"
    echo "Deteniendo Tika Server (PID: $TIKA_PID)..."
    kill $TIKA_PID || true
}
trap cleanup EXIT

echo "Esperando 10 segundos para que Tika Server se inicie..."
sleep 10
echo "Tika Server debería estar listo."

echo "Ejecutando el job de Glue: $@"
"$@"

echo "El job de Glue ha terminado."