# Usamos la imagen oficial de AWS Glue 4.0 como base
FROM public.ecr.aws/glue/aws-glue-libs:glue_libs_4.0.0_image_01

USER root

# --- PASO 1: Instalar Java 11 de la forma nativa de Amazon Linux ---
RUN amazon-linux-extras install -y java-openjdk11 && \
    yum install -y wget && \
    yum clean all

# --- PASO 2: Configuración de la aplicación ---
RUN mkdir -p /opt/tika && \
    wget https://archive.apache.org/dist/tika/3.2.3/tika-server-standard-3.2.3.jar -O /opt/tika/tika-server.jar

WORKDIR /home/glue_user/app

COPY . .

RUN chown -R glue_user /opt/tika /home/glue_user/app && \
    chmod +x ./entrypoint.sh

RUN pip3 install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=${PYTHONPATH}:/home/glue_user/app/src

USER glue_user

ENTRYPOINT ["./entrypoint.sh"]