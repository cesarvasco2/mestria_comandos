# Importa o publish do paho-mqtt
import sys
import json
import paho.mqtt.client as mqtt
import boto3
import ssl
#configurações do broker: 
Broker = 'message.hidroview.com.br'
PortaBroker = 1883 
Usuario = 'mestria_gateway'
Senha = 'UhFQ+^AG%6eL8MdzQ8ZW'
KeepAliveBroker = 60

sqs = boto3.resource('sqs', region_name='us-east-1')
queue = sqs.get_queue_by_name(QueueName='mestria_comandos')

try:
    print('[STATUS] Inicializando MQTT...') 
#inicializa MQTT:
    client = mqtt.Client()
    client.username_pw_set(Usuario, Senha)
    # the key steps here
    #context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    # if you do not want to check the cert hostname, skip it
    # context.check_hostname = False
    #client.tls_set_context(context)
    client.connect(Broker, PortaBroker, KeepAliveBroker)
    client.loop_start()
    while True:   
        for message in queue.receive_messages():
            try:
                payload = message.body
                payload_dict = json.loads(json.loads(payload))
                topic = 'mestria/'+payload_dict['id_dispositivo']+'/sub'           
                command_bin = '{'+'"'+payload_dict['codigo_comando']+'"'+':'+payload_dict['status']+'}'
                connected = client.publish(topic,command_bin, qos=0, retain=False)
                print('\033[42;1;33m'+'Tópico: '+'\033[0;0m'+topic+ '\n\033[42;1;33m'+'Comando: '+'\033[0;0m'+command_bin)
                message.delete()
            except KeyError:
                pass    

except KeyboardInterrupt:
    print ("\nCtrl+C pressionado, encerrando aplicacao e saindo...")
    sys.exit(0)