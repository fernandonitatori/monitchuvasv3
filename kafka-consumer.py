from kafka import KafkaConsumer, KafkaProducer
from multiprocessing.sharedctypes import Value
import sys
import requests
import json
from json import loads
import urllib.request
import os
import ssl
import psycopg2
import pandas as pd
from onesignal_sdk.client import Client



### Setting up the Python consumer

bootstrap_servers = ['200.145.27.25:9092']

topicName = 'medicao_notif'

consumer = KafkaConsumer(topicName, bootstrap_servers=bootstrap_servers,

                         auto_offset_reset='latest',
                         value_deserializer=lambda x: loads(x.decode('utf-8')))  ## You can also set it as latest

### Read message from consumer

try:

    for message in consumer:
        print(message.value)
        x = str(message.value)
        strx = x.split(",")
        dado_start = strx[0]
        dado_start1 = dado_start.split("'")
        dado_start1 = dado_start1[5]
#        print(dado_start1)

        dado_end = strx[1]
        dado_end1 = dado_end.split("'")
        dado_end1 = dado_end1[3]
 #       print(dado_end1)

        dado_device = strx[2]
        dado_device1 = dado_device.split("'")
        dado_device1 = dado_device1[3]
  #      print(dado_device1)

        dado_varaltura = strx[4].split(":")
        dado_varaltura = dado_varaltura[1]
        dado_varaltura = dado_varaltura.split("}")
        dado_varaltura = dado_varaltura[0]
   #     print(dado_varaltura)

        dado_volume = strx[3].split(":")
        dado_volume = dado_volume[1]
        dado_volume = float(dado_volume)
    #    print(dado_volume)

        data = {
            "Inputs": {
                "input1": [
                    {
                        "municipio": "SOROCABA",
                        "codEstacao": "355220503A",
                        "uf": "SP",
                        "nomeEstacao": "Cerrado",
                        "latitude": -23.508000000000003,
                        "longitude": -47.486000000000004,
                        "datahora": "2023-01-13 18:50:00.0",
                        "valorMedida": dado_volume,
                        "alagamento": 0
                    }
                ]
            },
            "GlobalParameters": {}
        }

        body = str.encode(json.dumps(data))

        url = 'http://354bd743-61fb-4993-abb8-1d17e65e4e97.eastus2.azurecontainer.io/score'
        # Replace this with the primary/secondary key or AMLToken for the endpoint
        api_key = 'yg3McRftHuwcb6qtav4NR0RXVuyR3PwI'
        if not api_key:
            raise Exception("A key should be provided to invoke the endpoint")

        headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}

        req = urllib.request.Request(url, body, headers)

        try:
            response = urllib.request.urlopen(req)

            result = response.read()
            x = json.loads(result)
            y = x["Results"]
            z = y["WebServiceOutput0"]
            a = z[0]
            b = a["AlagamentoPrediction"]
            print(b)
            dado_predbyvolume = b

            if dado_predbyvolume == 1:
                client = Client(app_id='473c152c-7766-4691-9e44-9bbd0e67de05',
                                rest_api_key='OTEzYWU5ZTUtNGE5Mi00YWQ0L\
                                        WJlOGEtMTZhNzhmYjc5MTE4')
                notification_body = {
                    'contents': {'en': 'Alerta de provável alagamento por volume de chuva intenso. \
                                         - Veja detalhes no Monit Chuvas'},
                    'included_segments': ['Subscribed Users'],
                }
                response = client.send_notification(notification_body)

        except urllib.error.HTTPError as error:
            print("The request failed with status code: " + str(error.code))

            # Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
            print(error.info())
            print(error.read().decode("utf8", 'ignore'))

        data2 = {
            "Inputs": {
                "input1": [
                    {
                        "chuva": 1,
                        "deviceid": 1,
                        "distancia": 52,
                        "timestamp": "2023-04-27 20:31:46.275",
                        "varaltura": dado_varaltura,
                        "volume": 0.0,
                        "alagamento": 0
                    }
                ]
            },
            "GlobalParameters": {}
        }

        body2 = str.encode(json.dumps(data2))

        url2 = 'http://0d23d719-19ca-457d-a2d2-483f11ecb7a0.eastus2.azurecontainer.io/score'
        # Replace this with the primary/secondary key or AMLToken for the endpoint
        api_key2 = 'jRTfJPhDWtE36Rpwp4dRb9sx8dKpfSzX'
        if not api_key2:
            raise Exception("A key should be provided to invoke the endpoint")

        headers2 = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key2)}

        req2= urllib.request.Request(url2, body2, headers2)

        try:
            response2 = urllib.request.urlopen(req2)

            result2 = response2.read()
            x2 = json.loads(result2)
            y2 = x2["Results"]
            z2 = y2["WebServiceOutput0"]
            a2 = z2[0]
            b2 = a2["AlagamentoPrediction"]
            print(b2)
            dado_predbyaltura = b2
            if dado_predbyaltura == 1:
                client = Client(app_id='473c152c-7766-4691-9e44-9bbd0e67de05',
                                rest_api_key='OTEzYWU5ZTUtNGE5Mi00YWQ0L\
                                        WJlOGEtMTZhNzhmYjc5MTE4')
                notification_body = {
                    'contents': {'en': 'Alerta de provável alagamento por subida de nivel de água. \
                                         - Veja detalhes no Monit Chuvas'},
                    'included_segments': ['Subscribed Users'],
                }
                response = client.send_notification(notification_body)

            print("####################")
        except urllib.error.HTTPError as error:
            print("The request failed with status code: " + str(error.code))

            # Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
            print(error.info())
            print(error.read().decode("utf8", 'ignore'))

        try:
            connection = psycopg2.connect(user="monitchuvas",
                                              password="Mki**&RfV",
                                              host="127.0.0.1",
                                              port="5432",
                                              database="monitchuvas")
            cursor = connection.cursor()



            postgres_insert_query = '''INSERT INTO core_monitchuva (inicio, fim, deviceid, max_volume, max_varaltura, predbyvolume, predbyaltura) VALUES (%s,%s,%s,%s,%s,%s,%s)'''
            record_to_insert = (dado_start1,dado_end1,dado_device1,dado_volume, dado_varaltura, dado_predbyvolume, dado_predbyaltura)
            cursor.execute(postgres_insert_query, record_to_insert)
            connection.commit()
            count = cursor.rowcount
            print(count, "Record inserted successfully into monitchuva table")

        except (Exception, psycopg2.Error) as error:
            print("Failed to insert record into monitchuva table", error)

        finally:
            # closing database connection.
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")


except KeyboardInterrupt:

    sys.exit()



def allowSelfSignedHttps(allowed):
    # bypass the server certificate verification on client side
    if allowed and not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None):
        ssl._create_default_https_context = ssl._create_unverified_context

allowSelfSignedHttps(True) # this line is needed if you use self-signed certificate in your scoring service.


