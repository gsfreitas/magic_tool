#importing library
from pymongo import MongoClient
import pandas as pd
import datetime
from openpyxl import Workbook, load_workbook

#mongo connection
client = MongoClient('34.226.197.143',27019)
print('\n')
print('#'*10 + 'Successfull connection to MongoDB!'+'#'*10)

#getting a database
magic_db = client['magic']
print('\n')
print('#'*10 + 'Successfull connection to Magic database!'+'#'*10)

#getting internet collection
internet_collection = magic_db['internet']
print('\n')
print('#'*10 + 'Successfull connection to Internet collection!'+'#'*10)

#COLETAR AS INFORMAÇÕES E ARMAZENÁ-LAS EM LISTA
activity_data = []
cert_phone_info = []
cert_wifi_5g = []
cert_wifi_2g = []
cert_wifi_5g_bs = []
cert_wifi_smarthome = []
cert_wan_omci = []
cert_wan_inet = []
cert_wan_dns = []
cert_lan_iface = []
cert_lan_dns = []
cert_voip = []
cert_speedtest = []

#append das informações em cada lista
for itens in internet_collection.find():
    activity_data.append(itens['dadosAtividade'])
    cert_phone_info.append(itens['cert_phone_info'])
    cert_wifi_5g.append(itens['cert_wifi_5G'])
    cert_wifi_2g.append(itens['cert_wifi_2G'])
    cert_wifi_5g_bs.append(itens['cert_wifi_5G_BS'])
    cert_wifi_smarthome.append(itens['cert_wifi_smartHome'])
    cert_wan_omci.append(itens['cert_wan_omci'])
    cert_wan_inet.append(itens['cert_wan_inet'])
    cert_wan_dns.append(itens['cert_wan_dns'])
    cert_lan_iface.append(itens['cert_lan_iface'])
    cert_lan_dns.append(itens['cert_lan_dns'])
    cert_voip.append(itens['cert_voip'])
    cert_speedtest.append(itens['cert_speedTest'])

#converter os dados em tabela
tb_cert_activity_data = pd.DataFrame(activity_data)
tb_cert_phone_info = pd.DataFrame(cert_phone_info)
tb_cert_wifi_5g = pd.DataFrame(cert_wifi_5g)
tb_cert_wifi_2g = pd.DataFrame(cert_wifi_2g)
tb_cert_5g_bs = pd.DataFrame(cert_wifi_5g_bs)
tb_cert_wifi_smarthome = pd.DataFrame(cert_wifi_smarthome)
tb_cert_wan_omci = pd.DataFrame(cert_wan_omci)
tb_cert_wan_inet = pd.DataFrame(cert_wan_inet)
tb_cert_wan_dns = pd.DataFrame(cert_wan_dns)
tb_cert_lan_iface = pd.DataFrame(cert_lan_iface)
tb_cert_lan_dns = pd.DataFrame(cert_lan_dns)
tb_cert_voip = pd.DataFrame(cert_voip)
tb_cert_speedtest = pd.DataFrame(cert_speedtest)

#converter a coluna sent time em datetime
itens = 0
list_sent_time = []

for itens in tb_cert_activity_data['sent_time']:
    if ":" in itens:
        itens = datetime.datetime.strptime(itens, '%Y-%m-%d %H:%M:%S')
        list_sent_time.append(itens)
    else:
        list_sent_time.append('2022-01-01 00:00:00')

#converting from list into table
tb_sent_time = pd.DataFrame(list_sent_time, columns=['sent_time'])

#droping old column and inserting new column
tb_cert_activity_data = tb_cert_activity_data.drop(columns=['sent_time'])
tb_cert_activity_data.insert(loc=8,column='sent_time',value=tb_sent_time)

print(type(tb_cert_activity_data['sent_time'][0]))

#analisar as informações dos 15 dias anteriores de dados atividade (for invertido)


#pegar o primeiro id do primeiro registro de 15 dias anteriores

#varrer o restante das collections a partir desse id e armazenar em uma lista

#criar um workbook para cada lista

#escrever/substituir os valores no arquivo criado

#salvar a planilha na nuvem

#conectar o PBI com o link

