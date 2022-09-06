#importing library
from pymongo import MongoClient
import pandas as pd
import datetime

#mongo connection
client = MongoClient('34.226.197.143',27019)
print('\n')
print('#'*10+' '+'Connected to MongoDB!'+' '+'#'*10)

#getting a database
magic_db = client['magic']

#getting internet collection
collection_internet = magic_db['internet']
collection_internet.find({"sent_time":{"$lt":datetime.date.today()-datetime.timedelta(days=15)}})
#creating lists internet colleciton
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

#appending information
for itens in collection_internet.find():
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

#converting json to pandas table and exporting to excel <- backup onedrive e uft-8
tb_cert_activity_data = pd.DataFrame(activity_data).to_excel('activity_data.xlsx')
tb_cert_phone_info = pd.DataFrame(cert_phone_info).to_excel('cert_phone_info.xlsx')
tb_cert_wifi_5g = pd.DataFrame(cert_wifi_5g).to_excel('cert_wifi_5g.xlsx')
tb_cert_wifi_2g = pd.DataFrame(cert_wifi_2g).to_excel('cert_wifi_2g.xlsx')
tb_cert_5g_bs = pd.DataFrame(cert_wifi_5g_bs).to_excel('cert_wifi_5g_bs.xlsx')
tb_cert_wifi_smarthome = pd.DataFrame(cert_wifi_smarthome).to_excel('cert_wifi_smarthome.xlsx')
tb_cert_wan_omci = pd.DataFrame(cert_wan_omci).to_excel('cert_wan_omci.xlsx')
tb_cert_wan_inet = pd.DataFrame(cert_wan_inet).to_excel('cert_wan_inet.xlsx')
tb_cert_wan_dns = pd.DataFrame(cert_wan_dns).to_excel('cert_wan_dns.xlsx')
tb_cert_lan_iface = pd.DataFrame(cert_lan_iface).to_excel('cert_lan_iface.xlsx')
tb_cert_lan_dns = pd.DataFrame(cert_lan_dns).to_excel('cert_lan_dns.xlsx')
tb_cert_voip = pd.DataFrame(cert_voip).to_excel('cert_voip.xlsx')
tb_cert_speedtest = pd.DataFrame(cert_speedtest).to_excel('cert_speedtest.xlsx')

''' %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% '''

#getting cards collections
collection_cards = magic_db['cards']

#creating lists cards colleciton
dados_atividade = []
cards = []
discovery_topology = []
discovery_hosts = []

for itens in collection_cards.find():
    discovery_hosts.append(itens['discovery']['hosts'])
    discovery_topology.append(itens['discovery']['topologia'])
    cards.append(itens['cards'])

#converting in pandas table and exporting to excel file
tb_discovery_hosts = pd.DataFrame(discovery_hosts).to_excel('tb_discovery_hosts.xlsx')
tb_discovery_topology = pd.DataFrame(discovery_topology).to_excel('tb_discovery_topology.xlsx')
tb_cards = pd.DataFrame(cards).to_excel('tb_cards.xlsx')

#colocar as tabelas na nuvem
#fazer o dumping diariamente <- agendador de tarefas windows/linux
#pegar as informações dos últimos 15 dias
#atualizar o power bi na nuvem

print(collection_internet.find({"sent_time": {"$lt": datetime.date.today() - datetime.timedelta(days=15)}}).sort("designador"))
