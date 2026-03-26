import mygeotab
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timezone, timedelta
import os
import json

# --- 1. CONFIGURAÇÕES GEOTAB ---
BASES = [
    {
        'name': 'MERCADO_LIVRE',
        'user': 'hitalo.correa@transmana.com.br',
        'password': '@Torpedo22',
        'db': 'mercadolivre'
    },
    {
        'name': 'TRANSMANA',
        'user': 'hitalo.correa@transmana.com.br',
        'password': '@Torpedo22',
        'db': 'transmana'
    }
]

# --- 2. CONFIGURAÇÕES GOOGLE SHEETS ---
PLANILHA_ID = "14Ym6mM2sbsAG7Rme__6F5R7Y4tBJ9Dk9BAJP4O72cJ0"
NOME_DA_ABA = "Visão da Frota"
ARQUIVO_CREDENCIAIS = 'porjeto-rodar-online-geotab-133b08a54dd7.json'

def normalizar_placa(placa):
    """Remove traços, espaços e deixa tudo maiúsculo para garantir o Match perfeito"""
    if not placa:
        return ""
    return str(placa).replace("-", "").replace(" ", "").upper().strip()

def buscar_status_geotab():
    status_por_placa = {}
    agora = datetime.now(timezone.utc)

    for config in BASES:
        print(f"\n>>> Sincronizando com a base Geotab: {config['name']}...")
        try:
            client = mygeotab.API(username=config['user'], password=config['password'], database=config['db'])
            client.authenticate()

            # Mapeia ID -> Placa 
            devices = client.call('Get', typeName='Device')
            mapa_id_placa = {}
            for d in devices:
                placa_bruta = d.get('licensePlate', '')
                if placa_bruta:
                    mapa_id_placa[d['id']] = normalizar_placa(placa_bruta)

            status_info = client.call('Get', typeName='DeviceStatusInfo')
            
            calls_odometro = []
            call_placas = []
            
            for status in status_info:
                dev_id = status['device']['id']
                placa_limpa_geotab = mapa_id_placa.get(dev_id)
                
                if placa_limpa_geotab:
                    is_communicating = status.get('isDeviceCommunicating', False)
                    last_comm = status.get('dateTime') or agora
                    
                    if not is_communicating:
                        tempo_off = agora - last_comm
                        
                        dias_inteiros = tempo_off.days 
                        
                        if dias_inteiros == 0:
                            dias_off = "Menos de 1 dia"
                        elif dias_inteiros == 1:
                            dias_off = "1 dia"
                        else:
                            dias_off = f"{dias_inteiros} dias"
                            
                        status_str = "Offline"
                        data_inicio = last_comm - timedelta(days=60)
                        data_fim = last_comm + timedelta(days=1)
                    else:
                        dias_off = "ONLINE" 
                        status_str = "Online"
                        data_inicio = agora - timedelta(days=2)
                        data_fim = agora
                        
                    status_por_placa[placa_limpa_geotab] = {
                        "status": status_str,
                        "dias_off": dias_off,
                        "odometro": "Buscando..."
                    }
                    
                    # Chamada 1: Hodômetro Ajustado
                    calls_odometro.append((
                        'Get', {
                            'typeName': 'StatusData',
                            'search': {
                                'deviceSearch': {'id': dev_id},
                                'diagnosticSearch': {'id': 'DiagnosticOdometerAdjustmentId'},
                                'fromDate': data_inicio,
                                'toDate': data_fim
                            }
                        }
                    ))
                    # Chamada 2: Hodômetro Físico (Cru)
                    calls_odometro.append((
                        'Get', {
                            'typeName': 'StatusData',
                            'search': {
                                'deviceSearch': {'id': dev_id},
                                'diagnosticSearch': {'id': 'DiagnosticOdometerId'},
                                'fromDate': data_inicio,
                                'toDate': data_fim
                            }
                        }
                    ))
                    call_placas.extend([placa_limpa_geotab, placa_limpa_geotab])

            # Executa a busca em lote
            if calls_odometro:
                print(f"Buscando hodômetros para {len(calls_odometro) // 2} veículos...")
                chunk_size = 200 
                
                for i in range(0, len(calls_odometro), chunk_size):
                    chunk_calls = calls_odometro[i:i + chunk_size]
                    chunk_placas = call_placas[i:i + chunk_size]
                    
                    try:
                        resultados_odo = client.multi_call(chunk_calls)
                        
                        for idx in range(0, len(resultados_odo), 2):
                            placa = chunk_placas[idx]
                            res_adj = resultados_odo[idx]
                            res_raw = resultados_odo[idx + 1]
                            
                            valor_km = "Sem dado"
                            
                            # Tenta Ajustado, se falhar, tenta Cru
                            if isinstance(res_adj, list) and len(res_adj) > 0:
                                valor_km = round(res_adj[-1].get('data', 0) / 1000)
                            elif isinstance(res_raw, list) and len(res_raw) > 0:
                                valor_km = round(res_raw[-1].get('data', 0) / 1000)
                                
                            status_por_placa[placa]["odometro"] = valor_km
                                
                    except Exception as e:
                        print(f"Aviso: Erro na busca de hodômetro do lote: {e}")
                        
        except Exception as e:
            print(f"ERRO ao consultar base {config['name']}: {e}")
            
    return status_por_placa

def atualizar_planilha():
    print("Conectando ao Google Sheets...")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    # --- MÁGICA PARA O GITHUB: LÊ DA MEMÓRIA EM VEZ DO ARQUIVO ---
    credenciais_github = os.environ.get('CREDENCIAIS_GCP')
    
    if credenciais_github:
        print("Lendo chaves secretas do GitHub...")
        info_credenciais = json.loads(credenciais_github)
        credenciais = ServiceAccountCredentials.from_json_keyfile_dict(info_credenciais, scope)
    else:
        print("Lendo chaves do arquivo local...")
        credenciais = ServiceAccountCredentials.from_json_keyfile_name(ARQUIVO_CREDENCIAIS, scope)

    cliente_sheets = gspread.authorize(credenciais)

    planilha = cliente_sheets.open_by_key(PLANILHA_ID)
    folha = planilha.worksheet(NOME_DA_ABA)

    print(f"Lendo as placas da aba '{NOME_DA_ABA}' (Coluna C)...")
    placas_sheets = folha.col_values(3)[1:] 
    
    if not placas_sheets:
        print("Nenhuma placa encontrada na Coluna C.")
        return

    status_geotab = buscar_status_geotab()

    print("\nCruzando os dados e calculando status para atualização...")
    lista_atualizacao = []
    
    for placa in placas_sheets:
        placa_limpa = normalizar_placa(placa)
        
        if not placa_limpa:
            lista_atualizacao.append(["", "", ""])
            continue
            
        dados = status_geotab.get(placa_limpa)
        if dados:
            lista_atualizacao.append([dados['status'], dados['dias_off'], dados['odometro']])
        else:
            lista_atualizacao.append(["Não Encontrado", "", ""])

    linha_final = len(lista_atualizacao) + 1
    intervalo_atualizacao = f'W2:Y{linha_final}'
    
    print(f"Gravando dados nas colunas W, X e Y (Intervalo {intervalo_atualizacao})...")
    folha.update(range_name=intervalo_atualizacao, values=lista_atualizacao)
    
    print("\n>>> Planilha atualizada com sucesso em tempo real!")

if __name__ == "__main__":
    atualizar_planilha()
