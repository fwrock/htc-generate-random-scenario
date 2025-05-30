import xml.etree.ElementTree as ET
import random
import argparse
import sys
import os
from xml.sax.saxutils import escape # Para escapar atributos XML manualmente

def parse_network_iterative(network_file):
    """
    Analisa o arquivo XML da rede de forma iterativa para extrair IDs de nós
    e links de saída, otimizado para baixo uso de memória.

    Args:
        network_file (str): Caminho para o arquivo XML da rede.

    Returns:
        tuple: Uma tupla contendo:
            - list: Uma lista de IDs de nós.
            - dict: Um dicionário onde as chaves são IDs de nós de origem e
                    os valores são listas de IDs de links de saída daquele nó.
            Retorna None, None se a análise falhar ou dados essenciais faltarem.
    """
    if not os.path.isfile(network_file):
        print(f"Erro: Arquivo de rede não encontrado: {network_file}", file=sys.stderr)
        return None, None

    node_ids = []
    outgoing_links = {}
    context = None # Para rastrear o contexto (nodes ou links)

    try:
        print(f"Iniciando leitura iterativa de {network_file}...")
        context_iter = ET.iterparse(network_file, events=('start', 'end'))

        for event, elem in context_iter:
            if event == 'start':
                if elem.tag == 'nodes':
                    context = 'nodes'
                elif elem.tag == 'links':
                    context = 'links'
                elif context == 'nodes' and elem.tag == 'node':
                    node_id = elem.get('id')
                    if node_id:
                        node_ids.append(node_id)
                elif context == 'links' and elem.tag == 'link':
                    from_node = elem.get('from')
                    link_id = elem.get('id')
                    if from_node and link_id:
                        if from_node not in outgoing_links:
                            outgoing_links[from_node] = []
                        outgoing_links[from_node].append(link_id)
            elif event == 'end':
                if elem.tag == 'nodes':
                    context = None
                    print(f"Processados {len(node_ids)} nós.")
                elif elem.tag == 'links':
                    context = None
                    print(f"Processados links (origens únicas: {len(outgoing_links)}).")
                elem.clear()
        print("Leitura iterativa concluída.")

        if not node_ids:
            print(f"Aviso: Nenhum nó (<node> com atributo 'id') encontrado em {network_file}", file=sys.stderr)
        if not outgoing_links:
             print(f"Aviso: Nenhum link (<link>) com 'from' e 'id' válidos encontrado em {network_file}.", file=sys.stderr)

        valid_origin_nodes_count = sum(1 for node in node_ids if node in outgoing_links and outgoing_links[node])
        if valid_origin_nodes_count == 0 and node_ids and outgoing_links:
             print("Erro Crítico: Existem nós e links, mas nenhum nó parece ser uma origem válida de um link. Verifique os IDs.", file=sys.stderr)
             return None, None
        return node_ids, outgoing_links
    except ET.ParseError as e:
        print(f"Erro: Falha ao analisar (parse) o arquivo XML da rede: {network_file}. Detalhes: {e}", file=sys.stderr)
        return None, None
    except FileNotFoundError:
        print(f"Erro: Arquivo de rede não encontrado: {network_file}", file=sys.stderr)
        return None, None
    except Exception as e:
        print(f"Erro inesperado durante a leitura iterativa de {network_file}: {e}", file=sys.stderr)
        return None, None

def generate_aggregated_and_sorted_trips(node_ids, outgoing_links, num_trips_to_generate, max_start_time):
    """
    Gera viagens, as agrupa por (origem, destino, tempo de início) e as ordena pelo tempo de início.

    Args:
        node_ids (list): Lista de IDs de nós disponíveis.
        outgoing_links (dict): Dicionário mapeando IDs de nós de origem para listas de IDs de links de saída.
        num_trips_to_generate (int): O número total de viagens individuais a serem geradas antes do agrupamento.
        max_start_time (int): O tempo máximo de início (em segundos) para uma viagem.

    Returns:
        list: Uma lista de dicionários, onde cada dicionário representa uma viagem agrupada e ordenada.
              Retorna uma lista vazia se não for possível gerar viagens.
    """
    if not node_ids:
        print("Erro: Não há nós disponíveis para gerar viagens.", file=sys.stderr)
        return []

    valid_origin_nodes = [node for node in node_ids if node in outgoing_links and outgoing_links[node]]
    if not valid_origin_nodes:
        print("Erro: Nenhum nó de origem com links de saída válidos foi encontrado.", file=sys.stderr)
        return []

    print(f"Gerando {num_trips_to_generate} viagens individuais para agrupamento...")
    
    # Dicionário para agregar viagens: chave=(origem, destino, tempo_inicio), valor={'count': N, 'link_origin': 'L_X_Y'}
    aggregated_trips_map = {}

    for _ in range(num_trips_to_generate):
        origin_node = random.choice(valid_origin_nodes)
        possible_link_origins = outgoing_links[origin_node]
        link_origin = random.choice(possible_link_origins)

        destination_node = random.choice(node_ids)
        while len(node_ids) > 1 and destination_node == origin_node:
            destination_node = random.choice(node_ids)
        
        start_time = random.randint(0, max_start_time) # Mantém como int para ordenação

        trip_key = (origin_node, destination_node, start_time)

        if trip_key not in aggregated_trips_map:
            aggregated_trips_map[trip_key] = {
                'count': 0,
                'link_origin': link_origin, # Pega o link_origin da primeira ocorrência desta chave
                # Atributos fixos como 'mode' e 'digital_rails_capable' serão adicionados depois
            }
        aggregated_trips_map[trip_key]['count'] += 1

    print(f"Agrupamento concluído. {len(aggregated_trips_map)} viagens únicas (O-D-Start).")

    # Converter o mapa agregado para uma lista de dicionários para ordenação
    processed_trips_list = []
    for (origin, destination, start), data in aggregated_trips_map.items():
        processed_trips_list.append({
            'origin': origin,
            'destination': destination,
            'start': start, # Mantém como int para ordenação
            'count': data['count'],
            'link_origin': data['link_origin'],
            'mode': "car", # Atributo fixo
            'digital_rails_capable': "false" # Atributo fixo
        })

    # Ordenar a lista de viagens pelo tempo de início ('start')
    print("Ordenando viagens por tempo de início...")
    processed_trips_list.sort(key=lambda x: x['start'])

    # Adicionar nomes sequenciais e converter 'start' para string para a saída XML
    final_trips_for_xml = []
    for i, trip_data in enumerate(processed_trips_list):
        trip_data_copy = trip_data.copy() # Evitar modificar a lista original durante a iteração se necessário
        trip_data_copy['name'] = f"trip_{i+1}" # Nomes sequenciais baseados na ordem final
        trip_data_copy['start'] = str(trip_data_copy['start']) # Converter para string para XML
        final_trips_for_xml.append(trip_data_copy)
    
    print("Geração e processamento de viagens concluídos.")
    return final_trips_for_xml

def write_trips_to_xml_iterative(trips_list_for_xml, output_file):
    """
    Escreve a lista de viagens (já agrupadas e ordenadas) para um arquivo XML.

    Args:
        trips_list_for_xml (list): Lista de dicionários de viagens prontas para escrita.
        output_file (str): Caminho para o arquivo XML de saída.

    Returns:
        bool: True se a escrita foi bem-sucedida, False caso contrário.
    """
    print(f"Escrevendo {len(trips_list_for_xml)} viagens agrupadas/ordenadas para {output_file}...")
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('<?xml version="1.0" encoding="utf-8"?>\n')
            f.write('<scsimulator_matrix>\n')

            for trip_attrs in trips_list_for_xml:
                # Constrói a string do atributo escapando os valores
                attr_string = " ".join(f'{k}="{escape(str(v))}"' for k, v in trip_attrs.items())
                f.write(f'  <trip {attr_string}/>\n') # Adiciona indentação manual

            f.write('</scsimulator_matrix>\n')

        print(f"Arquivo de viagens salvo com sucesso em: {output_file}")
        return True
    except IOError as e:
        print(f"Erro de I/O ao escrever no arquivo de saída {output_file}: {e}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Erro inesperado ao escrever viagens para {output_file}: {e}", file=sys.stderr)
        return False

def main():
    parser = argparse.ArgumentParser(
        description="Gera um arquivo XML de viagens aleatórias, agrupadas por O-D-Início e ordenadas por Início (otimizado para arquivos grandes).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-n", "--network-file", required=True, help="Caminho para o arquivo XML da rede (mapa).")
    parser.add_argument("-t", "--num-trips", required=True, type=int, help="Número total de viagens individuais a serem geradas (antes do agrupamento).")
    parser.add_argument("-o", "--output-file", required=True, help="Caminho para o arquivo XML de saída onde as viagens serão salvas.")
    parser.add_argument("-m", "--max-time", required=True, type=int, help="Tempo máximo de simulação (segundos) para o atributo 'start' (inclusive).")
    args = parser.parse_args()

    if args.num_trips <= 0:
        print("Erro: O número de viagens (--num-trips) deve ser um inteiro positivo.", file=sys.stderr)
        sys.exit(1)
    if args.max_time < 0:
         print("Erro: O tempo máximo (--max-time) não pode ser negativo.", file=sys.stderr)
         sys.exit(1)

    print(f"Lendo a rede de forma otimizada: {args.network_file}")
    node_ids, outgoing_links = parse_network_iterative(args.network_file)

    if node_ids is None or outgoing_links is None:
        print("Falha ao processar o arquivo de rede. Encerrando.", file=sys.stderr)
        sys.exit(1)
    if not node_ids:
         print("Nenhum nó encontrado no arquivo de rede. Encerrando.", file=sys.stderr)
         sys.exit(1)
    if not outgoing_links: # Adicionado para consistência
         print("Nenhum link de saída válido encontrado no arquivo de rede. Encerrando.", file=sys.stderr)
         sys.exit(1)
    if len(node_ids) == 1:
         print(f"Aviso: Apenas um nó ('{node_ids[0]}') foi encontrado. Todas as viagens terão a mesma origem e destino.", file=sys.stderr)

    # Gerar, agregar e ordenar as viagens
    final_xml_trips = generate_aggregated_and_sorted_trips(
        node_ids,
        outgoing_links,
        args.num_trips,
        args.max_time
    )

    if not final_xml_trips and args.num_trips > 0 : # Se pedimos viagens mas nenhuma foi gerada/processada
        print("Nenhuma viagem foi gerada ou processada. Verifique os dados da rede e os parâmetros. Encerrando.", file=sys.stderr)
        sys.exit(1)
    
    if not final_xml_trips and args.num_trips == 0: # Se pedimos 0 viagens, é um caso válido para não ter output
        print("Nenhuma viagem solicitada (--num-trips 0). Arquivo de saída conterá uma estrutura vazia.", file=sys.stderr)


    # Escrever as viagens processadas para o arquivo XML
    success_writing = write_trips_to_xml_iterative(final_xml_trips, args.output_file)

    if not success_writing:
        print("Falha ao escrever o arquivo de viagens. Encerrando.", file=sys.stderr)
        try:
            if os.path.exists(args.output_file):
                os.remove(args.output_file)
                print(f"Arquivo de saída parcial '{args.output_file}' removido.", file=sys.stderr)
        except OSError as e:
            print(f"Erro ao tentar remover o arquivo de saída parcial: {e}", file=sys.stderr)
        sys.exit(1)

    print("Processo concluído com sucesso.")

if __name__ == "__main__":
    main()