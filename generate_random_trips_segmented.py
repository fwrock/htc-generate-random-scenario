import xml.etree.ElementTree as ET
import random
import argparse
import sys
import os
import json # Para analisar as faixas de tempo
from xml.sax.saxutils import escape # Para escapar atributos XML manualmente

def parse_network_iterative(network_file):
    """
    Analisa o arquivo XML da rede de forma iterativa para extrair IDs de nós
    e links de saída, otimizado para baixo uso de memória.
    (Função original mantida - sem alterações)
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
        
        actual_valid_origins = {node for node in node_ids if node in outgoing_links and outgoing_links[node]}
        if not actual_valid_origins and node_ids and outgoing_links: # Adicionando a verificação crítica que estava em comentários
             print("Erro Crítico: Existem nós e links, mas nenhum nó parece ser uma origem válida de um link com ID. Verifique os IDs e a estrutura do link.", file=sys.stderr)
             # Dependendo da criticidade, você pode decidir retornar None, None aqui.
             # Por ora, permite continuar; generate_and_write_trips_iterative falhará se não houver valid_origin_nodes.

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

def parse_time_slots_json(json_string_or_path, max_total_time_simulation):
    """
    Analisa a definição de faixas de tempo de uma string JSON ou arquivo.
    Formato esperado: [{"name": "Madrugada", "start_hour": 0, "end_hour": 7, "percentage": 0.10}, ...]
    Converte horas para segundos. 'end_hour' é exclusivo.
    Valida se as porcentagens somam aproximadamente 1.0.
    """
    try:
        if os.path.isfile(json_string_or_path):
            with open(json_string_or_path, 'r', encoding='utf-8') as f:
                slots_input = json.load(f)
        else:
            slots_input = json.loads(json_string_or_path)
    except json.JSONDecodeError as e:
        print(f"Erro: JSON inválido para --time-slots: {e}", file=sys.stderr)
        return None
    except FileNotFoundError:
        print(f"Erro: Arquivo JSON para --time-slots não encontrado: {json_string_or_path}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Erro ao ler/processar --time-slots: {e}", file=sys.stderr)
        return None

    if not isinstance(slots_input, list):
        print("Erro: --time-slots JSON deve ser uma lista de objetos.", file=sys.stderr)
        return None

    processed_slots = []
    total_percentage = 0.0

    # Ordenar por hora de início para facilitar a verificação de sobreposição e lacunas
    try:
        slots_input.sort(key=lambda x: float(x.get("start_hour", -1)))
    except (TypeError, ValueError):
        print("Erro: 'start_hour' inválido ou ausente em alguma faixa horária. Não foi possível ordenar.", file=sys.stderr)
        return None


    last_end_sec = -1
    for i, slot_def in enumerate(slots_input):
        if not all(k in slot_def for k in ["start_hour", "end_hour", "percentage"]):
            print(f"Erro: Definição de slot inválida, faltam chaves (start_hour, end_hour, percentage): {slot_def}", file=sys.stderr)
            return None
        
        try:
            start_hour = float(slot_def["start_hour"])
            end_hour = float(slot_def["end_hour"])
            percentage = float(slot_def["percentage"])
            name = slot_def.get("name", f"Faixa {i+1}: {start_hour}h-{end_hour}h")
        except ValueError:
            print(f"Erro: Valores não numéricos em start_hour, end_hour, ou percentage no slot: {slot_def}", file=sys.stderr)
            return None

        if start_hour < 0 or end_hour <= start_hour or percentage <= 0 or percentage > 1.0:
            print(f"Erro: Valores inválidos no slot (start_hour >= 0, end_hour > start_hour, 0 < percentage <= 1.0): {name}", file=sys.stderr)
            return None

        start_sec = int(start_hour * 3600)
        # end_hour é exclusivo, então o último segundo da faixa é end_hour * 3600 - 1
        end_sec = int(end_hour * 3600) - 1 

        if start_sec <= last_end_sec :
             print(f"Erro: Faixas de tempo se sobrepõem ou estão fora de ordem. '{name}' ({start_sec}s) começa antes ou no mesmo tempo que o fim da faixa anterior ({last_end_sec}s).", file=sys.stderr)
             return None
        
        # Verifica se a faixa está completamente fora do tempo máximo de simulação
        if start_sec > max_total_time_simulation:
            print(f"Aviso: A faixa '{name}' ({start_sec}s) começa após o tempo máximo de simulação ({max_total_time_simulation}s) e será ignorada.", file=sys.stderr)
            continue # Pula esta faixa

        processed_slots.append({
            "name": name,
            "start_sec": start_sec,
            "end_sec": end_sec, # Será limitado por max_total_time_simulation na geração
            "percentage": percentage
        })
        total_percentage += percentage
        last_end_sec = end_sec

    if not (0.999 <= total_percentage <= 1.001):
        print(f"Aviso: A soma das porcentagens das faixas de tempo é {total_percentage:.4f}, que não é 1.0. As viagens serão distribuídas proporcionalmente.", file=sys.stderr)
        if total_percentage > 0: # Normaliza se a soma não for zero
            for slot in processed_slots:
                slot['percentage'] /= total_percentage
        else:
            print("Erro: Soma total das porcentagens é zero ou negativa. Não é possível normalizar.", file=sys.stderr)
            return None
            
    if not processed_slots:
        print("Erro: Nenhuma faixa horária válida definida ou todas as faixas estão além do tempo máximo de simulação.", file=sys.stderr)
        return None
        
    return processed_slots

def generate_and_write_trips_iterative(
    node_ids,
    outgoing_links,
    num_trips,
    max_start_time_simulation, # Tempo máximo global da simulação
    output_file,
    time_slot_definitions, # Lista de dicts: {"name", "start_sec", "end_sec", "percentage"}
    percentage_od_equal # float 0.0 a 1.0
):
    if not node_ids:
        print("Erro: Não há nós disponíveis para gerar viagens.", file=sys.stderr)
        return False

    valid_origin_nodes = [node for node in node_ids if node in outgoing_links and outgoing_links[node]]
    if not valid_origin_nodes:
        print("Erro: Nenhum nó de origem com links de saída válidos foi encontrado.", file=sys.stderr)
        return False

    is_single_node_network = len(node_ids) == 1
    actual_percentage_od_equal = percentage_od_equal
    if is_single_node_network:
        if percentage_od_equal < 1.0: # Avisa se o usuário esperava destinos diferentes
             print(f"Aviso: A rede possui apenas um nó ('{node_ids[0]}'). Todas as {num_trips} viagens terão origem e destino iguais.", file=sys.stderr)
        actual_percentage_od_equal = 1.0 # Força 100% O=D

    print(f"Gerando e escrevendo {num_trips} viagens para {output_file}...")
    print(f"  Target O=D iguais: {actual_percentage_od_equal*100:.2f}%")
    print(f"  Distribuição de tempo pelas faixas horárias (considerando max_start_time_simulation={max_start_time_simulation}s):")

    # 1. Preparar a lista de horários de início para todas as viagens
    all_trip_start_times = []
    trips_allocated_count = 0
    for i, slot in enumerate(time_slot_definitions):
        # Limita o fim da faixa pelo tempo máximo da simulação
        slot_actual_start_s = slot['start_sec']
        slot_actual_end_s = min(slot['end_sec'], max_start_time_simulation)

        if slot_actual_start_s > max_start_time_simulation or slot_actual_start_s > slot_actual_end_s: # Faixa inútil
            print(f"  - Faixa '{slot['name']}' ({slot_actual_start_s}-{slot_actual_end_s}s) ignorada ou vazia devido a max_start_time_simulation.")
            num_in_this_slot = 0
            if i == len(time_slot_definitions) - 1: # Se for a última faixa e ela for inútil
                 # Tenta alocar as viagens restantes para a última faixa útil, se houver
                remaining_trips_to_allocate = num_trips - trips_allocated_count
                if remaining_trips_to_allocate > 0:
                    print(f"  Tentando alocar {remaining_trips_to_allocate} viagens restantes para faixas anteriores válidas...")
                    # Esta lógica de realocação pode ficar complexa; por ora, a perda de viagens é uma possibilidade
                    # se a última faixa for a única com porcentagem restante e for inválida.
                    # Uma abordagem mais simples é garantir que `num_trips` seja atingido, adicionando ao último slot válido.
            pass # Continuar para a próxima faixa
        else:
            print(f"  - Faixa '{slot['name']}': {slot_actual_start_s}-{slot_actual_end_s}s ({slot['percentage']*100:.1f}%)")


        if i == len(time_slot_definitions) - 1: # Última faixa pega todas as viagens restantes
            num_in_this_slot = num_trips - trips_allocated_count
        else:
            num_in_this_slot = round(num_trips * slot['percentage'])
        
        trips_allocated_count += num_in_this_slot

        for _ in range(num_in_this_slot):
            # Garante que mesmo que start e end sejam iguais, um valor seja gerado
            all_trip_start_times.append(random.randint(slot_actual_start_s, slot_actual_end_s))
    
    # Ajuste para garantir o número exato de viagens devido a arredondamentos ou faixas ignoradas
    # Se faltarem viagens, adiciona-as à última faixa válida ou, como fallback, à primeira faixa válida.
    while len(all_trip_start_times) < num_trips:
        # Encontra a última faixa válida para adicionar as viagens restantes
        target_slot_for_remainder = None
        for slot_def in reversed(time_slot_definitions):
            s_start = slot_def['start_sec']
            s_end = min(slot_def['end_sec'], max_start_time_simulation)
            if s_start <= s_end and s_start <= max_start_time_simulation :
                target_slot_for_remainder = (s_start, s_end)
                break
        
        if target_slot_for_remainder:
            all_trip_start_times.append(random.randint(target_slot_for_remainder[0], target_slot_for_remainder[1]))
        else: # Fallback extremo: se nenhuma faixa for válida (deveria ser pego antes)
            print("Aviso: Nenhuma faixa válida para alocar viagens restantes. Usando [0, max_start_time_simulation].", file=sys.stderr)
            all_trip_start_times.append(random.randint(0, max_start_time_simulation))
            if len(all_trip_start_times) >= num_trips : break # Evita loop se max_start_time for 0

    all_trip_start_times = all_trip_start_times[:num_trips] # Garante o número exato

    # 2. Determinar quais viagens serão O=D
    target_od_equal_trips = int(num_trips * actual_percentage_od_equal)
    trip_is_od_equal_flags = [True] * target_od_equal_trips + [False] * (num_trips - target_od_equal_trips)
    random.shuffle(trip_is_od_equal_flags)

    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('<?xml version="1.0" encoding="utf-8"?>\n')
            f.write('<scsimulator_matrix>\n')

            generated_count = 0
            for i in range(num_trips):
                current_start_time = all_trip_start_times[i]
                is_this_trip_od_equal = trip_is_od_equal_flags[i]

                origin_node = random.choice(valid_origin_nodes)
                possible_link_origins = outgoing_links[origin_node]
                link_origin = random.choice(possible_link_origins)

                if is_this_trip_od_equal or is_single_node_network:
                    destination_node = origin_node
                else:
                    destination_node = random.choice(node_ids)
                    attempts = 0
                    max_attempts = len(node_ids) + 5 
                    while destination_node == origin_node and attempts < max_attempts:
                        destination_node = random.choice(node_ids)
                        attempts +=1
                    if destination_node == origin_node: 
                        other_nodes = [n for n in node_ids if n != origin_node]
                        if other_nodes:
                            destination_node = random.choice(other_nodes)
                        # Se não, destination_node permanece origin_node (O=D efetivo)

                trip_attrs = {
                    'name': f"trip_{i+1}",
                    'origin': origin_node,
                    'destination': destination_node,
                    'link_origin': link_origin,
                    'count': "1",
                    'start': str(current_start_time),
                    'mode': "car",
                    'digital_rails_capable': "false"
                }
                attr_string = " ".join(f'{k}="{escape(str(v))}"' for k, v in trip_attrs.items())
                f.write(f'  <trip {attr_string}/>\n')
                generated_count += 1

                if (i + 1) % 10000 == 0 and num_trips > 0: # Evita divisão por zero se num_trips for 0
                    print(f"  ... {i+1}/{num_trips} viagens escritas ...")

            f.write('</scsimulator_matrix>\n')

        print(f"{generated_count} viagens escritas com sucesso em: {output_file}")
        return True

    except IOError as e:
        print(f"Erro de I/O ao escrever no arquivo de saída {output_file}: {e}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Erro inesperado ao gerar/escrever viagens para {output_file}: {e}", file=sys.stderr)
        return False

def main():
    parser = argparse.ArgumentParser(
        description="Gera um arquivo XML de viagens aleatórias com base em um mapa de rede XML, com controle de tempo e O=D.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-n", "--network-file",
        required=True,
        help="Caminho para o arquivo XML da rede (mapa)."
    )
    parser.add_argument(
        "-t", "--num-trips",
        required=True,
        type=int,
        help="Número de viagens aleatórias a serem geradas."
    )
    parser.add_argument(
        "-o", "--output-file",
        required=True,
        help="Caminho para o arquivo XML de saída onde as viagens serão salvas."
    )
    parser.add_argument(
        "-m", "--max-time",
        required=True,
        type=int,
        help="Tempo máximo de simulação (em segundos) para definir o limite superior do atributo 'start' da viagem (inclusive)."
    )
    parser.add_argument(
        "--time-slots",
        required=True,
        help="Definição das faixas de tempo e suas porcentagens. Pode ser uma string JSON ou caminho para um arquivo JSON. "
             "Exemplo: '[{\"name\": \"Madrugada\", \"start_hour\": 0, \"end_hour\": 7, \"percentage\": 0.10}, ...]' "
             "Onde 'end_hour' é exclusivo (ex: 7 significa até 6:59:59). As porcentagens devem somar 1.0. As faixas devem ser ordenadas por start_hour e não devem se sobrepor."
    )
    parser.add_argument(
        "--percentage-od-equal",
        type=float,
        default=0.0,
        help="Porcentagem de viagens que terão origem e destino iguais (0.0 a 1.0). Default: 0.0 (0%%)."
    )

    args = parser.parse_args()

    if args.num_trips <= 0:
        print("Erro: O número de viagens (--num-trips) deve ser um inteiro positivo.", file=sys.stderr)
        sys.exit(1)
    if args.max_time < 0:
         print("Erro: O tempo máximo (--max-time) não pode ser negativo.", file=sys.stderr)
         sys.exit(1)
    if not (0.0 <= args.percentage_od_equal <= 1.0):
        print("Erro: --percentage-od-equal deve estar entre 0.0 e 1.0.", file=sys.stderr)
        sys.exit(1)

    time_slot_definitions = parse_time_slots_json(args.time_slots, args.max_time)
    if time_slot_definitions is None:
        sys.exit(1)
    
    # Verifica se alguma faixa horária utilizável existe dentro do max_time
    usable_slots_within_maxtime = any(
        slot['start_sec'] <= args.max_time and slot['start_sec'] <= min(slot['end_sec'], args.max_time)
        for slot in time_slot_definitions
    )
    if not usable_slots_within_maxtime:
        print(f"Erro: Nenhuma das faixas horárias definidas em --time-slots ocorre ou é válida dentro do --max-time ({args.max_time}s) especificado.", file=sys.stderr)
        sys.exit(1)


    print(f"Lendo a rede de forma otimizada: {args.network_file}")
    node_ids, outgoing_links = parse_network_iterative(args.network_file)

    if node_ids is None or outgoing_links is None:
        print("Falha ao processar o arquivo de rede. Encerrando.", file=sys.stderr)
        sys.exit(1)
    if not node_ids: # parse_network_iterative já avisa, mas é bom ter uma saída limpa
         print("Nenhum nó encontrado no arquivo de rede. Encerrando.", file=sys.stderr)
         sys.exit(1)

    success = generate_and_write_trips_iterative(
        node_ids,
        outgoing_links,
        args.num_trips,
        args.max_time,
        args.output_file,
        time_slot_definitions,
        args.percentage_od_equal
    )

    if not success:
        print("Falha ao gerar ou escrever o arquivo de viagens. Encerrando.", file=sys.stderr)
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