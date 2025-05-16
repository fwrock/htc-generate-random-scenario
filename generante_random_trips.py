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
        # Usa iterparse para processar o XML elemento por elemento
        # 'start' e 'end' são os eventos que queremos capturar
        context_iter = ET.iterparse(network_file, events=('start', 'end'))
        # Pula o elemento raiz inicial, se necessário (depende da estrutura exata)
        # event, root = next(context_iter)

        for event, elem in context_iter:
            # Quando encontramos o início de <nodes> ou <links>, definimos o contexto
            if event == 'start':
                if elem.tag == 'nodes':
                    context = 'nodes'
                elif elem.tag == 'links':
                    context = 'links'
                # Se estivermos dentro de <nodes> e encontrarmos um <node>
                elif context == 'nodes' and elem.tag == 'node':
                    node_id = elem.get('id')
                    if node_id:
                        node_ids.append(node_id)
                # Se estivermos dentro de <links> e encontrarmos um <link>
                elif context == 'links' and elem.tag == 'link':
                    from_node = elem.get('from')
                    link_id = elem.get('id')
                    if from_node and link_id:
                        if from_node not in outgoing_links:
                            outgoing_links[from_node] = []
                        outgoing_links[from_node].append(link_id)

            # Quando encontramos o fim de </nodes> ou </links>, limpamos o contexto
            # e liberamos a memória do elemento processado
            elif event == 'end':
                if elem.tag == 'nodes':
                    context = None
                    print(f"Processados {len(node_ids)} nós.")
                elif elem.tag == 'links':
                    context = None
                    print(f"Processados links (origens únicas: {len(outgoing_links)}).")

                # Importante: Limpa o elemento da memória após processá-lo
                # Isso é crucial para baixo consumo de memória
                elem.clear()
                # Opcional: Limpa também os irmãos anteriores para liberar mais memória
                # while elem.getprevious() is not None:
                #     del elem.getparent()[0]


        print("Leitura iterativa concluída.")

        # Validações após a leitura
        if not node_ids:
            print(f"Aviso: Nenhum nó (<node> com atributo 'id') encontrado em {network_file}", file=sys.stderr)
            # return None, None # Pode decidir falhar aqui

        if not outgoing_links:
             print(f"Aviso: Nenhum link (<link>) com 'from' e 'id' válidos encontrado em {network_file}.", file=sys.stderr)
             # return None, None # Pode decidir falhar aqui

        # Verifica consistência (opcional)
        valid_origin_nodes_count = 0
        for node in node_ids:
            if node in outgoing_links and outgoing_links[node]:
                valid_origin_nodes_count += 1
        if valid_origin_nodes_count == 0 and node_ids and outgoing_links:
             print("Erro Crítico: Existem nós e links, mas nenhum nó parece ser uma origem válida de um link. Verifique os IDs.", file=sys.stderr)
             return None, None

        return node_ids, outgoing_links

    except ET.ParseError as e:
        print(f"Erro: Falha ao analisar (parse) o arquivo XML da rede: {network_file}. Detalhes: {e}", file=sys.stderr)
        return None, None
    except FileNotFoundError: # Captura redundante, já verificado no início, mas seguro
        print(f"Erro: Arquivo de rede não encontrado: {network_file}", file=sys.stderr)
        return None, None
    except Exception as e:
        print(f"Erro inesperado durante a leitura iterativa de {network_file}: {e}", file=sys.stderr)
        return None, None

def generate_and_write_trips_iterative(node_ids, outgoing_links, num_trips, max_start_time, output_file):
    """
    Gera viagens aleatórias e as escreve diretamente no arquivo de saída XML
    de forma iterativa para economizar memória.

    Args:
        node_ids (list): Lista de IDs de nós disponíveis.
        outgoing_links (dict): Dicionário mapeando IDs de nós de origem para listas de IDs de links de saída.
        num_trips (int): O número de viagens a gerar.
        max_start_time (int): O tempo máximo de início (em segundos) para uma viagem.
        output_file (str): Caminho para o arquivo XML de saída.

    Returns:
        bool: True se a escrita foi bem-sucedida, False caso contrário.
    """
    if not node_ids:
        print("Erro: Não há nós disponíveis para gerar viagens.", file=sys.stderr)
        return False

    valid_origin_nodes = [node for node in node_ids if node in outgoing_links and outgoing_links[node]]

    if not valid_origin_nodes:
        print("Erro: Nenhum nó de origem com links de saída válidos foi encontrado.", file=sys.stderr)
        return False

    print(f"Gerando e escrevendo {num_trips} viagens para {output_file}...")
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            # Escreve o cabeçalho XML e a tag raiz de abertura
            f.write('<?xml version="1.0" encoding="utf-8"?>\n')
            f.write('<scsimulator_matrix>\n') # Adiciona nova linha para legibilidade

            generated_count = 0
            for i in range(1, num_trips + 1):
                # --- Seleciona Origem ---
                origin_node = random.choice(valid_origin_nodes)
                possible_link_origins = outgoing_links[origin_node]
                link_origin = random.choice(possible_link_origins)

                # --- Seleciona Destino (diferente da origem) ---
                destination_node = random.choice(node_ids)
                while len(node_ids) > 1 and destination_node == origin_node:
                    destination_node = random.choice(node_ids)
                if len(node_ids) == 1 and destination_node == origin_node:
                     # Aviso já é dado na função principal se necessário
                     pass

                # --- Gera Tempo de Início ---
                start_time = random.randint(0, max_start_time)

                # --- Monta a string do elemento <trip> manualmente ---
                # Usamos escape() para garantir que valores com caracteres especiais (&, <, >) sejam tratados corretamente
                trip_attrs = {
                    'name': f"trip_{i}",
                    'origin': origin_node,
                    'destination': destination_node,
                    'link_origin': link_origin,
                    'count': "1",
                    'start': str(start_time),
                    'mode': "car",
                    'digital_rails_capable': "false"
                }
                # Constrói a string do atributo escapando os valores
                attr_string = " ".join(f'{k}="{escape(str(v))}"' for k, v in trip_attrs.items())
                # Escreve o elemento <trip> diretamente no arquivo
                # Adiciona indentação manual para legibilidade
                f.write(f'  <trip {attr_string}/>\n')
                generated_count += 1

                # Feedback de progresso para arquivos muito grandes (opcional)
                if i % 10000 == 0:
                    print(f"  ... {i}/{num_trips} viagens escritas ...")


            # Escreve a tag raiz de fechamento
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
    """
    Função principal para analisar argumentos e orquestrar a geração de viagens.
    """
    parser = argparse.ArgumentParser(
        description="Gera um arquivo XML de viagens aleatórias com base em um mapa de rede XML (otimizado para arquivos grandes).",
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

    args = parser.parse_args()

    if args.num_trips <= 0:
        print("Erro: O número de viagens (--num-trips) deve ser um inteiro positivo.", file=sys.stderr)
        sys.exit(1)

    if args.max_time < 0:
         print("Erro: O tempo máximo (--max-time) não pode ser negativo.", file=sys.stderr)
         sys.exit(1)

    # --- Leitura Otimizada da Rede ---
    print(f"Lendo a rede de forma otimizada: {args.network_file}")
    node_ids, outgoing_links = parse_network_iterative(args.network_file)

    if node_ids is None or outgoing_links is None:
        print("Falha ao processar o arquivo de rede. Encerrando.", file=sys.stderr)
        sys.exit(1)
    if not node_ids:
         print("Nenhum nó encontrado no arquivo de rede. Encerrando.", file=sys.stderr)
         sys.exit(1)
    if not outgoing_links:
         print("Nenhum link de saída válido encontrado no arquivo de rede. Encerrando.", file=sys.stderr)
         sys.exit(1)
    if len(node_ids) == 1:
         print(f"Aviso: Apenas um nó ('{node_ids[0]}') foi encontrado. Todas as viagens terão a mesma origem e destino.", file=sys.stderr)


    # --- Geração e Escrita Otimizada das Viagens ---
    success = generate_and_write_trips_iterative(
        node_ids,
        outgoing_links,
        args.num_trips,
        args.max_time,
        args.output_file
    )

    if not success:
        print("Falha ao gerar ou escrever o arquivo de viagens. Encerrando.", file=sys.stderr)
        # Opcional: Tentar remover o arquivo parcialmente escrito
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
