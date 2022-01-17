import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
# import pyarrow

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

colunas_dengue = [
    'id',
    'data_iniSE',
    'casos',
    'ibge_code',
    'cidade',
    'uf',
    'cep',
    'latitude',
    'longitude'
]

def lista_para_dicionario(elemento, colunas):
    """
    Recebe duas listas
    Retorna um dicionário com as colunas definindo as chaves e o elemento definindo os valores
    """
    return dict(zip(colunas, elemento))

def texto_para_lista(elemento, delimitador='|'):
    """
    Recebe um texto e um delimitador
    Retorna uma lista de elementos pelo delimitador
    """
    return elemento.split(delimitador)

def trata_datas(elemento):
    """
    Recebe um dicionário e cria um novo campo com ANO-MES
    Retorna o mesmo dicionário com o novo campo 
    """
    elemento['ano_mes'] = '-'.join(elemento['data_iniSE'].split('-')[:2])
    return elemento

def chave_uf(elemento):
    """
    Recebe um dicionário
    Retorna uma tupla com o Estado (UF) e o elemento (UF, dicionário)
    """
    chave = elemento['uf']
    return (chave, elemento)

def casos_dengue(elemento):
    """
    Recebe uma tupla no formato ('RS', [{}, {}])
    Retorna uma tupla no formato ('RS-2014-12', 8.0)
    """
    uf, registros = elemento
    for registro in registros:
        if bool(re.search(r'\d', registro['casos'])):
            yield (f"{uf}-{registro['ano_mes']}", float(registro['casos']))
        else:
            yield (f"{uf}-{registro['ano_mes']}", 0.0)

def chave_uf_ano_mes_de_lista(elemento):
    """
    Recebe uma lista de elementos
    Retorna uma tupla contendo uma chave e o valor de chuva em mm no formato ('UF-ANO-MES', 1.3)
    """
    data, mm, uf = elemento
    ano_mes_chuva = '-'.join(data.split('-')[:2])
    chave = f'{uf}-{ano_mes_chuva}'
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)
    return chave, mm

def arredonda(elemento):
    """
    Recebe uma tupla com dois items
    Retorna uma tupla com o segundo valor arredondado
    """
    chave , mm = elemento
    return (chave, round(mm, 1))

def filtra_campos_vazios(elemento):
    """
    Remove elementos que tenham chaves vazias
    Recebe uma tupla no formato ('CE-2015-02', {'chuvas': [], 'dengue': [337.0]})
    Retorna uma tupla no formato ('CE-2015-07', {'chuvas': [38.0], 'dengue': [242.0]})
    """
    chaves, dados = elemento
    if all([dados['chuvas'], dados['dengue']]):
        return True
    return False

def descompactar_elementos(elemento):
    """
    Recebe uma tupla no formato ('CE-2015-07', {'chuvas': [38.0], 'dengue': [242.0]})
    Retorna uma tupla no formato ('CE', '2015', '07', '38.0', '242.0')
    """
    chave, dados = elemento
    chuva = dados['chuvas'][0]
    dengue = dados['dengue'][0]
    uf, ano, mes = chave.split('-')
    # Preparando para persistir em arquivo Parquet
    # chaves = ['uf', 'ano', 'mes', 'chuvas', 'dengue']
    # resultado = dict(zip(chaves, [uf, int(ano), int(mes), chuva, dengue]))
    # return resultado
    return uf, ano, mes, str(chuva), str(dengue)

def preparar_csv(elemento, delimitador=';'):
    """
    Recebe uma tupla no formato ('CE', 2015, 07, 38.0, 242.0)
    Retorna uma string delimitada contendo todos os elementos da tupla anterior no formato 'CE; 2015; 07; 38.0; 242.0'
    """
    return f"{delimitador}".join(elemento)

# schema = [('uf', pyarrow.string()),
#           ('ano', pyarrow.int32()),
#           ('mes', pyarrow.int32()),
#           ('chuvas', pyarrow.float32()),
#           ('dengue', pyarrow.float32())
#           ]

dengue = (
    pipeline
    | "Leitura de dataset de casos de dengue" >> ReadFromText('casos_dengue.txt', skip_header_lines=1)
    | "De texto para lista" >> beam.Map(texto_para_lista)
    | "De lista para dicionário" >> beam.Map(lista_para_dicionario, colunas_dengue)
    | "Criar campo ano_mes" >> beam.Map(trata_datas)
    | "Criar chave pelo Estado" >> beam.Map(chave_uf)
    | "Agrupar pelo Estado" >> beam.GroupByKey()
    | "Descompactar casos de dengue" >> beam.FlatMap(casos_dengue)
    | "Soma dos casos de dengue pela chave" >> beam.CombinePerKey(sum)
    # | "Mostrar resultados de casos de dengue" >> beam.Map(print)
)

chuvas = (
    pipeline
    | "Leitura de dataset de casos de chuvas" >> ReadFromText('chuvas.csv', skip_header_lines=1)
    | "De texto para lista de chuvas" >> beam.Map(texto_para_lista, delimitador=',')
    | "Criando a chave UF-ANO-MES e convertendo chuva em float" >> beam.Map(chave_uf_ano_mes_de_lista)
    | "Soma do total de chuvas pela chave" >> beam.CombinePerKey(sum)
    | "Arredondar resultados de chuvas" >> beam.Map(arredonda)
    # | "Mostrar resultados de chuvas" >> beam.Map(print)
)

resultado = (
    # (chuvas, dengue)
    # | "Empilha as pcollections" >> beam.Flatten()
    # | "Agrupa as pcollections" >> beam.GroupByKey()
    ({'chuvas' : chuvas, 'dengue': dengue})
    | "Mesclar pcollections" >> beam.CoGroupByKey()
    | "Filtrar dados vazios" >> beam.Filter(filtra_campos_vazios)
    | "Descompactar elementos" >> beam.Map(descompactar_elementos)
    # | "Persistindo em Parquet" >> beam.io.WriteToParquet('resultado_parquet', file_name_suffix='.parquet', schema=pyarrow.schema(schema))
    | "Preparar csv" >> beam.Map(preparar_csv)
    # | "Mostrar resultados da junção de chuvas e casos de dengue" >> beam.Map(print)
)

header = 'UF;ANO;MES;CHUVA;DENGUE'

resultado | "Criar arquivo csv" >> WriteToText('resultado', file_name_suffix='.csv', header= header)

pipeline.run()
