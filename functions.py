# Arquivo destinado a construção dos métodos do projeto
# Bibliotecas de dependências:  Requests, Pandas, pyarrow, fastparquet, tkinter.filedialog 
# Versão do Python utilizada: 3.10


import requests
import pandas as pd
import tkinter.filedialog as fdlg



# Método responsável por acessar a api, capturar os dados e armazenar em uma variável, para iniciar o tratamento

def capture_data():

    print('Iniciando captura de dados, aguarde...')

    try:

        url = 'https://sidra.ibge.gov.br/Ajax/JSon/Tabela/1/1737?versao=-1' # Url da api de onde os dados serão consumidos
        response = requests.get(url).json() # Variável responsável por guardar os dados capturados, em formato json
        print('Dados capturados com sucesso!')
        treat_data(response) # Invocação da função responsável por tratar os dados

    except:

        print('Erro ao capturar dados, tente novamente!')


# Método responsável por efetuar o tratamento dos dados, para geranção do arquivo parquet, recebe o objeto que é enviado pelo método "capture_data"

def treat_data(response):

    print('Iniciando tratamento dos dados, aguarde...')

    try:

        consolidated = [] # Array responsável por guardar o resultado final, com os dados tratados
        periods = [] # Array responsável por armazenar os dados do atributo "Periodos" do objeto de retorno da API"
        conjunts = [] # Array responsável por armazenar os dados do atributo "Conjuntos" do objeto de retorno da API"
        measurement_units = [] # Array responsável por armazenar os dados do atributo "UnidadesDeMedida" do objeto de retorno da API"
        variables = [] # Array responsável por armazenar os dados do atributo "Variaveis" do objeto de retorno da API"
        territories = []


        # Varáveis responsável por guardar dados do objeto de retorno da API, quando existe apenas um registro por atributo

        result_id = response['Id']
        result_name = response['Nome']
        result_font = response['Fonte']
        result_notes = response['Notas'][0]
        search_id = response['Pesquisa']['Id']
        search_name = response['Pesquisa']['Nome']
        search_theme = response['Pesquisa']['Temas'][0]
        period_name_title = response['Periodos']['Nome']
        period_frequency = response['Periodos']['Periodicidade']

        if len(response['Classificacoes']) > 0:
            classifications = response['Classificacoes']
        else:
            classifications = ''

        territories_name = response['Territorios']['Nome']
        territories_dict_levels_id = response['Territorios']['DicionarioNiveis']['Ids'][0]
        territories_dict_levels_name = response['Territorios']['DicionarioNiveis']['Nomes'][0]
        territories_dict_levels_dt_extinction = response['Territorios']['DicionarioNiveis']['DatasExtincao'][0]
        territories_dict_units_id = response['Territorios']['DicionarioUnidades']['Ids'][0]
        territories_dict_units_name = response['Territorios']['DicionarioUnidades']['Nomes'][0]
        territories_dict_units_level = response['Territorios']['DicionarioUnidades']['Niveis'][0]
        territories_dict_units_uf = response['Territorios']['DicionarioUnidades']['SiglasUF'][0]
        territories_dict_units_cod_ibge = response['Territorios']['DicionarioUnidades']['CodigosIBGE'][0]
        territories_dict_units_comp1 = response['Territorios']['DicionarioUnidades']['Complementos1'][0]
        territories_dict_units_comp2 = response['Territorios']['DicionarioUnidades']['Complementos2'][0]
        territories_dict_units_dt_extinction = response['Territorios']['DicionarioUnidades']['DatasExtincao'][0]

        for level in response['Territorios']['NiveisTabela']:

            territories_level_table_id = level['Id']
            territories_level_table_initials = level['Sigla']
            territories_level_table_Availability = level['Disponibilidade']
            territories_level_table_territorial_mesh = level['PossuiMalhaTerritorial']
            territories_level_table_omitted_scope = level['AbrangenciasOmitidas']
            territories_level_table_unit = level['Unidades'][0]

            if len(level['NiveisAbrangentes']) > 0:
                territories_level_table_comprehensive_Level = level['NiveisAbrangentes'][0]
            else:
                territories_level_table_comprehensive_Level = ''
            
            if len(response['Territorios']['VisoesTerritoriais']) > 0:
                territories_territorial_view = response['Territorios']['VisoesTerritoriais'][0]
            else:
                territories_territorial_view =''




        # Laço de repetição responsável por alimentar o array "consolidated"

        for dict in response['Periodos']['Periodos']:

            item = ( dict['Id'], dict['Codigo'], dict['Nome'], dict['Disponivel'], dict['DataLiberacao'] )
            periods.append( item )



        # Laço de repetição responsável por alimentar o array "conjunts"

        for dict in response['Periodos']['Conjuntos']:

            for row in dict["Periodos"]:

                item = ( dict["Id"], dict['Nome'], row )
                conjunts.append( item )



        # Laço de repetição responsável por alimentar o array "measurement_units"

        for dict in response['UnidadesDeMedida']:

            item = ( dict["Id"], dict['Nome'])
            measurement_units.append( item )


        # Laço de repetição responsável por alimentar o array "variables"

        for dict in response['Variaveis']:

            for row in dict['UnidadeDeMedida']:

                if dict['VariaveisDerivadas'] is None:
                    derivative_variable = dict['VariaveisDerivadas'][0]
                else:
                    derivative_variable = ''

                item = ( 
                    dict["Id"], dict['Nome'], dict['Desidentificacao'], dict['DecimaisApresentacao'], dict['DecimaisArmazenamento'], dict['Descricao'], derivative_variable, row['Periodo'], row['Unidade']
                )

                variables.append(item)


        # Laço de repetição responsável por alimentar o array "consolidated"
        for unit in measurement_units:    

            for variable in variables:

                for period in periods:

                    period_id = period[0]
                    period_cod = period[1]
                    period_name = period[2]
                    period_available = period[3]
                    period_liberation_date = period[4]
                    conjunt_id = ''
                    conjunt_name = ''
                    conjunt_period = 0

                    conjunts_cons = []

                    for conjunt in conjunts:

                        if conjunt[2] == period[0]:
                            conjunt_id = conjunt[0]
                            conjunt_name = conjunt[1]
                            conjunt_period = conjunt[2]

                            item = (conjunt_id, conjunt_name, conjunt_period)

                            conjunts_cons.append( item )

                    if len(conjunts_cons) > 0:
                        for list in conjunts_cons:

                            conjunt_id = list[0]
                            conjunt_name = list[1]
                            conjunt_period = list[2]

                            item = (
                                result_id, result_name, result_font, result_notes, search_id, search_name, search_theme, period_name_title, period_frequency, variable[0], 
                                variable[1], variable[2], variable[3], variable[4], variable[5], variable[6], variable[7], variable[8],period_id, period_cod, period_name, 
                                period_available, unit[0], unit[1], classifications, territories_name, 
                                territories_dict_levels_id, territories_dict_levels_name, territories_dict_levels_dt_extinction, territories_dict_units_id, territories_dict_units_name, 
                                territories_dict_units_level, territories_dict_units_uf, territories_dict_units_cod_ibge, territories_dict_units_comp1, territories_dict_units_comp2, 
                                territories_dict_units_dt_extinction, territories_level_table_id, territories_level_table_initials, territories_level_table_Availability, territories_level_table_territorial_mesh,
                                territories_level_table_omitted_scope, territories_level_table_unit, territories_level_table_comprehensive_Level, territories_territorial_view, 
                                conjunt_id, conjunt_name, conjunt_period,
                                pd.to_datetime(str(period_liberation_date).replace('T', ' ')), pd.to_datetime(str(response['DataAtualizacao']).replace('T', ' '))
                            )

                            consolidated.append( item )

                    else:
                        item = (
                            result_id, result_name, result_font, result_notes, search_id, search_name, search_theme, period_name_title, period_frequency, variable[0], 
                            variable[1], variable[2], variable[3], variable[4], variable[5], variable[6], variable[7], variable[8],period_id, period_cod, period_name, 
                            period_available, unit[0], unit[1], classifications, territories_name, 
                            territories_dict_levels_id, territories_dict_levels_name, territories_dict_levels_dt_extinction, territories_dict_units_id, territories_dict_units_name, 
                            territories_dict_units_level, territories_dict_units_uf, territories_dict_units_cod_ibge, territories_dict_units_comp1, territories_dict_units_comp2, 
                            territories_dict_units_dt_extinction, territories_level_table_id, territories_level_table_initials, territories_level_table_Availability, territories_level_table_territorial_mesh,
                            territories_level_table_omitted_scope, territories_level_table_unit, territories_level_table_comprehensive_Level, territories_territorial_view, 
                            conjunt_id, conjunt_name, conjunt_period,
                            pd.to_datetime(str(period_liberation_date).replace('T', ' ')), pd.to_datetime(str(response['DataAtualizacao']).replace('T', ' '))
                        )

                        consolidated.append( item )
                
        print('Dados tratados com sucesso!')
        
        file_generate(consolidated) # Invocação da função responsável por criar o arquivo parquet

    except:

        print('Erro ao tratar dados, tente novamente!')



# Método responsável por criar o arquivo parquet, recebendo o array com os dados consolidados enviados pelo método "treat_data"

def file_generate(consolidated):

    print('Iniciando geração de arquivo .parquet, aguarde...')

    try:

        directory =  fdlg.asksaveasfilename(defaultextension='parquet')

        columns=[
            'result_id', 'result_name', 'result_font', 'result_notes', 'search_id', 'search_name', 'search_theme', 'period_name_title', 'period_frequency', 'variable_id', 'variable_name', 'variable_de_identification', 
            'variable_decimals_presentation', 'variable_decimal_storage', 'variable_description', 'variable_derivatives_variable', 'variable_period', 'variable_measurement_units',
            'period_id', 'period_cod', 'period_name', 'period_available', 'id_measurement_units', 'name_measurement_units', 'classifications', 'territories_name', 
            'territories_dict_levels_id', 'territories_dict_levels_name', 'territories_dict_levels_dt_extinction', 'territories_dict_units_id', 'territories_dict_units_name', 
            'territories_dict_units_level', 'territories_dict_units_uf', 'territories_dict_units_cod_ibge', 'territories_dict_units_comp1', 'territories_dict_units_comp2', 
            'territories_dict_units_dt_extinction', 'territories_level_table_id', 'territories_level_table_initials', 'territories_level_table_Availability', 'territories_level_table_territorial_mesh', 
            'territories_level_table_omitted_scope', 'territories_level_table_unit', 'territories_level_table_comprehensive_Level', 'territories_territorial_view', 'conjunt_id', 'conjunt_name', 'conjunt_period',
            'period_liberation_date', 'date_atualization'
        ]
        
        df = pd.DataFrame(data=consolidated, columns=columns)

        df.to_parquet(directory)

        print('Arquivo gerado com sucesso!')

    except:

        print('erro ao gerar arquivo, tente novamente!')
