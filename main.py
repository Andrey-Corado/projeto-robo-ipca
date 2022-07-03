# Arquivo principal, constituído pela invocação dos métodos do arquivo "Functions" e interface de comunicação com o usuário
# Bibliotecas de dependências: PySimpleGUI
# Versão do Python utilizada: 3.10

from functions import capture_data
import PySimpleGUI as sg


# Layout de construção da interface gráfica que será exibida ao usuário

layout = [
    [
        sg.Button('Iniciar')
    ],
    [
        sg.Output(size=(100,30))
    ]
]

janela = sg.Window('Dados IPCA - CNI', layout=layout, finalize=True)

#Laço de repetição infinito, para manter a tela sempre visível ao usuário

while True:

    button, values = janela.read()
    
    if button in (None, 'Cancel'):        
        break
    
    if button == sg.WINDOW_CLOSED:        
        break
    
    if button == 'Iniciar':

        #inicio do processo de execução dos métodos

        print('Iniciando processo, aguarde...')

        try:

            capture_data() #invocação da função que inicia o processo de captura e tratamentos dos dados, localizada no arquivo "Functions".

            print('Processo finalizado com sucesso!')

            print('Aguarde a proxima janela de execução...')

        except:

            print('Processo finalizado com erro, tente novamente!')
