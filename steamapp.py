from h2o_wave import Q, main, app, ui, on, handle_on
import pandas as pd
import h2osteam
from h2osteam.clients import AdminKubernetesClient
import traceback
# import psycopg2

steamDataFrame: pd.DataFrame = None
keycloakDataFrame: pd.DataFrame = None
curTableData: pd.DataFrame = None

def get_keycloak_events(env='prod'):
    kuserdata = None
    df=get_config(env)
    print('fetching the keycloak data from '+ str(env))
    conn_string = "host="+ df.pghost +" port="+ "5432" +" dbname="+ df.pgdatabase +" user=" + df.pguser  +" password="+ df.pgpassword
    try:
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        cur.execute("select evt.client_id, usr.username from event_entity as evt, user_entity as usr where usr.id=evt.user_id group by  evt.client_id,usr.username order by evt.client_id, usr.username;")
        kuserdata = cur.fetchall()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    print('fetched the data from keycloak'+pd.DataFrame(kuserdata, columns=['client_id','username']).head())
    keyclockDf = pd.DataFrame(kuserdata, columns=['client_id','username'])
    return keyclockDf

def get_config(env='prod'):
    print('get config data for '+ str(env))
    df=pd.read_csv('config.csv',index_col=0)
    return df.loc[env]

def get_steam_usage(env='prod'):
    data = None
    df=get_config(env)
    print('fetching the steam usage data from '+ str(env))
    path="steam_"+env+"_usage_data.csv"
    try:
        steamadmin = h2osteam.login(url=df.url, username=df.user, password=df.password, verify_ssl=True)
        admin_instance = AdminKubernetesClient(steamadmin)
        admin_instance.download_dai_usage_statistics(path)
        usage_data = pd.read_csv(path)
        data=usage_data[['username','instance_name','instance_id','version','profile_name','cpu_count','gpu_count','memory_gb','storage_gb',
                         'session_launch_date','session_end_reason', 'session_duration_sec' ]].drop_duplicates()
        print('fetched the data from steam usage')
    except:
        #e = sys.exc_info()[0]
        print("H2o Connecitivty exception: " + traceback.format_exc())
    return data



envchoice = [
    ui.choice('prod', 'Production'),
    ui.choice('dev', 'Development')
]

userschoice = [
    ui.choice('A', 'Steam Users'),
    ui.choice('B', 'H2o Wave Users'),
    ui.choice('C', 'MLOPS Users')
]

steamchoice = [
    ui.choice('A', 'Raw data'),
    ui.choice('B', 'Filter by date'),
    ui.choice('C', 'Top RAM'),
    ui.choice('D', 'Top Storage'),
    ui.choice('E', 'Plots')
    ]

topchoice = [
    ui.choice('A', '10'),
    ui.choice('B', '25'),
    ui.choice('C', '50')
    ]

@on('get_data')
async def onGetData(q: Q):
    global steamDataFrame
    global keycloakDataFrame
    global curTableData
    data_page = q.page["datacard"]
    print('In get data function : ' + str(q.args.env_choice))
    q.client.steamdata = steamDataFrame = get_steam_usage(q.args.env_choice)
    q.client.keycloakdata = keycloakDataFrame = steamDataFrame #get_keycloak_events(q.args.env_choice)
    data_page.items= [ ui.text_xl('Data have been loaded from : '+str(q.args.env_choice)),]
    doUserCardInit(q)
    

@on('show_users')
async def onShowUsers(q: Q):
    global steamDataFrame
    global keycloakDataFrame
    global curTableData
    curTableData = None
    data_page = q.page["datacard"]
    print('In show users function : ' + str(q.args.users_choice))
    if (q.args.users_choice == 'A') :
        print(steamDataFrame.head())
        curTableData = steamDataFrame
        showSteamOption(q)
        data_page.items = showTable(curTableData.columns, curTableData, "steam")
    elif (q.args.users_choice == 'B' ):
        print(keycloakDataFrame.head())
        curTableData = keycloakDataFrame.query("client_id == 'h2oaic-wave'")
        data_page.items = showTable(curTableData.columns, curTableData, "wave")
    elif ( q.args.users_choice == 'C'):
        print(keycloakDataFrame.head())
        curTableData = keycloakDataFrame.query("client_id == 'mlops'")
        data_page.items = showTable(curTableData.columns, curTableData, "mlops")
    else : 
        data_page.items= [ui.text_xl('Something wrong in selecting users'),]

@on('sfilter_data')
async def onSteamFilters(q: Q):
    global steamDataFrame
    global curTableData
    data_page = q.page["datacard"]
    if (q.args.steam_choice == 'A') :
        curTableData = steamDataFrame
        data_page.items = showTable(curTableData.columns, curTableData,"steam")
    elif (q.args.steam_choice == 'B' ):
        curTableData = steamDataFrame
        data_page.items = showTable(curTableData.columns, curTableData,"steam")
    elif ( q.args.steam_choice == 'C'):
        curTableData = steamDataFrame
        data_page.items = showTable(curTableData.columns, curTableData)
    elif ( q.args.steam_choice == 'D'):
        curTableData = steamDataFrame
        data_page.items = showTable(curTableData.columns, curTableData)
    elif ( q.args.steam_choice == 'E'):
        curTableData = steamDataFrame
        data_page.items = showTable(curTableData.columns, curTableData)
    else : 
        data_page.items= [ui.text_xl('Something wrong in filter options'),]

@on('steam_choice')
async def onSteamChoice(q: Q):
    steamfilter_page =q.page['steamoptioncard']
    if (q.args.steam_choice == 'A') :
        steamfilter_page.items= [steamfilter_page.items[0],
                                 ui.button(name='sfilter_data', label='Filter', primary=True)]
    elif (q.args.steam_choice == 'B' ):
        steamfilter_page.items= [steamfilter_page.items[0],
                                 ui.date_picker(name='start_date'),
                                 ui.date_picker(name='end_date'),
                                 ui.button(name='sfilter_data', label='Filter', primary=True)]
    elif ( q.args.steam_choice == 'C'):
        steamfilter_page.items= [steamfilter_page.items[0],
                                 ui.dropdown(name='topNRAM',value='A', required=True,choices=topchoice),
                                 ui.button(name='sfilter_data', label='Filter', primary=True)]
    elif ( q.args.steam_choice == 'D'):
        steamfilter_page.items= [steamfilter_page.items[0],
                                 ui.dropdown(name='topNStorage',value='A', required=True,choices=topchoice),
                                 ui.button(name='sfilter_data', label='Filter', primary=True)]
    elif ( q.args.steam_choice == 'E'):
        steamfilter_page.items= [steamfilter_page.items[0],
                                 ui.dropdown(name='topNStorage',value='A', required=True,choices=topchoice),
                                 ui.button(name='sfilter_data', label='Filter', primary=True)]
    else : 
        steamfilter_page.items= [steamfilter_page.items[0],ui.text_xl('Something wrong in filter options'),]

def showTable(table_headers,table_rows,label:str = None):
    items =  None
    dataCols = [ui.table_column(name='_'+str(col),label=str(col)) for col in table_headers]
    dataRows = [buildRow(row)  for row in table_rows.iterrows()]
    items=[
        ui.table(
            name='f{label}',
            columns=dataCols,
            rows=dataRows,
        ) ]
    
    if label !=None:
        items=[ui.text_xl(label),
        items[0]
        ]
    return items

def buildRow(row):
    global curTableData
    types = curTableData.dtypes
    cells = [getCellValueByType(val,type)  for val,type in zip(row[1],types)]
    return ui.table_row(name=str(row[0]), cells = cells)

def getCellValueByType(val,type):
    return str(val)

@app('/')
async def serve(q: Q):
    global steamDataFrame
    global keycloakDataFrame
    if not await handle_on(q):
        show_pages(q)
    await q.page.save()

def show_pages(q: Q):
    q.page['envcard'] = ui.form_card( title="Environment selection", box='1 1 2 2', items=[
                                    ui.choice_group(name='env_choice', value='prod', required=True, choices=envchoice),
                                    ui.button(name='get_data', label='Get Data', primary=True)]
            )
    q.page['datacard'] = ui.form_card(box='3 1 8 10', items=[
                ui.text_xl('Waiting for data to be loaded'),
                ])
    doUserCardInit(q)

def doUserCardInit(q:Q):
    q.page['usercard'] = ui.form_card(title="Application selection", box='1 3 2 3', items=[
                ui.choice_group(name='users_choice', required=True, choices=userschoice),
                ui.button(name='show_users', label='Show Users', primary=True),
            ])

def showSteamOption(q: Q):
    q.page['steamoptioncard'] = ui.form_card( title="Steam Data", box='1 6 2 3', items=[
                                    ui.dropdown(name='steam_choice', label="filter by",value='A', required=True, choices=steamchoice,trigger=True)]
            )
