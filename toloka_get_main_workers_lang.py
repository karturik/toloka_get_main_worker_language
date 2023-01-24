import pandas as pd
import requests
import toloka.client as toloka


URL_WORKER = 'https://toloka.yandex.ru/requester/worker/'
URL_API = "https://toloka.yandex.ru/api/v1/"
OAUTH_TOKEN = ''
HEADERS = {"Authorization": "OAuth %s" % OAUTH_TOKEN, "Content-Type": "application/JSON"}
toloka_client = toloka.TolokaClient(OAUTH_TOKEN, 'PRODUCTION')

# PASS LIST OF POOLS FOR WORKERS CHECK
list_of_pools = []
project_id = 0

df_toloka_full = pd.DataFrame()

for pool_number in list_of_pools:
    df_toloka = toloka_client.get_assignments_df(pool_number, status = ['APPROVED', 'SUBMITTED', 'REJECTED'])
    df_toloka = df_toloka['ASSIGNMENT:worker_id']
    df_toloka_full = pd.concat([df_toloka_full, df_toloka])

df_toloka_full = df_toloka_full.drop_duplicates()

df_toloka_full.to_csv(f'all_workers_{project_id}.tsv', sep='\t', index=False)

df_workers_data = pd.DataFrame(columns=['worker_id', 'worker_country', 'worker_languages', 'project_number'], data=None)

# GET ALL WORKER'S DATA
for worker_id in df_toloka_full['ASSIGNMENT:worker_id']:
    worker_data = {}
    print(worker_id)
    worker = requests.get(url='https://toloka.yandex.ru/api/new/requester/workers/' + worker_id, headers=HEADERS).json()
    print(worker)
    worker_data['worker_id'] = worker_id
    worker_data['worker_country'] = worker['country']
    worker_data['worker_languages'] = str(worker['languages'])
    print(worker_data['worker_languages'])
    worker_data['project_number'] = project_id
    final_all_df = pd.DataFrame(columns=['worker_id', 'worker_country', 'worker_languages', 'project_number'], data = worker_data, index=[0])
    df_workers_data = pd.concat([df_workers_data, final_all_df])
    worker_data = {}

df_workers_data.to_csv(f'workers_project_{project_id}.csv', sep=',', index=False)
