import time
tic = time.time()
import pandas as pd
import gc
gc.collect()

## Importa Parquet
file_path = 'dados/transfeera_06_11_2024.parquet'
df = pd.read_parquet(file_path)
print(f'Import completo')

## Limpeza do DF
df = df[df['transaction_type'] == 'cash-out']
df = df.drop_duplicates(subset = 'transaction_id')
df.dropna(axis='columns', how='all', inplace=True)
print(f'Limpeza completa')

## altera os nomes do dataset
nomes_padronizados = {'payer_address_city': 'transaction_payer_registration_data_address_city',
'payer_address_complement': 'transaction_payer_registration_data_address_complement',
'payer_address_country': 'transaction_payer_registration_data_address_country',
'payer_address_street_name': 'transaction_payer_registration_data_address_state',
'payer_address_street_number': 'transaction_payer_registration_data_address_street_name',
'payer_address_zip_code': 'transaction_payer_registration_data_address_street_number',
'payer_bank_account_internal_id': 'transaction_payer_banking_data_account_id',
'payer_bank_account_type': 'transaction_payer_banking_data_account_type',
'payer_bank_bank_code': 'transaction_payer_banking_data_bank_code',
'payer_doc_type': 'transaction_payer_registration_data_doc_type',
'payer_doc': 'transaction_payer_registration_data_doc',
'payer_id': 'transaction_payer_id',
'payer_name': 'transaction_payer_registration_data_name',
'payer_phone_number': 'transaction_payer_registration_data_phone_number',
'payer_pix_key_type': 'transaction_payer_pix_key_type',
'payer_pix_key': 'transaction_payer_pix_key',
'receiver_address_country': 'transaction_receiver_registration_data_address_country',
'receiver_bank_account_internal_id': 'transaction_receiver_banking_data_account_id',
'receiver_bank_account_type': 'transaction_receiver_banking_data_account_type',
'receiver_bank_bank_code': 'transaction_receiver_banking_data_bank_code',
'receiver_birthdate': 'transaction_receiver_registration_data_birthdate',
'receiver_doc_type': 'transaction_receiver_registration_data_doc_type',
'receiver_doc': 'transaction_receiver_registration_data_doc',
'receiver_email': 'transaction_receiver_registration_data_email',
'receiver_id': 'transaction_receiver_id',
'receiver_name': 'transaction_receiver_registration_data_name',
'receiver_pix_key_type': 'transaction_receiver_pix_key_type',
'receiver_pix_key': 'transaction_receiver_pix_key',
'transaction_channel': 'transaction_channel',
'transaction_currency_code': 'transaction_currency_code',
'transaction_datetime_request': 'transaction_datetime_request',}
df.rename(columns=nomes_padronizados, inplace=True)
print(f'Nomes Alterados')

## Salva DF
df.to_parquet('dados/df_cashout_limpo2.parquet')
print(f'Save completo')

toc = time.time()
print(f'Tempo de Processamento: {(toc-tic)/60:.2f} minutos')
