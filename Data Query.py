import asyncio
from terra_sdk.client.lcd import AsyncLCDClient
from terra_sdk.client.lcd import LCDClient
import pandas as pd
from terra_sdk import exceptions
from terra_sdk import core
import json

'''
read all the related information including validators and correspoding delegators 
and transaction within each block
'''
col1 = 'timestamp'
col2 = 'height'
col3 = 'validators'
col4 = 'delegators'
col5 = 'fee_denom'
col6 = 'fee_amount'
col7 = 'height1'
col8 = 'height2'
col9 = 'delegator_address'
col10 = 'validator_address'
col11 = 'stakes'
col12 = 'commission_rate'
col13 = 'delegator_stake'
col14 = 'tx_count'
timelist = []
height = []
height1 = []
height2 = []
validators = []
delegators = []
fee_denom = []
fee_amount = []
validator_pair = []
delegator_pair = []
stakes = []
commission_rates = []
delegator_stakes = []
tx_counts = []

#"http://130.60.191.138:1317"
async def main():
    count = 1
    for i in range(7550000,7600000):
        terra = AsyncLCDClient("https://columbus-lcd.terra.dev", "columbus-5")
        try:
            time = await terra.tendermint.block_info(height=i)
            print(time['block']['header']['time'])
        except exceptions.LCDResponseError:
            i = i+1
            await terra.session.close()
            continue
        timelist.append(time['block']['header']['time'])
        height.append(i)
        try:
            txinfos = await terra.tx.tx_infos_by_height(height=i)
        except json.decoder.JSONDecodeError:
            i = i+1
            await terra.session.close()
            continue
        except ValueError:
            i = i+1
            await terra.session.close()
            continue
        except exceptions.LCDResponseError:
            i = i+1
            await terra.session.close()
            continue
        print(i)
        
        length = len(txinfos)
        validator_set = await terra.tendermint.validator_set(height=i)
        
        #print(txinfos[i].tx.to_data()['body']['messages'])
        length_validators = len(validator_set['validators'])
        for n in range(0, length):
            length1 = len(txinfos[n].tx.to_data()['auth_info']['fee']['amount'])
            
            for k in range(0, length1):
                height1.append(i)
                tx_counts.append(count)
                count = count+1
                fee_denom.append(txinfos[n].tx.to_data()['auth_info']['fee']['amount'][k]['denom'])
                fee_amount.append(txinfos[n].tx.to_data()['auth_info']['fee']['amount'][k]['amount'])
            count = 1
            # validator_pair.append(txinfos[n].tx.to_data()['body']['messages'][j]['validator_address'])
            # delegator_pair.append(txinfos[n].tx.to_data()['body']['messages'][j]['delegator_address'])
            # length1 = len(txinfos[n].tx.to_data()['body']['messages'])
            
            # delerewardinfos = txinfos[n].tx.to_data()['body']['messages']
        for m in range(0, length_validators):
            stake_each = validator_set['validators'][m]['voting_power']
            validator_addr = validator_set['validators'][m]['address']
            validator_operator_addr = core.bech32.to_val_address(validator_addr)
            validators.append(validator_operator_addr)
            stakes.append(stake_each)
            height2.append(i)
            try:
                validator_infos = await terra.staking.validator(validator = validator_operator_addr)
                commission_rate = validator_infos.commission.commission_rates.rate
                commission_rates.append(commission_rate)
                delegator_total_stake = validator_infos.delegator_shares
                delegator_stakes.append(delegator_total_stake)
            except exceptions.LCDResponseError:
                m = m+1
                commission_rates.append('0')
                delegator_stakes.append('0')
                # await terra.session.close()
                continue
            except asyncio.exceptions.TimeoutError:
                m = m+1
                commission_rates.append('0')
                delegator_stakes.append('0')
                # await terra.session.close()
                continue
            
        # col14:tx_counts,              
        await terra.session.close()
        data = pd.DataFrame({col2:height2,col3:validators})
        data.to_excel('./results/sample_data5_3.xlsx', sheet_name='sheet1', index=False)
        data1 = pd.DataFrame({col7:height1,col5:fee_denom,col6:fee_amount})
        data1.to_excel('./results/sample_data6_3.xlsx', sheet_name='sheet1', index=False)
        data2 = pd.DataFrame({col8:height2,col3:validators,col12:commission_rates,col13:delegator_stakes})
        data2.to_excel('./results/sample_data7_3.xlsx', sheet_name='sheet1', index=False)
        data3 = pd.DataFrame({col1:timelist,col2:height})
        data3.to_excel('./results/sample_data8_3.xlsx', sheet_name='sheet1', index=False)

asyncio.get_event_loop().run_until_complete(main())