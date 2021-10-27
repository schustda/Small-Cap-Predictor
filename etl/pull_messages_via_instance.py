from reagan import PSQL, Ihub
from time import sleep

def add_new_code(message_data, ps):
    if message_data.get('ihub_code',''):
        ihub_code = message_data['ihub_code']

        # fixing an error
        ihub_code = ihub_code.replace('%','%%')
        
        if not ps.get_scalar("SELECT CASE WHEN '{0}' IN (SELECT ihub_code FROM items.symbol) THEN 1 ELSE 0 END".format(ihub_code)):
            try:
                ihub_id = ihub_code.split('-')[-1]
                symbol = ihub_code.split('-')[-2]
                # name = ihub_code.replace(f'-{symbol}-{ihub_id}','').replace('-',' ')
                name = ihub_code.replace("-{0}-{1}".format(symbol,ihub_id),'').replace('-',' ')
                # ps.execute(f'''INSERT INTO items.symbol (symbol, name, ihub_code, status, ihub_id) VALUES ('{symbol}','{name}','{ihub_code}','active','{ihub_id}')''')
                ps.execute("INSERT INTO items.symbol (symbol, name, ihub_code, status, ihub_id) VALUES ('{0}','{1}','{2}','active','{3}')".format(symbol,name,ihub_code,ihub_id))
            except:
                # ps.execute(f'''INSERT INTO items.symbol (ihub_code, status) VALUES ('{ihub_code}','active')''')
                ps.execute("INSERT INTO items.symbol (ihub_code, status) VALUES ('{ihub_code}','active')".format(ihub_code))
            return

def pull_new_messages():
    while True:
        db = PSQL('scp')
        queue = db.to_list('''SELECT message_id FROM ihub.missing_ids ORDER BY RANDOM() LIMIT 1000''')
        ihub = Ihub()
        for message_id in queue:
            print(message_id)
            sleep(1)
            message_data = ihub.get_message_data(message_id)
            add_new_code(message_data, db)
            # to_update = ','.join([f"{k} = '{str(v)}' " for k,v in message_data.items()])
            to_update = ','.join(['''{0} = '{1}' '''.format(k,str(v)) for k,v in message_data.items()])
            # db.execute(f'''UPDATE ihub.message_sentiment SET {to_update}, updated_date = NOW() WHERE message_id = {message_id}''')
            db.execute("UPDATE ihub.message_sentiment SET {0}, updated_date = NOW() WHERE message_id = {1}".format(to_update, message_id))

pull_new_messages()