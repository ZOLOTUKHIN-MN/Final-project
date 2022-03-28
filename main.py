#!/bin/python

# Подгружаем библиотеки

import pandas as pd
import jaydebeapi
import datetime
import glob
import os

# Подключаемся к БД

conn = jaydebeapi.connect(
    'oracle.jdbc.driver.OracleDriver',
    'jdbc:oracle:thin:de2tm/balinfundinson@de-oracle.chronosavant.ru:1521/deoracle',
    ['de2tm', 'balinfundinson'],
    '/home/de2tm/ojdbc8.jar')

curs = conn.cursor()

# Выключаем автокоммит

conn.jconn.setAutoCommit(False)

# Задаем форматы дат

curs.execute("""ALTER SESSION SET nls_timestamp_format = 'YYYY-MM-DD hh24:mi:ss'""")
curs.execute("""ALTER SESSION SET nls_date_format = 'YYYY-MM-DD'""")

# ************Инкремент***********

# Очистка таблицы-отчета

curs.execute("""DELETE FROM de2tm.mzol_rep_fraud""")

# Очистка стейджинга
## таблиц-измерений

curs.execute("""DELETE FROM de2tm.mzol_stg_terminals""")
curs.execute("""DELETE FROM de2tm.mzol_stg_cards""")
curs.execute("""DELETE FROM de2tm.mzol_stg_accounts""")
curs.execute("""DELETE FROM de2tm.mzol_stg_clients""")

curs.execute("""DELETE FROM de2tm.mzol_stg_delete_cards""")
curs.execute("""DELETE FROM de2tm.mzol_stg_delete_accounts""")
curs.execute("""DELETE FROM de2tm.mzol_stg_delete_clients""")

## таблиц-фактов

curs.execute("""DELETE FROM de2tm.mzol_stg_transactions""")
curs.execute("""DELETE FROM de2tm.mzol_stg_pssprt_blcklst""")

# Вставка в стейджинг изменений источника
## таблиц-измерений
### из бд

curs.execute("""INSERT INTO de2tm.mzol_stg_cards
                        (
                        card_num,
                        account_num,
                        create_dt,
                        update_dt
                        )
                        SELECT 
                            card_num,
                            account,
                            create_dt,
                            update_dt
                        FROM bank.cards
                        WHERE COALESCE (update_dt, create_dt) >
                        (
                        SELECT 
                            MAX(last_update_dttm)
                        FROM de2tm.mzol_meta_cards
                        )
            """)

curs.execute("""INSERT INTO de2tm.mzol_stg_accounts
                        (
                        account_num,
                        valid_to,
                        client,
                        create_dt,
                        update_dt
                        )
                    SELECT 
                        account,
                        valid_to,
                        client,
                        create_dt,
                        update_dt
                    FROM bank.accounts
                    WHERE COALESCE (update_dt, create_dt) >
                        (
                        SELECT 
                            MAX(last_update_dttm)
                        FROM de2tm.mzol_meta_accounts
                        )
            """)

curs.execute("""INSERT INTO de2tm.mzol_stg_clients
                    (
                    client_id,
                    last_name,
                    first_name,
                    patronymic,
                    date_of_birth,
                    passport_num,
                    passport_valid_to,
                    phone,
                    create_dt,
                    update_dt
                    )
                SELECT 
                    client_id,
                    last_name,
                    first_name,
                    patronymic,
                    date_of_birth,
                    passport_num,
                    passport_valid_to,
                    phone,
                    create_dt,
                    update_dt
                FROM bank.clients
                WHERE COALESCE (update_dt, create_dt) >
                    (
                    SELECT 
                        MAX(last_update_dttm)
                    FROM de2tm.mzol_meta_clients
                    )
            """)

### из excel-файла
#### путь к файлу терминалов

path_terminals = glob.glob('/home/de2tm/MZOL/terminals*.xlsx')

#### загрузка excel-файла в дф

terminals_df = pd.read_excel(path_terminals[0])

#### вставка в стг

curs.executemany("""INSERT INTO de2tm.mzol_stg_terminals 
                        (
                        terminal_id, 
                        terminal_type, 
                        terminal_city, 
                        terminal_address
                        )
                    VALUES (?, ?, ?, ?)""",
                 terminals_df.values.tolist())

## таблиц-фактов
#### путь к файлу трансакций

path_transactions = glob.glob('/home/de2tm/MZOL/transactions*.csv')

#### загрузка excel-файла в дф

transactions_df = pd.read_csv(path_transactions[0], sep=';', decimal=",")

#### преобразование дат в дф

transactions_df['transaction_date'] = transactions_df['transaction_date'].astype(str)

#### вставка в стг

curs.executemany("""INSERT INTO de2tm.mzol_stg_transactions
                        (
                        trans_id,
                        trans_date,
                        amt,
                        card_num,
                        oper_type,
                        oper_result,
                        terminal
                        )
                    VALUES (?, TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS'), ?, ?, ?, ?, ?)""",
                 transactions_df.values.tolist())

#### путь к файлу черного списка паспортов

path_pssprt_blcklst = glob.glob('/home/de2tm/MZOL/passport_blacklist*.xlsx')

#### загрузка excel-файла в дф

pssprt_blcklst_df = pd.read_excel(path_pssprt_blcklst[0])

#### выборка новых записей паспортов

curs.execute("SELECT last_update_dttm FROM de2tm.mzol_meta_pssprt_blcklst")
dttm = curs.fetchall()
pssprt_blcklst_df = pssprt_blcklst_df.loc[pssprt_blcklst_df['date'] > dttm[0][0]]

#### преобразование дат в дф

pssprt_blcklst_df['date'] = pssprt_blcklst_df['date'].astype(str)

#### вставка в стг

curs.executemany("""INSERT INTO de2tm.mzol_stg_pssprt_blcklst
                        (
                        entry_dt,
                        passport_num
                        )
                    VALUES (TO_DATE(?, 'YYYY-MM-DD'), ?)""",
                 pssprt_blcklst_df.values.tolist())

# Загрузка ключей для проверки удалений

curs.execute("""INSERT INTO de2tm.mzol_stg_delete_cards
                    (
                    card_num
                    )
                SELECT 
                    card_num
                FROM bank.cards""")

curs.execute("""INSERT INTO de2tm.mzol_stg_delete_accounts
                    (
                    account_num
                    )
                SELECT 
                    account
                FROM bank.accounts""")

curs.execute("""INSERT INTO de2tm.mzol_stg_delete_clients
                    (
                    client_id
                    )
                SELECT 
                    client_id
                FROM bank.clients""")

# Определение текущей и предыдущей даты

curs.execute("SELECT trans_date FROM de2tm.mzol_stg_transactions")
current_date = curs.fetchone()[0]

curs.execute("SELECT trans_date - interval '1' day FROM de2tm.mzol_stg_transactions")
prev_current_date = curs.fetchone()[0]

# Загрузка в приемники
## таблиц-измерений

curs.execute("""INSERT INTO de2tm.mzol_dwh_dim_terminals_hist
                        (
                        terminal_id,
                        terminal_type,
                        terminal_city, 
                        terminal_address,
                        effective_from,
                        effective_to,
                        deleted_flg
                        )
                    SELECT
                        stg.terminal_id, 
                        stg.terminal_type, 
                        stg.terminal_city, 
                        stg.terminal_address,
                        TO_DATE('{}', 'YYYY-MM-DD HH24-MI-SS') + INTERVAL '1' DAY,
                        TO_DATE('5999-12-01', 'YYYY-MM-DD'),
                        0
                    FROM de2tm.mzol_dwh_dim_terminals_hist dwh
                    FULL JOIN de2tm.mzol_stg_terminals stg
                    ON dwh.terminal_id = stg.terminal_id            
                    WHERE dwh.terminal_id IS NULL""".format(current_date))

curs.execute("""INSERT INTO de2tm.mzol_dwh_dim_accounts_hist
                        (
                        account_num,
                        valid_to,
                        client,
                        effective_from
                        )
                    SELECT
                        account_num,
                        valid_to,
                        client,
                        COALESCE (update_dt,create_dt)
                    FROM de2tm.mzol_stg_accounts""")

curs.execute("""MERGE INTO de2tm.mzol_dwh_dim_accounts_hist dwh
                    USING de2tm.mzol_stg_accounts stg
                    ON (dwh.account_num = stg.account_num 
                        AND dwh.effective_from < COALESCE (stg.update_dt, stg.create_dt))
                    WHEN MATCHED THEN UPDATE SET 
                        dwh.effective_to = COALESCE (stg.update_dt, stg.create_dt) - 1
                    WHERE dwh.effective_to = TO_DATE('5999-12-01', 'YYYY-MM-DD')""")

curs.execute("""INSERT INTO de2tm.mzol_dwh_dim_clients_hist
                        (
                        client_id,
                        last_name,
                        first_name,
                        patronymic,
                        date_of_birth,
                        passport_num,
                        passport_valid_to,
                        phone,
                        effective_from
                        )
                    SELECT
                        client_id,
                        last_name,
                        first_name,
                        patronymic,
                        date_of_birth,
                        passport_num,
                        passport_valid_to,
                        phone,
                        COALESCE (update_dt, create_dt)
                    FROM de2tm.mzol_stg_clients""")

curs.execute("""MERGE INTO de2tm.mzol_dwh_dim_clients_hist dwh
                    USING de2tm.mzol_stg_clients stg
                    ON (dwh.client_id = stg.client_id 
                        AND dwh.effective_from < COALESCE(stg.update_dt, stg.create_dt))
                    WHEN MATCHED THEN UPDATE SET
                        dwh.effective_to = COALESCE(stg.update_dt, stg.create_dt) - 1
                    WHERE dwh.effective_to = TO_DATE('5999-12-01', 'YYYY-MM-DD')""")

curs.execute("""INSERT INTO de2tm.mzol_dwh_dim_cards_hist
                        (
                        card_num,
                        account_num,
                        effective_from)
                    SELECT
                        card_num,
                        account_num,
                        COALESCE (update_dt, create_dt)
                    FROM de2tm.mzol_stg_cards""")

curs.execute("""MERGE INTO de2tm.mzol_dwh_dim_cards_hist dwh
                    USING de2tm.mzol_stg_cards stg
                    ON (dwh.card_num = stg.card_num 
                        AND dwh.effective_from < COALESCE (stg.update_dt, stg.create_dt))
                    WHEN MATCHED THEN UPDATE SET 
                        dwh.effective_to = COALESCE(stg.update_dt, stg.create_dt) - 1
                    WHERE dwh.effective_to = TO_DATE('5999-12-01', 'YYYY-MM-DD')""")

## таблиц-фактов

curs.execute("""INSERT INTO de2tm.mzol_dwh_fct_pssprt_blcklst
                        (
                        passport_num,
                        entry_dt
                        )
                    SELECT
                        passport_num,
                        entry_dt
                    FROM de2tm.mzol_stg_pssprt_blcklst""")

curs.execute("""INSERT INTO de2tm.mzol_dwh_fct_transactions
                        (
                        trans_id,
                        trans_date,
                        card_num,
                        oper_type,
                        amt,
                        oper_result,
                        terminal
                        )
                    SELECT
                        trans_id,
                        trans_date,
                        card_num,
                        oper_type,
                        amt,
                        oper_result,
                        terminal
                    FROM de2tm.mzol_stg_transactions""")

# Обновление удаленных данных таблиц-измерений
## вставка новых удаленных данных

cards = """INSERT INTO de2tm.mzol_dwh_dim_cards_hist 
                (
                card_num,
                account_num,
                effective_from,
                deleted_flg
                )
            SELECT
                dwh.card_num,
                dwh.account_num,
                TO_DATE ('{}', 'YYYY-MM-DD HH24-MI-SS'),
                1
            FROM de2tm.mzol_dwh_dim_cards_hist dwh
            LEFT JOIN de2tm.mzol_stg_delete_cards stg
            ON dwh.card_num = stg.card_num
            WHERE stg.card_num IS NULL
                AND dwh.effective_to = TO_DATE('5999-12-01','YYYY-MM-DD')
                AND dwh.deleted_flg = '0' """.format(current_date)

curs.execute(cards)

accounts = """INSERT INTO de2tm.mzol_dwh_dim_accounts_hist 
                    (
                    account_num,
                    valid_to,
                    client, 
                    effective_from,
                    deleted_flg
                    )
                SELECT
                    dwh.account_num,
                    dwh.valid_to,
                    dwh.client,
                    TO_DATE ('{}', 'YYYY-MM-DD HH24-MI-SS'),
                    1
                FROM de2tm.mzol_dwh_dim_accounts_hist dwh
                LEFT JOIN de2tm.mzol_stg_delete_accounts stg
                ON dwh.account_num = stg.account_num
                WHERE stg.account_num IS NULL
                    AND dwh.effective_to = TO_DATE('5999-12-01', 'YYYY-MM-DD')
                    AND dwh.deleted_flg = '0' """.format(current_date)

curs.execute(accounts)

clients = """INSERT INTO de2tm.mzol_dwh_dim_clients_hist 
                (
                client_id,
                last_name,
                first_name,
                patronymic, 
                date_of_birth,
                passport_num,
                passport_valid_to, 
                phone,
                effective_from,
                deleted_flg
                )
            SELECT 
                dwh.client_id,
                dwh.last_name,
                dwh.first_name,
                dwh.patronymic,
                dwh.date_of_birth,
                dwh.passport_num,
                dwh.passport_valid_to,
                dwh.phone,
                TO_DATE ('{}', 'YYYY-MM-DD HH24-MI-SS'),
                1
            FROM de2tm.mzol_dwh_dim_clients_hist dwh
            LEFT JOIN de2tm.mzol_stg_delete_clients stg
            ON dwh.client_id = stg.client_id
            WHERE stg.client_id IS NULL
                AND dwh.effective_to = TO_DATE ('5999-12-01', 'YYYY-MM-DD')
                AND dwh.deleted_flg = '0' """.format(current_date)

curs.execute(clients)

terminals = """INSERT INTO de2tm.mzol_dwh_dim_terminals_hist 
                    (
                    terminal_id,
                    terminal_type,
                    terminal_city, 
                    terminal_address,
                    effective_from,
                    deleted_flg
                    )
                SELECT
                    dwh.terminal_id,
                    dwh.terminal_type, 
                    dwh.terminal_city, 
                    dwh.terminal_address, 
                    TO_DATE ('{}', 'YYYY-MM-DD HH24-MI-SS'),
                    1
                FROM de2tm.mzol_dwh_dim_terminals_hist dwh
                FULL JOIN de2tm.mzol_stg_terminals stg
                ON dwh.terminal_id = stg.terminal_id
                WHERE stg.terminal_id IS NULL
                    AND dwh.effective_to = TO_DATE ('5999-12-01', 'YYYY-MM-DD')
                    AND dwh.deleted_flg = '0' """.format(current_date)

curs.execute(terminals)

## обновление старых удаленных данных

cards = """UPDATE de2tm.mzol_dwh_dim_cards_hist
            SET effective_to = TO_DATE ('{}', 'YYYY-MM-DD HH24-MI-SS')
            WHERE card_num IN 
                ( 
                SELECT
                    dwh.card_num
                FROM de2tm.mzol_dwh_dim_cards_hist dwh
                LEFT JOIN de2tm.mzol_stg_delete_cards stg
                ON dwh.card_num = stg.card_num
                WHERE stg.card_num IS NULL
                    AND dwh.effective_to = TO_DATE ('5999-12-01', 'YYYY-MM-DD')
                    AND dwh.deleted_flg = '0'
                )""".format(prev_current_date)

curs.execute(cards)

accounts = """UPDATE de2tm.mzol_dwh_dim_accounts_hist
                SET effective_to = TO_DATE ('{}', 'YYYY-MM-DD HH24-MI-SS') 
                WHERE account_num IN
                    ( 
                    SELECT
                        dwh.account_num
                    FROM de2tm.mzol_dwh_dim_accounts_hist dwh
                    LEFT JOIN de2tm.mzol_stg_delete_accounts stg
                    ON dwh.account_num = stg.account_num
                    WHERE stg.account_num IS NULL
                        AND dwh.effective_to = TO_DATE ('5999-12-01', 'YYYY-MM-DD')
                        AND dwh.deleted_flg = '0'
                    )""".format(prev_current_date)

curs.execute(accounts)

clients = """UPDATE de2tm.mzol_dwh_dim_clients_hist
            SET effective_to = TO_DATE ('{}', 'YYYY-MM-DD HH24-MI-SS') 
            WHERE client_id IN 
            (               
            SELECT 
                dwh.client_id
            FROM de2tm.mzol_dwh_dim_clients_hist dwh
            LEFT JOIN de2tm.mzol_stg_delete_clients stg
            ON dwh.client_id = stg.client_id
            WHERE stg.client_id IS NULL
                AND dwh.effective_to = TO_DATE ('5999-12-01', 'YYYY-MM-DD')
                AND dwh.deleted_flg = '0'
            )""".format(prev_current_date)

curs.execute(clients)

terminals = """UPDATE de2tm.mzol_dwh_dim_terminals_hist
                SET effective_to = TO_DATE ('{}', 'YYYY-MM-DD HH24-MI-SS')
                WHERE terminal_id IN 
                    ( 
                    SELECT 
                        dwh.terminal_id
                    FROM de2tm.mzol_dwh_dim_terminals_hist dwh
                    FULL JOIN de2tm.mzol_stg_terminals stg
                    ON dwh.terminal_id = stg.terminal_id
                    WHERE stg.terminal_id IS NULL    
                        AND dwh.effective_to = TO_DATE ('5999-12-01', 'YYYY-MM-DD')
                        AND dwh.deleted_flg = '0'
                    )""".format(prev_current_date)

curs.execute(terminals)

# Обновление метаданных
## таблиц-измерений

curs.execute("""UPDATE de2tm.mzol_meta_cards
                SET last_update_dttm = 
                    (
                    SELECT 
                        MAX (COALESCE (update_dt, create_dt)) 
                    FROM de2tm.mzol_stg_cards
                    )
                WHERE
                    (
                    SELECT
                        MAX(COALESCE (update_dt, create_dt))
                    FROM de2tm.mzol_stg_cards
                    ) IS NOT NULL""")

curs.execute("""UPDATE de2tm.mzol_meta_accounts
                SET last_update_dttm = 
                    (
                    SELECT
                        MAX (COALESCE (update_dt, create_dt))
                    FROM de2tm.mzol_stg_accounts
                    )
                WHERE
                    (
                    SELECT
                        MAX(COALESCE (update_dt, create_dt))
                    FROM de2tm.mzol_stg_accounts
                    ) IS NOT NULL""")

curs.execute("""UPDATE de2tm.mzol_meta_clients
                SET last_update_dttm = 
                    (
                    SELECT 
                        MAX (COALESCE (update_dt, create_dt))
                    FROM de2tm.mzol_stg_clients
                    )
                WHERE
                    (
                    SELECT
                        MAX (COALESCE (update_dt, create_dt))
                    FROM de2tm.mzol_stg_clients
                    ) IS NOT NULL""")

## таблиц-источников

curs.execute("""UPDATE de2tm.mzol_meta_pssprt_blcklst
                SET last_update_dttm = 
                    (
                    SELECT  
                        MAX (entry_dt)
                    FROM de2tm.mzol_stg_pssprt_blcklst
                    )
                WHERE
                    (
                    SELECT 
                        MAX (entry_dt)
                    FROM de2tm.mzol_stg_pssprt_blcklst
                    ) IS NOT NULL""")

curs.execute("""UPDATE de2tm.mzol_meta_transactions
                SET last_update_dttm = 
                (
                SELECT 
                    MAX (trans_date)
                FROM de2tm.mzol_stg_transactions
                )
                WHERE
                    (
                    SELECT 
                        MAX (trans_date)
                    FROM de2tm.mzol_stg_transactions
                    ) IS NOT NULL""")

# Построение отчета

curs.execute("""INSERT INTO de2tm.mzol_rep_fraud 
                    (
                    event_dt,
                    passport,
                    fio,
                    phone,
                    event_type,
                    report_dt
                    )
                SELECT 
                    trans_date,
                    passport_num, 
                    last_name||' '||first_name||' '||patronymic, 
                    phone,
                    event_type,
                    current_date 
                FROM
                    (
                    SELECT
                        client_id,
                        last_name,
                        first_name,
                        patronymic,
                        phone,
                        terminal_city,
                        trans_date,
                        card_num, 
                        next_terminal_city,
                        next_trans_date,
                        date_of_birth,
                        passport_valid_to,
                        valid_to, 
                        passport_num,
                        pssprt_blcklst,
                        CASE
                            WHEN pssprt_blcklst IS NOT NULL 
                                OR passport_valid_to < TO_DATE ('{}', 'YYYY-MM-DD HH24-MI-SS') 
                            THEN 'Совершение операции при просроченном или заблокированном паспорте'
                            WHEN valid_to < TO_DATE ('{}', 'YYYY-MM-DD HH24-MI-SS') 
                            THEN 'Совершение операции при недействующем договоре'
                            WHEN terminal_city <> next_terminal_city 
                                AND (next_trans_date - trans_date) < INTERVAL '1' HOUR
                            THEN 'Совершение операций в разных городах в течение одного часа'
                            WHEN oper_result = 'SUCCESS' 
                                AND prev_oper_result = 'REJECT' 
                                AND pre_existing_result = 'REJECT'
                                AND amt < prev_amt AND prev_amt < pre_existing_amt
                                AND trans_date - pre_existing_trans_date < INTERVAL '20' MINUTE
                            THEN 'Попытка подбора суммы'
                        END event_type
                    FROM
                        (
                        SELECT 
                            client_id,
                            last_name,
                            first_name,
                            patronymic,
                            phone, 
                            terminal_city,
                            trans_date,
                            card_num,
                            date_of_birth,
                            passport_valid_to,
                            valid_to,
                            passport_num,
                            amt, 
                            oper_result,
                            prev_trans_date,
                            prev_amt,
                            prev_oper_result, 
                            next_terminal_city,
                            next_trans_date,
                            pssprt_blcklst,
                            COALESCE (LAG(prev_trans_date) 
                                OVER (PARTITION BY card_num 
                                    ORDER BY trans_date), TO_DATE ('1900-01-01', 'YYYY-MM-DD')) 
                            AS pre_existing_trans_date,
                            COALESCE (LAG(prev_amt) 
                                OVER (PARTITION BY card_num 
                                        ORDER BY trans_date), 0) 
                            AS pre_existing_amt,
                            COALESCE (LAG(prev_oper_result) 
                                OVER (PARTITION BY card_num 
                                    ORDER BY trans_date), 'нет данных') 
                            AS pre_existing_result
                        FROM
                            (
                            SELECT 
                                cl.client_id,
                                cl.last_name,
                                cl.first_name,
                                cl.patronymic,
                                cl.phone,
                                ter.terminal_city,
                                tr.trans_date,
                                car.card_num,
                                cl.date_of_birth,
                                cl.passport_valid_to,
                                acc.valid_to,
                                pas.passport_num AS pssprt_blcklst, 
                                cl.passport_num,
                                tr.amt,
                                tr.oper_result,
                                COALESCE (LEAD(ter.terminal_city) 
                                    OVER (PARTITION BY car.card_num 
                                        ORDER BY tr.trans_date), 'нет данных') 
                                AS next_terminal_city,
                                COALESCE (LEAD(tr.trans_date) 
                                    OVER (PARTITION BY car.card_num 
                                        ORDER BY tr.trans_date), TO_DATE ('5999-12-31', 'YYYY-MM-DD')) 
                                AS next_trans_date,
                                COALESCE (LAG(tr.trans_date) 
                                    OVER (PARTITION BY tr.card_num 
                                        ORDER BY tr.trans_date), TO_DATE ('1900-01-01', 'YYYY-MM-DD')) 
                                AS prev_trans_date,
                                COALESCE (LAG(tr.amt) 
                                    OVER (PARTITION BY tr.card_num 
                                        ORDER BY tr.trans_date), 0) 
                                        AS prev_amt,
                                COALESCE (LAG(tr.oper_result) 
                                    OVER (PARTITION BY tr.card_num 
                                        ORDER BY tr.trans_date), 'нет данных') 
                                AS prev_oper_result
                            FROM de2tm.mzol_dwh_fct_transactions tr
                            LEFT JOIN de2tm.mzol_dwh_dim_cards_hist car
                            ON tr.card_num = RTRIM(car.card_num)
                            LEFT JOIN de2tm.mzol_dwh_dim_accounts_hist acc
                            ON car.account_num = acc.account_num
                            LEFT JOIN de2tm.mzol_dwh_dim_clients_hist cl
                            ON acc.client = cl.client_id
                            LEFT JOIN de2tm.mzol_dwh_fct_pssprt_blcklst pas
                            ON cl.passport_num = pas.passport_num
                            LEFT JOIN de2tm.mzol_dwh_dim_terminals_hist ter
                            ON ter.terminal_id = tr.terminal
                            )
                        )
                    )
                WHERE event_type IS NOT NULL""".format(current_date, current_date))

# Сохранение трансакций

conn.commit()

# Закрытие соединения

conn.close()

# Перемещение и сохранение файлов-исходников в архив

os.replace(path_pssprt_blcklst[0],  os.path.join('/home/de2tm/MZOL/archive', os.path.basename(path_pssprt_blcklst[0] + '.backup')))
os.replace(path_terminals[0],  os.path.join('/home/de2tm/MZOL/archive', os.path.basename(path_terminals[0] + '.backup')))
os.replace(path_transactions[0],  os.path.join('/home/de2tm/MZOL/archive', os.path.basename(path_transactions[0] + '.backup')))