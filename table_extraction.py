from sqlalchemy import create_engine
import pandas as pd
import os
import re
import psycopg2
from py4j.java_gateway import JavaGateway
from subprocess import *

cwd = os.getcwd()
jar_file = cwd + "/test_jdbc-1.0-SNAPSHOT-jar-with-dependencies.jar"

def table_extraction(url, username, password, path):
    _start_gateway()
    gateway = JavaGateway()
    conn_type = url.split(':')[0]
    conn_string  = url.split("//")[0] + "//" + username + ':' + password + "@" + url.split("//")[1]
    java_conn = "jdbc:" + url
    #print(conn_type, conn_string, java_conn)
    postgres_engine = _check_db_connection(conn_string)
    overview_dict, table_dict, view_dict = _plot_postgres_db(postgres_engine)
    sql_files = _get_files(path)
    #print(sql_files)
    file_list = []
    org_sql_list = []
    sql_list = []
    table_list = []
    for f in sql_files:
        org_sql = open(f, mode='r', encoding='utf-8-sig').read()
        # special treatment for mimic dataset, should be changed later on
        sql = _preprocess_str(org_sql).replace('physionet-data.', '').replace('oe.VALUE', 'oe.value_temp')
        temp = sql.split('JOIN')
        t = []
        if len(temp) >= 1:
            for i in temp[1:]:
                t.append(i.split(maxsplit=1)[0])
        temp = sql.split('FROM')
        if len(temp) >= 1:
            for i in temp[1:]:
                t.append(i.split(maxsplit=1)[0])
        for i in t:
            if "mimiciii_clinical." + i in overview_dict['table_names']:
                sql = sql.replace(i, "mimiciii_clinical." + i)
                sql = sql.replace("mimiciii_clinical.mimiciii_clinical." + i, "mimiciii_clinical." + i)
        sql = sql.lower()
        # special treatment ends here
        #print(sql)
        try:
            extracted_tables = gateway.get_table(java_conn, username, password, sql)
            file_list.append(os.path.basename(f))
            org_sql_list.append(org_sql)
            table_list.append(extracted_tables)
            sql_list.append(sql)
            print(f, extracted_tables)
            #print(df)
            gateway.close()
        except:
            print(f"error in the SQL")
    df = pd.DataFrame({'file': file_list, 'original sql': org_sql_list, 'sql':sql_list, 'tables': table_list})
    return df, table_dict

def _check_db_connection(conn_string):
    try:
        psycopg2.connect(conn_string)
        print("database connected")
    except:
        print("authentication error")
    return create_engine(conn_string)

def _check_connection():
    test_gateway = JavaGateway()
    try:
        # Call a dummy method just to make sure we can connect to the JVM
        test_gateway.jvm.System.currentTimeMillis()
    except Py4JNetworkError:
        # We could not connect. Let"s wait a long time.
        # If it fails after that, there is a bug with our code!
        sleep(2)
    finally:
        print("gateway tested")
        test_gateway.close()

def _start_gateway():
    args = [jar_file] # Any number of args to be passed to the jar file
    p = Popen(['java', '-jar']+list(args), stdout=PIPE, stderr=PIPE, shell = True)
    print("gateway opened")
    _check_connection()

def _get_files(path):
    if os.path.isfile(path):
        sql_files = [path]
    elif os.path.isdir(path):
        sql_files = [os.path.join(path, f) for f in os.listdir(path) if f.endswith('.sql') or f.endswith('.SQL')]
    else:
        sql_files = []
    return sql_files

def _find_parens(s):
    toret = {}
    pstack = []
    for i, c in enumerate(s):
        if c == '(':
            pstack.append(i)
        elif c == ')':
            if len(pstack) == 0:
                raise IndexError("No matching closing parens at: " + str(i))
            toret[pstack.pop()] = i

    if len(pstack) > 0:
        raise IndexError("No matching opening parens at: " + str(pstack.pop()))
    return toret

def _preprocess_str(str1):
    # remove the /* */ comments
    q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", str1)
    # remove whole line -- and # comments
    lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]
    # remove trailing -- and # comments
    q = " ".join([re.split("--|#", line)[0] for line in lines])
    # replace all spaces around commas
    q = re.sub(r'\s*,\s*', ',', q)
    # replace all multiple spaces to one space
    str1 = re.sub("\s\s+", " ", q)
    str1 = re.sub('union distinct', 'UNION', str1, flags=re.IGNORECASE)
    # bracket positions
    toret = _find_parens(str1)

    # change the format of DATETIME_DIFF to TIMESTAMPDIFF
    datediffs = [m.start() for m in re.finditer('DATETIME_DIFF', str1, flags=re.IGNORECASE)]
    datediff_nums = len(datediffs)
    datediff_idx = datediffs[0] + 13 if datediff_nums != 0 else None
    for i in range(datediff_nums):
        if datediff_idx in toret.keys():
            temp = str1[datediff_idx + 1:toret[datediff_idx]].split(',')
            str1 = str1[:datediff_idx-13] + "TIMESTAMPDIFF(" + temp[2] + "," + temp[0] + "," + temp[1] + ")" + str1[toret[datediff_idx] + 1:]
            toret = _find_parens(str1)
            datediff_idx = re.search('DATETIME_DIFF', str1, flags=re.IGNORECASE).start() + 13 if i < datediff_nums -1 else None

    # change width_bucket to NTILE since it is not implemented yet
    widthbuckets = [m.start() for m in re.finditer('width_bucket', str1, flags=re.IGNORECASE)]
    widthbucket_nums = len(widthbuckets)
    widthbucket_idx = widthbuckets[0] + 12 if widthbucket_nums != 0 else None
    for i in range(widthbucket_nums):
        if widthbucket_idx in toret.keys():
            temp = str1[widthbucket_idx + 1:toret[widthbucket_idx]].split(',')
            str1 = str1[:widthbucket_idx-12] + "NTILE(" + temp[3] +")" + str1[toret[widthbucket_idx] + 1:]
            toret = _find_parens(str1)
            widthbucket_idx = re.search('width_bucket', str1, flags=re.IGNORECASE).start() + 12 if i < widthbucket_nums -1 else None

    # remove DISTINCT ON -- future work to find the column
    distincts = [m.start() for m in re.finditer('DISTINCT ON', str1, flags=re.IGNORECASE)]
    distinct_nums = len(distincts)
    distinct_idx = distincts[0] + 12 if distinct_nums != 0 else None
    for i in range(distinct_nums):
        if distinct_idx in toret.keys():
            str1 = str1[:distinct_idx-12] + str1[toret[distinct_idx]+2:]
            toret = _find_parens(str1)
            distinct_idx = re.search('DISTINCT ON', str1, flags=re.IGNORECASE).start() + 12 if i < distinct_nums -1 else None

    # remove GENERATE_SERIES -- future work to find the columns
    gens = [m.start() for m in re.finditer('GENERATE_SERIES', str1, flags=re.IGNORECASE)]
    gen_nums = len(gens)
    gen_idx = gens[0] + 15 if gen_nums != 0 else None
    for i in range(gen_nums):
        if gen_idx in toret.keys():
            str1 = str1[:gen_idx-15] + 'CAST(1 AS Integer)' + str1[toret[gen_idx]+1:]
            toret = _find_parens(str1)
            gen_idx = re.search('GENERATE_SERIES', str1, flags=re.IGNORECASE).start() + 15 if i < gen_nums -1 else None

    # remove date_trunc
    truncs = [m.start() for m in re.finditer('date_trunc', str1, flags=re.IGNORECASE)]
    trunc_nums = len(truncs)
    trunc_idx = truncs[0] + 10 if trunc_nums != 0 else None
    for i in range(trunc_nums):
        if trunc_idx in toret.keys():
            sub = str1[trunc_idx-10:toret[trunc_idx]+1]
            str1 = str1[:trunc_idx-10] + sub.split(',')[1][:-1] + str1[toret[trunc_idx]+1:]
            toret = _find_parens(str1)
            trunc_idx = re.search('date_trunc', str1, flags=re.IGNORECASE).start() + 10 if i < trunc_nums -1 else None

    # change DATETIME_ADD to TIMESTAMPADD
    dates = [m.start() for m in re.finditer('DATETIME_ADD', str1, flags=re.IGNORECASE)]
    date_nums = len(dates)
    date_idx = dates[0] + 12 if date_nums != 0 else None
    for i in range(date_nums):
        if date_idx in toret.keys():
            s = str1[date_idx-12:toret[date_idx]+1]
            temp = s.split(',')
            #print(temp)
            sub = "TIMESTAMPADD(" + temp[-1][:-1].split(" ")[-1] + "," + re.split(r"(.*)(?=\s)", s)[1].split('INTERVAL')[-1] + "," + re.split(r"(\()(.*)", re.split(r"(.*)(?=\,)", s)[1])[-2] + ")"
            str1 = str1[:date_idx-12] + sub + str1[toret[date_idx]+1:]
            toret = _find_parens(str1)
            date_idx = re.search('DATETIME_ADD', str1, flags=re.IGNORECASE).start() + 12 if i < date_nums -1 else None

    # adjust to create view
    idx = str1.find("CREATE VIEW")
    if idx != -1:
        idx = str1.find("AS", idx)
        str1 = str1[idx+3:]
    # change brackets around table names
    from_index = str1.find('FROM')
    flag = True
    if toret:
        while flag:
            for i in toret.keys():
                # to elminate single bracket after from a table
                if from_index != -1 and from_index + 5 in toret.keys():
                    i = from_index + 5
                    if str1[i+1:i+8].casefold() != 'select '.casefold():
                        str1 = str1[:i] + str1[i+1:toret[i]] + str1[toret[i]+1:]
                        from_index = str1.find('FROM'.casefold(), from_index+1)
                        toret = _find_parens(str1)
                        flag = True
                        break
                if i+1 in toret.keys():
                    # to eliminate general double brackets
                    if toret[i+1] == toret[i] - 1:
                        str1 = str1[:i] + str1[i+1:toret[i]] + str1[toret[i]+1:]
                        toret = _find_parens(str1)
                        flag = True
                        break
                    # to eliminate double brackets before select
                    elif str1[i+2:i+8].casefold() == 'select'.casefold():
                        str1 = str1[:i] + str1[i+1:toret[i]] + str1[toret[i]+1:]
                        toret = _find_parens(str1)
                        flag = True
                        break
                flag = False
            if flag:
                continue
            else:
                break
    return str1.replace('`', '').replace(';', '').replace('!=', '<>').strip()

def _plot_postgres_db(postgres_engine):
    # Table level SQL, schema name, table name, row count
    table_sql = pd.read_sql("""SELECT s.schemaname, concat_ws('.', s.schemaname, tablename) AS table_name, hasindexes, n_live_tup AS row_count
      FROM pg_stat_user_tables s
      JOIN pg_tables t ON t.tablename = s.relname AND t.schemaname = s.schemaname ORDER BY 1,2;""", postgres_engine)
#     pd.read_sql("""SELECT t.schemaname, concat_ws('.', t.schemaname, t.tablename) AS table_name, hasindexes, CAST(reltuples AS integer) AS row_count FROM pg_class c
# JOIN pg_tables t on t.tablename = c.relname AND c.relnamespace = t.schemaname::regnamespace::oid
# WHERE t.schemaname != 'pg_catalog' AND t.schemaname != 'information_schema' AND relkind='r' ORDER BY 1,2""", postgres_engine)
    # View level SQL
    view_sql = pd.read_sql("""SELECT schemaname, concat_ws('.', v.schemaname, v.viewname) AS view_name, definition FROM pg_class c
JOIN pg_views v on v.viewname = c.relname AND c.relnamespace = v.schemaname::regnamespace::oid
WHERE v.schemaname != 'pg_catalog' AND v.schemaname != 'information_schema' AND relkind = 'v' ORDER BY 1,2""", postgres_engine)
    # PK/FK constraints
    pk_fk = pd.read_sql("""SELECT conname as constraint_name,
        CASE
            WHEN contype = 'p' THEN 'primary key'
            WHEN contype = 'f' THEN 'foreign key'
            WHEN contype = 'u' THEN 'unique key'
        END AS constraint_type
          , concat_ws('.', n.nspname, conrelid::regclass) AS "table_name"
          , CASE WHEN pg_get_constraintdef(c.oid) LIKE 'FOREIGN KEY %%' THEN substring(pg_get_constraintdef(c.oid), 14, position(')' in pg_get_constraintdef(c.oid))-14) WHEN pg_get_constraintdef(c.oid) LIKE 'PRIMARY KEY %%' THEN substring(pg_get_constraintdef(c.oid), 14, position(')' in pg_get_constraintdef(c.oid))-14) END AS "col_name"
          , CASE WHEN pg_get_constraintdef(c.oid) LIKE 'FOREIGN KEY %%' THEN concat_ws('.', n.nspname, substring(pg_get_constraintdef(c.oid), position(' REFERENCES ' in pg_get_constraintdef(c.oid))+12, position('(' in substring(pg_get_constraintdef(c.oid), 14))-position(' REFERENCES ' in pg_get_constraintdef(c.oid))+1)) END AS "ref_table"
          , CASE WHEN pg_get_constraintdef(c.oid) LIKE 'FOREIGN KEY %%' THEN substring(pg_get_constraintdef(c.oid), position('(' in substring(pg_get_constraintdef(c.oid), 14))+14, position(')' in substring(pg_get_constraintdef(c.oid), position('(' in substring(pg_get_constraintdef(c.oid), 14))+14))-1) END AS "ref_col"
          , pg_get_constraintdef(c.oid) as constraint_def,
          CASE
            WHEN confupdtype = 'a' THEN 'NO ACTION'
            WHEN confupdtype = 'r' THEN 'RESTRICT'
            WHEN confupdtype = 'c' THEN 'CASCADE'
            WHEN confupdtype = 'n' THEN 'SET NULL'
            WHEN confupdtype = 'd' THEN 'SET DEFAULT'
        END AS update_rule,
        CASE
            WHEN confdeltype = 'a' THEN 'NO ACTION'
            WHEN confdeltype = 'r' THEN 'RESTRICT'
            WHEN confdeltype = 'c' THEN 'CASCADE'
            WHEN confdeltype = 'n' THEN 'SET NULL'
            WHEN confdeltype = 'd' THEN 'SET DEFAULT'
        END AS delete_rule
    FROM   pg_constraint c
    JOIN   pg_namespace n ON n.oid = c.connamespace
    WHERE  contype IN ('f', 'p', 'u')
    ORDER  BY conrelid::regclass::text, contype DESC;""", postgres_engine)
    # List the schemas
    schema_list = list(table_sql['schemaname'])
    schema_str = ','.join(set(schema_list))
    # Stats for column level stats
    all_cols = pd.read_sql("""select DISTINCT ON(table_name, col_name) concat_ws('.',
            --n.nspname,
            attrelid::regclass) AS table_name, f.attname AS col_name,
            pg_catalog.format_type(f.atttypid,f.atttypmod) AS type, attnotnull,
            CASE
                WHEN f.atthasdef = 't' THEN d.adsrc
            END AS default, description,
            CASE
                WHEN d.adsrc LIKE 'nextval%%' THEN True
                ELSE False
            END AS auto_increment, null_frac * c.reltuples AS num_null, null_frac AS perc_of_null,
            CASE WHEN s.n_distinct < 0
                THEN -s.n_distinct * c.reltuples
                ELSE s.n_distinct
           END AS num_of_distinct,
           CASE WHEN s.n_distinct < 0
                THEN round((-s.n_distinct * 100)::numeric, 2)
                ELSE round((s.n_distinct / c.reltuples * 100)::numeric, 2)
           END AS perc_of_distinct, c.relkind
            FROM pg_attribute f
            JOIN pg_class c ON c.oid = f.attrelid
            --JOIN pg_type t ON t.oid = f.atttypid
            LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = f.attnum
            LEFT JOIN pg_description de on de.objoid = c.oid
            LEFT JOIN pg_stats s on s.schemaname::regnamespace::oid = c.relnamespace AND s.tablename = c.relname AND s.attname = f.attname
            WHERE (c.relkind = 'v'::char or c.relkind = 'r'::char or c.relkind = 'p'::char)
            AND f.attnum > 0
            AND attisdropped is False
            AND n.nspname in ('{}');""".format(schema_str), postgres_engine)
    # Check for any table that is not in the pg_stats tables
    diff_list = list(set(all_cols['table_name']) - set(table_sql['table_name']))
    if diff_list:
        for i in diff_list:
            line = pd.DataFrame({"schemaname": i.split(".")[0], "table_name": i, "hasindexes": "False", "row_count": "n/a"}, index=[0])
            table_sql = pd.concat([table_sql, line])
    table_sql = table_sql.sort_values(by=['schemaname', 'table_name']).reset_index(drop=True)
    # List of tables
    table_list = list(table_sql['table_name'])
    view_list = list(view_sql['view_name'])
    #table_list = [m + '.' + str(n) for m, n in zip(schema_list, table_list)]
    overview_dict = {}
    # Show the stats for schemas, tables and PK/FK
    overview_dict['num_of_schemas'] = len(set(schema_list))
    overview_dict['schema_names'] = list(set(schema_list))
    overview_dict['num_of_tables'] = len(table_list)
    overview_dict['table_names'] = table_list
    overview_dict['num_of_views'] = len(view_list)
    overview_dict['view_names'] = view_list
    overview_dict['tables_no_index'] = list(table_sql[table_sql['hasindexes'] == "False"]['table_name'])
    overview_dict['num_of_pk'] = len(pk_fk[pk_fk['constraint_type'] == 'primary key'])
    overview_dict['num_of_fk'] = len(pk_fk[pk_fk['constraint_type'] == 'foreign key'])
    overview_dict['num_of_uk'] = len(pk_fk[pk_fk['constraint_type'] == 'unique key'])

    # Split into intermediate result dictionary form - table
    table_dict = {}
    for i in table_list:
        temp = {}
        temp_cols = all_cols[all_cols['table_name'] == i].drop(columns=['table_name']).to_dict(orient = 'records')
        for j in temp_cols:
            temp[j['col_name']] = {}
            element = j.pop('col_name')
            temp[element] = j
            temp[element]['children'] = list(pk_fk[(pk_fk['ref_table'] == i) & (pk_fk['ref_col'] == element)]['table_name'])
            temp[element]['parents'] = list(pk_fk[(pk_fk['table_name'] == i) & (pk_fk['col_name'] == element) & (pk_fk['constraint_type'] == 'foreign key')]['ref_table'])
        temp[i+'_num_of_parents'] = len(pk_fk[(pk_fk['table_name'] == i) & (pk_fk['constraint_type'] == 'foreign key')])
        temp[i+'_num_of_children'] = len(pk_fk[(pk_fk['ref_table'] == i)])
        temp[i+'_num_of_row'] = table_sql[table_sql['table_name'] == i]['row_count'].values[0]
        temp[i+'_num_of_cols'] = len(all_cols[all_cols['table_name'] == i])
        temp['constraints'] = {}
        temp_pk_fk = pk_fk[pk_fk['table_name'] == i].drop(columns=['table_name']).to_dict(orient = 'records')
        for j in temp_pk_fk:
            temp['constraints'][j['constraint_name']] = {}
            element = j.pop('constraint_name')
            temp['constraints'][element] = j
        table_dict[i] = temp
    # Split into intermediate result dictionary form - view
    view_dict = {}
    for i in view_list:
        temp = {}
        temp_cols = all_cols[all_cols['table_name'] == i].drop(columns=['table_name']).to_dict(orient = 'records')
        for j in temp_cols:
            temp[j['col_name']] = {}
            element = j.pop('col_name')
            temp[element] = j
        temp[i+'_num_cols'] = len(all_cols[all_cols['table_name'] == i])
        temp[i+'_definition'] = view_sql[view_sql['view_name'] == i]['definition'].values[0]
        view_dict[i] = temp
    return overview_dict, table_dict, view_dict
