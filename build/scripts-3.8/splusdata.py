import astropy
import pandas as pd
import numpy as np

from astropy.table import Table
from astropy.io import ascii
import sqlalchemy
from sqlalchemy import inspect
import psycopg2
import re

def queryidr3(Survey, *args):
    password = str(input("input password: "))
    engine = sqlalchemy.create_engine(f'postgresql://SPLUS_readonly:{password}@flaskpro.cm2v0pvygspl.sa-east-1.rds.amazonaws.com:5432/Splus')

    try:
        if args == ():
            query = f"""SELECT * FROM {Survey} """
        if args[0][0].split('=')[0].strip().lower() == 'field':
            query = f"""SELECT * FROM {Survey} WHERE "Field" = '{args[0][0].split('=')[1].strip()}'"""
            result = pd.read_sql_query(query, engine)
            print('Done!')
            return(result)
        if args[0][0].split('==')[0].strip().lower() == 'field':
            query = f"""SELECT * FROM {Survey} WHERE "Field" = '{args[0][0].split('==')[1].strip()}'"""
            result = pd.read_sql_query(query, engine)
            print('Done!')
            return(result)

        if args[0][0].split('=')[0].strip().lower() == 'id':
            query = f"""SELECT * FROM {Survey} WHERE "ID" = '{args[0][0].split('=')[1].strip()}'"""
            result = pd.read_sql_query(query, engine)
            print('Done!')
            return(result)
        if args[0][0].split('==')[0].strip().lower() == 'id':
            query = f"""SELECT * FROM {Survey} WHERE "ID" = '{args[0][0].split('==')[1].strip()}'"""
            result = pd.read_sql_query(query, engine)
            print('Done!')
            return(result)

        if args[0] != ():
            conditions = get_conditions(args[0])
            query = f"""SELECT * FROM "{Survey}" WHERE """
            for key, condition in enumerate(conditions):
                if key == 0:
                    query = str(query) + str(condition) + ' '
                if key != 0:
                    query = str(query) + 'and ' + str(condition) + ' '
            print('getting data...\n')
            print('it may take a minute sometimes\n')
        result = pd.read_sql_query(query, engine)
        print('Done!')
        return(result)


    except sqlalchemy.exc.ProgrammingError:
        print("Error while searching, check if the column exists or if it is typed correctly")
    else:
        print("Something is wrong!")

def get_columns():
    password = str(input("input password: "))
    engine = sqlalchemy.create_engine(f'postgresql://SPLUS_readonly:{password}@flaskpro.cm2v0pvygspl.sa-east-1.rds.amazonaws.com:5432/Splus')

    cols = []
    for x in inspect(engine).get_columns('main1'):
        cols.append(x['name'])

    return cols

def get_columns_return(engine):
    cols = []
    for x in inspect(engine).get_columns('main1'):
        cols.append(x['name'])

    return cols


def get_surveys():
    surveys = ['MAIN1']
    print(surveys)

def get_conditions(condits):
    conditions = []
    for condit in condits:
        if '<'in condit:
            operation = get_operation(condit)
            if operation != 'none':
                goal = replace_sym(condit)
                cond = f'"{goal[0].strip()}" {operation} "{goal[1].strip()}" < {goal[2].strip()}'
                conditions.append(cond)
            else:
                splits = condit.split('<')
                cond = f'"{splits[0].strip()}" < {(splits[1])}'
                conditions.append(cond)
        if '>'in condit:
            operation = get_operation(condit)
            if operation != 'none':
                goal = replace_sym(condit)
                cond = f'"{goal[0].strip()}" {operation} "{goal[1].strip()}" > {goal[2].strip()}'
                conditions.append(cond)
            else:
                splits = condit.split('>')
                cond = f'"{splits[0].strip()}" > {float(splits[1])}'
                conditions.append(cond)
        if '=' in condit:
            splits = condit.split('=')
            try:
                operation = get_operation(condit)
                if operation != 'none':
                    goal = replace_sym(condit)
                    cond = f'"{goal[0].strip()}" {operation} "{goal[1].strip()}" = {goal[2].strip()}'
                    conditions.append(cond)

                else:
                    float(splits[1])
                    cond = f'"{splits[0].strip()}" = {splits[1]}'
                    conditions.append(cond)
            except:
                pass

        if '==' in condit:
            splits = condit.split('==')
            try:
                operation = get_operation(condit)
                if operation != 'none':
                    goal = replace_sym(condit)
                    cond = f'"{goal[0].strip()}" {operation} "{goal[1].strip()}" = {goal[2].strip()}'
                    conditions.append(cond)
                else:
                    float(splits[1])
                    cond = f""""{splits[0].strip()}" = {splits[1]}' """
                    conditions.append(cond)
            except:
                pass

    return conditions

def replace_sym(string):
    rep = {"=": ";", "==": ";", "<": ";", ">": ";", "+": ";", "-": ";", "*": ";", "/": "<=", ">=": ";"}
    # string = string.replace(rep)

    rep = dict((re.escape(k), v) for k, v in rep.items())
    pattern = re.compile("|".join(rep.keys()))
    string = pattern.sub(lambda m: rep[re.escape(m.group(0))], string)

    string = string.split(";")
    return string

def get_operation(string):
    if '+' in string:
        return '+'
    if '-' in string:
        return '-'
    if '*' in string:
        return '*'
    if '/' in string:
        return '/'
    else:
        return 'none'


# In[52]:


def queryidr3_complex(Survey, *args):
    password = str(input("input password: "))
    engine = sqlalchemy.create_engine(f'postgresql://SPLUS_readonly:{password}@flaskpro.cm2v0pvygspl.sa-east-1.rds.amazonaws.com:5432/Splus')

    try:
        if args == ():
            query = f"""SELECT * FROM {Survey} """
        if args[0][0].split('=')[0].strip().lower() == 'field':
            query = f"""SELECT * FROM {Survey} WHERE "Field" = '{args[0][0].split('=')[1].strip()}'"""
            result = pd.read_sql_query(query, engine)
            print('Done!')
            return(result)
        if args[0][0].split('==')[0].strip().lower() == 'field':
            query = f"""SELECT * FROM {Survey} WHERE "Field" = '{args[0][0].split('==')[1].strip()}'"""
            result = pd.read_sql_query(query, engine)
            print('Done!')
            return(result)

        if args[0][0].split('=')[0].strip().lower() == 'id':
            query = f"""SELECT * FROM {Survey} WHERE "ID" = '{args[0][0].split('=')[1].strip()}'"""
            result = pd.read_sql_query(query, engine)
            print('Done!')
            return(result)
        if args[0][0].split('==')[0].strip().lower() == 'id':
            query = f"""SELECT * FROM {Survey} WHERE "ID" = '{args[0][0].split('==')[1].strip()}'"""
            result = pd.read_sql_query(query, engine)
            print('Done!')
            return(result)

        if args[0] != ():
            conditions = get_conditions_complex(args[0], engine)
            query = f"""SELECT * FROM "{Survey}" WHERE """
            for key, condition in enumerate(conditions):
                if key == 0:
                    query = str(query) + str(condition) + ' '
                if key != 0:
                    query = str(query) + 'and ' + str(condition) + ' '

            print('getting data...\n')
            print('it may take a minute sometimes\n')

        result = pd.read_sql_query(query, engine)
        print('Done!')
        return(result)


    except sqlalchemy.exc.ProgrammingError:
        print("Error while searching, check if the column exists or if it is typed correctly")
    else:
        print("Something is wrong!")


def get_conditions_complex(condits, engine):
    try:
        conditions = []
        for condit in condits:
            operation = get_operation(condit)
            if operation != 'none':
                cond = sql_ready(condit, engine)
                conditions.append(cond)
            else:
                cond = sql_ready(condit, engine)
                conditions.append(cond)

        return conditions
    except:
        print('Error with conditions')

def sql_ready(string, engine):
    try:
        cols = get_columns_return(engine)

        for key, col in enumerate(cols):
            if col != 'A' in string and key == 0:
                string = string.replace(col, f'"{col}"')
            if col != 'A' and col != 'CLASS_STAR' and col in string and key != 0:
                string = string.replace(col, f'"{col}"')

        return(string)
    except:
        print('Error with conditions')
