import astropy
import pandas as pd
import numpy as np

from astropy.table import Table
from astropy.io import ascii
import sqlalchemy
from sqlalchemy import inspect
import psycopg2
import re

def queryidr3(Survey, conditions=[], columns = ['Field', 'ID', 'RA', 'DEC', 'X', 'Y', 'ISOarea', 'MU_MAX', 'A', 'B', 'THETA', 'ELONGATION', 'ELLIPTICITY', 'PhotoFlagDet', 'CLASS_STAR', 'FWHM', 'U_auto', 'e_U_auto', 'F378_auto', 'e_F378_auto', 'F395_auto', 'e_F395_auto', 'F410_auto', 'e_F410_auto', 'F430_auto', 'e_F430_auto', 'G_auto', 'e_G_auto', 'F515_auto', 'e_F515_auto', 'R_auto', 'e_R_auto', 'F660_auto', 'e_F660_auto', 'I_auto', 'e_I_auto', 'F861_auto', 'e_F861_auto', 'Z_auto', 'e_Z_auto', 'nDet_auto', 'U_aper_3', 'e_U_aper_3', 'F378_aper_3', 'e_F378_aper_3', 'F395_aper_3', 'e_F395_aper_3', 'F410_aper_3', 'e_F410_aper_3', 'F430_aper_3', 'e_F430_aper_3', 'G_aper_3', 'e_G_aper_3', 'F515_aper_3', 'e_F515_aper_3', 'R_aper_3', 'e_R_aper_3', 'F660_aper_3', 'e_F660_aper_3', 'I_aper_3', 'e_I_aper_3', 'F861_aper_3', 'e_F861_aper_3', 'Z_aper_3', 'e_Z_aper_3', 'U_PStotal', 'e_U_PStotal', 'F378_PStotal', 'e_F378_PStotal', 'F395_PStotal', 'e_F395_PStotal', 'F410_PStotal', 'e_F410_PStotal', 'F430_PStotal', 'e_F430_PStotal', 'G_PStotal', 'e_G_PStotal', 'F515_PStotal', 'e_F515_PStotal', 'R_PStotal', 'e_R_PStotal', 'F660_PStotal', 'e_F660_PStotal', 'I_PStotal', 'e_I_PStotal', 'F861_PStotal', 'e_F861_PStotal', 'Z_PStotal', 'e_Z_PStotal', 'U_petro', 'F378_petro', 'F395_petro', 'F410_petro', 'F430_petro','G_petro','F515_petro','R_petro','F660_petro','I_petro','F861_petro','Z_petro','e_U_petro','e_F378_petro','e_F395_petro','e_F410_petro','e_F430_petro','e_G_petro','e_F515_petro','e_R_petro','e_F660_petro','e_I_petro','e_F861_petro','e_Z_petro']):
    password = str(input("input password: "))
    engine = sqlalchemy.create_engine(f'postgresql://SPLUS_readonly:{password}@143.107.18.89:5432/splus')

    if columns != 'all':
        columns = format_cols(columns)
    if columns == 'all':
        columns = "*"

    try:
        if conditions == []:
            query = f"""SELECT {columns} FROM "{Survey.lower()}" """
            result = pd.read_sql_query(query, engine)
            print('Done!')
            return(result)

        if conditions[0].split('=')[0].strip().lower() == 'field':
            query = f"""SELECT {columns} FROM "{Survey.lower()}" WHERE "Field" = '{conditions[0].split('=')[1].strip()}'"""
            result = pd.read_sql_query(query, engine)
            print('Done!')
            return(result)
        if conditions[0].split('==')[0].strip().lower() == 'field':
            query = f"""SELECT {columns} FROM "{Survey.lower()}" WHERE "Field" = '{conditions[0].split('==')[1].strip()}'"""
            result = pd.read_sql_query(query, engine)
            print('Done!')
            return(result)

        if conditions[0].split('=')[0].strip().lower() == 'id':
            query = f"""SELECT {columns} FROM "{Survey.lower()}" WHERE "ID" = '{conditions[0].split('=')[1].strip()}'"""
            result = pd.read_sql_query(query, engine)
            print('Done!')
            return(result)
        if conditions[0].split('==')[0].strip().lower() == 'id':
            query = f"""SELECT {columns} FROM "{Survey.lower()}" WHERE "ID" = '{conditions[0].split('==')[1].strip()}'"""
            result = pd.read_sql_query(query, engine)
            print('Done!')
            return(result)

        if conditions != []:
            conditions = get_conditions(conditions)
            query = f"""SELECT {columns} FROM "{Survey.lower()}" WHERE """
            for key, condition in enumerate(conditions):
                if key == 0:
                    query = str(query) + str(condition) + ' '
                if key != 0:
                    query = str(query) + 'and ' + str(condition) + ' '
            print('getting data...\n')
            print('it may take a minute\n')
        result = pd.read_sql_query(query, engine)
        print('Done!')
        return(result)


    except sqlalchemy.exc.ProgrammingError:
        print("Error while searching, check if the column exists or if it is typed correctly")
    else:
        print("Something is wrong!")

def get_columns():
    password = str(input("input password: "))
    engine = sqlalchemy.create_engine(f'postgresql://SPLUS_readonly:{password}@143.107.18.89:5432/splus')

    cols = []
    for x in inspect(engine).get_columns('main3.1'):
        cols.append(x['name'])

    return cols

def get_columns_return(engine):
    cols = []
    for x in inspect(engine).get_columns('main3.1'):
        cols.append(x['name'])

    return cols


def get_surveys():
    surveys = ['main3.1', 'main3.2', 'main3.3', 'main3.4', 'main3.5', 'stripe82']
    print(surveys)

def get_conditions(condits):
    conditions = []
    for condit in condits:

        if '>='in condit:
            operation = get_operation(condit)
            if operation != 'none':
                goal = replace_sym(condit)
                cond = f'"{goal[0].strip()}" {operation} "{goal[1].strip()}" >= {goal[2].strip()}'
                conditions.append(cond)
                continue
            else:
                splits = condit.split('>=')
                cond = f'"{splits[0].strip()}" >= {(splits[1])}'
                conditions.append(cond)
                continue

        if '<='in condit:
            operation = get_operation(condit)
            if operation != 'none':
                goal = replace_sym(condit)
                cond = f'"{goal[0].strip()}" {operation} "{goal[1].strip()}" < {goal[2].strip()}'
                conditions.append(cond)
                continue
            else:
                splits = condit.split('<=')
                cond = f'"{splits[0].strip()}" <= {(splits[1])}'
                conditions.append(cond)
                continue

        if '<' in condit:
            operation = get_operation(condit)
            if operation != 'none':
                goal = replace_sym(condit)
                cond = f'"{goal[0].strip()}" {operation} "{goal[1].strip()}" < {goal[2].strip()}'
                conditions.append(cond)
            else:
                splits = condit.split('<')
                print(splits)
                cond = f'"{splits[0].strip()}" < {(splits[1])}'
                conditions.append(cond)

        if '>' in condit:
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



def queryidr3_complex(Survey, conditions=[], columns = ['Field', 'ID', 'RA', 'DEC', 'X', 'Y', 'ISOarea', 'MU_MAX', 'A', 'B', 'THETA', 'ELONGATION', 'ELLIPTICITY', 'PhotoFlagDet', 'CLASS_STAR', 'FWHM', 'U_auto', 'e_U_auto', 'F378_auto', 'e_F378_auto', 'F395_auto', 'e_F395_auto', 'F410_auto', 'e_F410_auto', 'F430_auto', 'e_F430_auto', 'G_auto', 'e_G_auto', 'F515_auto', 'e_F515_auto', 'R_auto', 'e_R_auto', 'F660_auto', 'e_F660_auto', 'I_auto', 'e_I_auto', 'F861_auto', 'e_F861_auto', 'Z_auto', 'e_Z_auto', 'nDet_auto', 'U_aper_3', 'e_U_aper_3', 'F378_aper_3', 'e_F378_aper_3', 'F395_aper_3', 'e_F395_aper_3', 'F410_aper_3', 'e_F410_aper_3', 'F430_aper_3', 'e_F430_aper_3', 'G_aper_3', 'e_G_aper_3', 'F515_aper_3', 'e_F515_aper_3', 'R_aper_3', 'e_R_aper_3', 'F660_aper_3', 'e_F660_aper_3', 'I_aper_3', 'e_I_aper_3', 'F861_aper_3', 'e_F861_aper_3', 'Z_aper_3', 'e_Z_aper_3', 'U_PStotal', 'e_U_PStotal', 'F378_PStotal', 'e_F378_PStotal', 'F395_PStotal', 'e_F395_PStotal', 'F410_PStotal', 'e_F410_PStotal', 'F430_PStotal', 'e_F430_PStotal', 'G_PStotal', 'e_G_PStotal', 'F515_PStotal', 'e_F515_PStotal', 'R_PStotal', 'e_R_PStotal', 'F660_PStotal', 'e_F660_PStotal', 'I_PStotal', 'e_I_PStotal', 'F861_PStotal', 'e_F861_PStotal', 'Z_PStotal', 'e_Z_PStotal', 'U_petro', 'F378_petro', 'F395_petro', 'F410_petro', 'F430_petro','G_petro','F515_petro','R_petro','F660_petro','I_petro','F861_petro','Z_petro','e_U_petro','e_F378_petro','e_F395_petro','e_F410_petro','e_F430_petro','e_G_petro','e_F515_petro','e_R_petro','e_F660_petro','e_I_petro','e_F861_petro','e_Z_petro']):

    password = str(input("input password: "))
    engine = sqlalchemy.create_engine(f'postgresql://SPLUS_readonly:{password}@143.107.18.89:5432/splus')
    if columns != 'all':
        columns = format_cols(columns)
    if columns == 'all':
        columns = "*"

    try:
        if conditions == []:
            query = f"""SELECT {columns} FROM "{Survey.lower()}" """
            result = pd.read_sql_query(query, engine)
            print('Done!')
            return(result)

        if conditions[0].split('=')[0].strip().lower() == 'field':
            query = f"""SELECT {columns} FROM "{Survey.lower()}" WHERE "Field" = '{conditions[0].split('=')[1].strip()}'"""
            result = pd.read_sql_query(query, engine)
            print('Done!')
            return(result)
        if conditions[0].split('==')[0].strip().lower() == 'field':
            query = f"""SELECT {columns} FROM "{Survey.lower()}" WHERE "Field" = '{conditions[0].split('==')[1].strip()}'"""
            result = pd.read_sql_query(query, engine)
            print('Done!')
            return(result)

        if conditions[0].split('=')[0].strip().lower() == 'id':
            query = f"""SELECT {columns} FROM "{Survey.lower()}" WHERE "ID" = '{conditions[0].split('=')[1].strip()}'"""
            result = pd.read_sql_query(query, engine)
            print('Done!')
            return(result)
        if conditions[0].split('==')[0].strip().lower() == 'id':
            query = f"""SELECT {columns} FROM "{Survey.lower()}" WHERE "ID" = '{conditions[0].split('==')[1].strip()}'"""
            result = pd.read_sql_query(query, engine)
            print('Done!')
            return(result)

        if conditions != []:
            conditions = get_conditions_complex(conditions, engine)
            query = f"""SELECT {columns} FROM "{Survey.lower()}" WHERE """
            for key, condition in enumerate(conditions):
                print(condition)
                if key == 0:
                    query = str(query) + str(condition) + ' '
                if key != 0:
                    query = str(query) + 'and ' + str(condition) + ' '

            print('getting data...\n')
            print('it may take a minute\n')

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


def queryidr3_sql():
    print('Example: SELECT "RA", "DEC" FROM "main3.1" WHERE "RA" > 68.1 and "RA" < 68.2')
    password = str(input("input password: "))
    engine = sqlalchemy.create_engine(f'postgresql://SPLUS_readonly:{password}@143.107.18.89:5432/splus')

    try:
        query = input("input query: ")

        result = pd.read_sql_query(query, engine)

        return result
    except:
        print("ERROR with query")

def format_cols(columns):
    x = ''
    for key, col in enumerate(columns):
        if key != len(columns) - 1:
            x = x + str(f'"{col}",')
        if key == len(columns) - 1:
            x = x + str(f'"{col}"')
    return x
