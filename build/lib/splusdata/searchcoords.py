from astropy.coordinates import SkyCoord
from astropy import units as u
import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy import inspect
import psycopg2

searchcords(48.8034,-30.9018).search_id()

class searchcords:
  def __init__(self, ra, dec):
    self.ra = ra
    self.dec = dec

  def search_field(self):
    ra = self.ra
    dec = self.dec

    password = str(input("input password: "))
    engine = sqlalchemy.create_engine(f'postgresql://SPLUS_readonly:{password}@143.107.18.89:5432/splus')

    query = f"""SELECT "RA", "DEC", "NAME" FROM "Ref" WHERE "RA" < {ra+7} and "RA" > {ra - 7} and "DEC" > {dec -7} and "DEC" < {dec + 7}"""
    Galaxy = pd.read_sql_query(query, engine)

    lent = len(Galaxy)
    ra2 = Galaxy.RA.to_numpy()
    dec2 = Galaxy.DEC.to_numpy()

    c = SkyCoord(ra=ra*u.degree, dec=dec*u.degree)
    catalog = SkyCoord(ra=ra2*u.degree, dec=dec2*u.degree)
    idx, d2d, d3d = c.match_to_catalog_sky(catalog)
    idx2 = idx

    Field = Galaxy.NAME[idx2]

    return Field

  def search_obj(self):
    ra = self.ra
    dec = self.dec

    password = str(input("input password: "))
    engine = sqlalchemy.create_engine(f'postgresql://SPLUS_readonly:{password}@143.107.18.89:5432/splus')

    query = f"""SELECT "RA", "DEC", "NAME" FROM "Ref" WHERE "RA" < {ra+7} and "RA" > {ra - 7} and "DEC" > {dec -7} and "DEC" < {dec + 7}"""
    Galaxy = pd.read_sql_query(query, engine)

    ra2 = Galaxy.RA.to_numpy()
    dec2 = Galaxy.DEC.to_numpy()

    c = SkyCoord(ra=ra*u.degree, dec=dec*u.degree)
    catalog = SkyCoord(ra=ra2*u.degree, dec=dec2*u.degree)
    idx, d2d, d3d = c.match_to_catalog_sky(catalog)
    idx2 = idx

    Field = Galaxy.NAME[idx2]

    whereField = pd.read_csv('https://raw.githubusercontent.com/Schwarzam/Data-analyse---SPLUS-objs/master/fields.csv', skiprows=1)
    print(Field)
    ans = whereField[whereField['NAME'] == Field]

    if len(ans) < 1:
        print('Field not in any Survey')
        return 0

    query = f"""SELECT "RA", "DEC", "ID" FROM "{ans.SUBREGION.array[0].lower()}" WHERE "RA" < {ra+0.02} and "RA" > {ra - 0.02} and "DEC" > {dec -0.02} and "DEC" < {dec + 0.02}"""
    Galaxy = pd.read_sql_query(query, engine)

    ra2 = Galaxy.RA.to_numpy()
    dec2 = Galaxy.DEC.to_numpy()

    c = SkyCoord(ra=ra*u.degree, dec=dec*u.degree)
    catalog = SkyCoord(ra=ra2*u.degree, dec=dec2*u.degree)
    idx, d2d, d3d = c.match_to_catalog_sky(catalog)
    idx2 = idx

    obj = Galaxy[int(idx2):int(idx2)+1]

    return obj