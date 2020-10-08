class searchcords:
  def __init__(self, ra, dec):
    self.RA = ra
    self.dec = dec

  def search(self):
    ra = self.RA
    dec = self.dec
    try:
        Galaxy = Ref.objects.all().values('ra', 'dec', 'id')
        lent = len(Galaxy)
        ra2 = np.zeros((lent))
        dec2 = np.zeros((lent))

        for idx, value in enumerate(Galaxy):
          ra2[idx] = value['ra']
          dec2[idx] = value['dec']

        c = SkyCoord(ra=ra*u.degree, dec=dec*u.degree)
        catalog = SkyCoord(ra=ra2*u.degree, dec=dec2*u.degree)
        idx, d2d, d3d = c.match_to_catalog_sky(catalog)
        idx2 = idx
        del ra2
        del dec2

        Field = Ref.objects.values('id', 'name', 'status').filter(id = f'{idx+1}').first()
        Status = Field['status']
        Field = Field['name']
        Galaxy = create_model(Field).objects.all().values('RA', 'Dec', 'id')
        lent = len(Galaxy)
        ra2 = np.zeros((lent))
        dec2 = np.zeros((lent))
        for idx, value in enumerate(Galaxy):
          ra2[idx] = value['RA']
          dec2[idx] = value['Dec']

        c = SkyCoord(ra=ra*u.arcsec, dec=dec*u.arcsec)
        catalog = SkyCoord(ra=ra2*u.arcsec, dec=dec2*u.arcsec)
        idx, d2d, d3d = c.match_to_catalog_sky(catalog)
        del ra2
        del dec2
        Galaxyrd = Galaxy[idx.tolist()]
        Galaxyrd = Galaxyrd['id']
        Galaxy = create_model(Field).objects.filter(id = str(Galaxyrd)).first()
        c = SkyCoord(ra=ra*u.degree, dec=dec*u.degree)
        d2d = c.separation(SkyCoord(ra=float(Galaxy.RA)*u.degree, dec=float(Galaxy.Dec)*u.degree))
        if (d2d < 1.0 *u.arcsec) == True:
          return Galaxy, Status
        if (d2d < 1.0 *u.arcsec) == False:
            Field = Ref.objects.values('id', 'name', 'status').filter(id = f'{idx2}').first()
            Status = Field['status']
            Field = Field['name']
            Galaxy = create_model(Field).objects.all().values('RA', 'Dec', 'id')
            lent = len(Galaxy)
            ra2 = np.zeros((lent))
            dec2 = np.zeros((lent))
            for idx, value in enumerate(Galaxy):
              ra2[idx] = value['RA']
              dec2[idx] = value['Dec']

            c = SkyCoord(ra=ra*u.arcsec, dec=dec*u.arcsec)
            catalog = SkyCoord(ra=ra2*u.arcsec, dec=dec2*u.arcsec)
            idx, d2d, d3d = c.match_to_catalog_sky(catalog)
            del ra2
            del dec2
            Galaxyrd = Galaxy[idx.tolist()]
            Galaxyrd = Galaxyrd['id']
            Galaxy = create_model(Field).objects.filter(id = str(Galaxyrd)).first()
            c = SkyCoord(ra=ra*u.degree, dec=dec*u.degree)
            d2d = c.separation(SkyCoord(ra=float(Galaxy.RA)*u.degree, dec=float(Galaxy.Dec)*u.degree))
            return Galaxy, Status
    except:
          pass
