import setuptools

setuptools.setup(
     name='splusdata',
     version='2.15',
     packages = setuptools.find_packages(),
     author="Gustavo Schwarz",
     author_email="gustavo.b.schwarz@gmail.com",
     description="Get data a lot of data within minutes",
     url="https://github.com/schwarzam/pip-splusdata",
     install_requires = ['pandas', 'astropy', 'sqlalchemy', 'psycopg2-binary', 'numpy', 'matplotlib', 'colour', 'pillow', 'requests'],
     classifiers=[
         "Programming Language :: Python :: 3",
         "License :: OSI Approved :: Apache Software License"
     ],
 )
#python3 setup.py bdist_wheel
#python3 -m twine upload dist/*
