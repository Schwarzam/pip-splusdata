import setuptools

setuptools.setup(
     name='splusdata',
     version='1.0',
     packages = setuptools.find_packages(),
     author="Gustavo Schwarz",
     author_email="gustavo.b.schwarz@gmail.com",
     description="__",
     url="https://github.com/schwarzam/splusdata",
     install_requires = ['pandas', 'astropy', 'sqlalchemy', 'psycopg2-binary', 'numpy', 'matplotlib', 'colour', 'pillow', 'requests', 'scipy'],
     classifiers=[
         "Programming Language :: Python :: 3",
         "License :: OSI Approved :: Apache Software License"
     ],
 )
#python3 setup.py bdist_wheel
#python3 -m twine upload dist/*
