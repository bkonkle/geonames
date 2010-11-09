from distutils.core import setup

VERSION = __import__('geonames').__version__

try:
    long_description = open('README', 'rt').read()
except IOError:
    long_description = ''

description = "Models for using the geonames database with Django"

setup(
    name='geonames',
    version=VERSION,
    description=description,
    long_description = long_description,
    author='Alberto Garcia Hierro',
    author_email='fiam@rm-fr.net',
    maintainer='Brandon Konkle',
    maintainer_email='brandon@brandonkonkle.com',
    license='License :: OSI Approved :: BSD License',
    url='https://github.com/bkonkle/geonames/',
    packages=['geonames'],
    classifiers=[
        'Framework :: Django',
        'Intended Audience :: Developers',
        'Development Status :: 3 - Alpha',
    ]
)
