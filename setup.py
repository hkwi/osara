from setuptools import setup, find_packages

setup(name='osara',
	version='0.0.1',
	description='micro-framewaork on top of kappa architecture',
	author='Hiroaki Kawai',
	url='http://github.com/hkwi/osara',
	packages=find_packages(exclude=("tests",)),
	install_requires=[
		'confluent_kafka',
		'sqlalchemy',
		'werkzeug',
	],
)
