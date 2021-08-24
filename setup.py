from setuptools import setup, find_packages

with open("README.md","r") as fh:
	long_description = fh.read()

setup(name='osara',
	version='0.0.4',
	description='micro-framewaork on top of kappa architecture',
	author='Hiroaki Kawai',
	author_email="hiroaki.kawai@gmail.com",
	long_description=long_description,
	long_description_content_type="text/markdown",
	url='http://github.com/hkwi/osara',
	packages=["osara"],
	install_requires=[
		'confluent_kafka',
		'sqlalchemy',
		'werkzeug',
	],
	classifiers=[
		"Programming Language :: Python :: 3",
		"License :: OSI Approved :: MIT License",
		"Operating System :: OS Independent",
	],
	python_requires=">=3.6",
)
