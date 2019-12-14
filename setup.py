from setuptools import find_packages, setup

description = 'TBD'

with open('README.md') as f:
    long_description = f.read()

requirements = [
    'elasticsearch',
]
extras = {
   'pandas': ['pandas']
}

keywords = [
    'elasticsearch',
    'python',
    'pandas',
    'batch-processing',
]

classifiers = [
    'Environment :: Console',
    'License :: OSI Approved :: MIT License',
    'Natural Language :: English',
    'Operating System :: OS Independent',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
]

setup(
    name='elasticbatch',
    version='1.0.0',
    author='Daniel Kaslovsky',
    author_email='dkaslovsky@gmail.com',
    license='MIT',
    description=description,
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    install_requires=requirements,
    extras_require=extras,
    url='https://github.com/dkaslovsky/ElasticBatch',
    keywords=keywords,
    classifiers=classifiers,
)
