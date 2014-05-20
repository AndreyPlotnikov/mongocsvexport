from setuptools import setup, find_packages

setup(
    name='mongocsvexport',
    version='0.1',
    packages=find_packages(exclude=['tests*']),
    url='https://github.com/AndreyPlotnikov/mongocsvexport',
    license='MIT',
    author='Andrey Plotnikov',
    author_email='plotnikoff@gmail.com',
    description='Utility that produces a CSV of data stored in a MongoDB',
    install_requires=[
      'pymongo',
      'tqdm'
    ],
    entry_points={
        "console_scripts": [
            "mongocsvexport=mongocsvexport:main"
        ]
    }
)
