from setuptools import setup

setup(
    name='yaml-column-reorderer',
    version='0.1.0',
    py_modules=['yaml_reorder'],
    entry_points={
        'console_scripts': [
            'yaml-reorder=yaml_reorder:main',
        ],
    },
    install_requires=[
        'sqlglot',
        'ruamel.yaml',
    ],
)