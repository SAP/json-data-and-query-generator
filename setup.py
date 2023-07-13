from setuptools import setup, find_packages

setup(
    name='json_data_and_query_generator',
    version='0.0.1',    
    description='json_data_and_query_generator',
    url='https://github.wdf.sap.corp/D037559/json_data_and_query_generator',
    author='TODO',
    author_email='TODO',
    license='TODO',
    packages=[
      'json_data_and_query_generator',
      'json_data_and_query_generator.pipeline',
      'json_data_and_query_generator.data_generators',
      'json_data_and_query_generator.data_generators.faker_generator',
      'json_data_and_query_generator.query_generator',
      'json_data_and_query_generator.feasibility',
      'json_data_and_query_generator.examples.hello_data'
    ],
    include_package_data=True,
    package_data={'json_data_and_query_generator.examples.hello_data': ['*.json']},
    install_requires=[
      'faker',
      'jinjasql',
      'numpy',
      'MarkupSafe',
      'jinja2==3.0.1',
    ]
)
