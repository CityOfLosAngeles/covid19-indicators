from setuptools import find_packages, setup

setup(
    name='processing_utils',
    packages=find_packages(),
    version='0.1.0',
    description='Utilities for processing COVID-19 data',
    author='City of Los Angeles',
    license='Apache',
    include_package_data=True,
    package_dir={"processing_utils": "processing_utils"},
    install_requires=["altair", "geopandas", "numpy", "pandas"],
)