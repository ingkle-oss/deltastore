from setuptools import setup

setup(
    name="deltastore",
    version="0.7.0",
    description="Python Connector for DeltaStore",
    author="The Delta Store Project Authors",
    author_email="haruband@gmail.com",
    url="https://github.com/dataplatform-lab/deltastore",
    python_requires=">= 3.8",
    packages=["deltastore"],
    install_requires=["pandas", "pyarrow", "pytest"],
    zip_safe=False,
    package_data={},
    include_package_data=True,
)
