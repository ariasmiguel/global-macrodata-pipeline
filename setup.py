from setuptools import setup, find_packages

setup(
    name="macrodata_pipeline",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "requests>=2.31.0",
        "beautifulsoup4>=4.12.0",
        "selenium>=4.18.0",
        "pandas>=2.2.0",
        "python-dotenv>=1.0.0",
        "clickhouse-driver>=0.2.0",
    ],
    python_requires=">=3.8",
) 