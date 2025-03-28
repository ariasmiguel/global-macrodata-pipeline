"""Setup configuration for the Macroeconomic Data Pipeline package."""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="macrodata_pipeline",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="A pipeline for extracting and processing macroeconomic data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/global-macrodata-pipeline",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Office/Business :: Financial :: Investment",
        "Programming Language :: Python :: 3.10",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",
    install_requires=[
        "pandas>=2.0.0",
        "clickhouse-connect>=0.6.0",
        "python-dotenv>=1.0.0",
        "pydantic>=2.0.0",  # For data validation
        "loguru>=0.7.0",    # For better logging
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "black>=23.0.0",
            "isort>=5.0.0",
            "mypy>=1.0.0",
            "python-dotenv>=1.0.0",
        ],
    },
) 