import os
from pathlib import Path
from setuptools import setup, find_packages

parent_dir = Path(__file__).resolve().parent

setup(
    name='basicetl',
    version='v0.1.0',
    description='A local ETL automation tool.',
    author='Your Name',
    author_email='your.email@example.com',
    license="Hippocratic License",
    packages=find_packages(),
    install_requires=parent_dir.joinpath("requirements.txt").read_text().splitlines(),
    python_requires=">=3.11",
)
