from setuptools import setup, find_packages


setup(
    license="Hippocratic License",
    packages=find_packages(),
    # install_requires=parent_dir.joinpath("requirements.txt").read_text(encoding="utf-8").splitlines(),
    python_requires=">=3.11",
)
