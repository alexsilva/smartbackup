import os
from setuptools import setup, find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name="smartbackup",
    version="0.1.0",
    author="alex",
    author_email="geniofuturo@gmail.com",
    description="Backup tools using the framework bakthat.",
    license="MIT",
    keywords="s3 backup tools",
    url="https://github.com/alexsilva/smartbackup",
    packages=find_packages(exclude=[]),
    long_description=read('README.rst'),
    install_requires=["bakthat", "filechunkio"],
    entry_points={'console_scripts': ["smartbackup = smartbackup:main"]},
    classifiers=[
        "Development Status :: 1 - Beta",
        "Intended Audience :: Developers",
        "Topic :: System :: Archiving :: Backup",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
    ],
    zip_safe=False,
)
