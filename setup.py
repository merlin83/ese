from setuptools import setup, find_packages

install_requires = [
    "requests>=2.5.0",
    "elasticsearch>=1.0.0,<2.0.0"
]

setup(
    name="ese",
    version="0.1",
    description="Tool to export Elasticsearch index from one cluster to another cluster",
    url="https://github.com/merlin83/ese",
    author="Khee Chin",
    author_email="kheechin@gmail.com",
    license="MIT",
    packages=find_packages(),
    zip_safe=False,
    install_requires=install_requires,
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'ese = ese.ese:main'
        ]
    }
)

