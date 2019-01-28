import os
from setuptools import setup
from setuptools import find_packages

README = open(os.path.join(os.path.dirname(__file__), 'README.rst')).read()


setup(
    name='aeropress',
    version=open("deployer/_version.py").readlines()[-1].split()[-1].strip("\"'"),
    packages=find_packages(),
    python_requires='>=3.5.2',
    install_requires=[
        'boto3==1.9.62',
        'mypy==0.550',
        'flake8==3.6.0',
        'PyYAML==3.13',
    ],
    include_package_data=True,
    license='BSD License',
    description='Helper for deploying Docker images to AWS ECS.',
    long_description=README,
    keywords='aws ecs deploy deployer docker image container containerization',
    url='https://github.com/muraty/aeropress',
    author='Omer Murat Yildirim',
    author_email='omermuratyildirim@gmail.com',
    entry_points={
        'console_scripts': [
            'aeropress = deployer.cli:main',
        ]
    }
)
