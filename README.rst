
aeropress
=========

``aeropress`` is a CLI program for deploying Docker images to AWS ECS. It receives a folder path that includes
ECS task and service definitions and then does the jobs respectively;

  - Register ECS task definitions
  - Create Cloudwatch metrics for scaling policies
  - Create or update scaling policies for ECS services
  - Create or update alarms on Cloudwatch
  - Create or update ECS services

Installation
------------
``aeropress`` works with Python3.

::

 pip3 install aeropress

Usage
-----

::

    $ aeropress --help
    usage: aeropress [-h] [--logging-level {debug,info,warning,error}] [--version]
                    path image_url

    aeropress AWS ECS deployment helper

    positional arguments:
    path                  Config path that includes service definitions.
    image_url             Image URL for docker image.

    optional arguments:
    -h, --help            show this help message and exit
    --logging-level {debug,info,warning,error}
                            Print debug logs
    --version             show program's version number and exit


Example
-------

You must have defined an ECS cluster first. Then, you can define ECS tasks and services in a yaml file and run
``aeropress`` with required arguments.
::

  aeropress 'example/foo.yaml' 'registry.hub.docker.com/library/python'