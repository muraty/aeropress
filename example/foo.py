
import deployer


if __name__ == '__main__':
    path = 'config'
    image_url = 'registry.hub.docker.com/library/python'

    deployer.deploy(path, image_url)
