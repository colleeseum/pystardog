from stardog2.connector.client import Client
from stardog2.resource import Base, Resource


class TestConnection:
    @staticmethod
    def get_connection_details_class():
        return {
            "endpoint": "http://class:5820",
            "username": "class",
            "password": "class",
        }

    @staticmethod
    def get_connection_details_subclass():
        return {
            "endpoint": "http://subclass:5820",
            "username": "subclass",
            "password": "subclass",
        }

    @staticmethod
    def get_connection_details_instance():
        return {
            "endpoint": "http://instance:5820",
            "username": "instance",
            "password": "instance",
        }

    def test_set_client(self):
        res = Resource("test")

        ##########################
        # Default
        ##########################
        client = Base.client()
        assert client.username == "admin"
        client = Resource.client()
        assert client.username == "admin"
        client = res.client()
        assert client.username == "admin"

        #########################
        # Change Overall default
        #########################
        Base.set_client(Client(**self.get_connection_details_class()))
        client = Base.client()
        assert client.username == "class"
        client = res.client()
        assert client.username == "class"

        #####################################
        # Change default of a subclass family
        ######################################
        Resource.set_client(Client(**self.get_connection_details_subclass()))
        client = Base.client()
        assert client.username == "class"
        client = Resource.client()
        assert client.username == "subclass"
        client = res.client()
        assert client.username == "subclass"

        #####################################
        # Change default of a specific instance
        ######################################
        res.set_client(Client(**self.get_connection_details_instance()))
        client = Base.client()
        assert client.username == "class"
        client = Resource.client()
        assert client.username == "subclass"
        client = res.client()
        assert client.username == "instance"
