from zope.interface import Interface


class DatabaseInterface(Interface):
    """Use Case Interface"""

    def connect(self) -> None:
        """Connect to database instance/connection

        Raises:
            NotImplementedError: Raise an error if not implemented
        """
        raise NotImplementedError

    def close(self) -> None:
        """Close database instance/connection

            Raises:
                NotImplementedError: Raise an error if not implemented
            """
        raise NotImplementedError