from zope.interface import Interface

class UseCaseInterface(Interface):
    """Use Case Interface"""

    def run(self) -> None:
        """Usecase runner

        Raises:
            NotImplementedError: Raise an error if not implemented
        """
        raise NotImplementedError