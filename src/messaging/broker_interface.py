from abc import ABC, abstractmethod


class BrokerInterface(ABC):
    @abstractmethod
    def publish(self, topic, event):
        raise NotImplementedError

    @abstractmethod
    def subscribe(self, topic, handler):
        raise NotImplementedError